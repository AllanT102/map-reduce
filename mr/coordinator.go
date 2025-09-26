package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"

	"github.com/google/uuid"
)

type TaskType int
type TaskStatus int

const (
	MapTask TaskType = iota
	ReduceTask
)

const (
	Idle TaskStatus = iota
	InProgress
	Completed
)

type Task struct {
	id        uuid.UUID
	typ       TaskType
	status    TaskStatus
	fileNames []string // for map task, this is input file name; for reduce task, this is intermediate file names
}

type WorkerMetadata struct {
	id            uuid.UUID
	lastHeartbeat time.Time
}

/*
Coordinator Responsibilities
- Assign tasks to workers upon request
- Track task status (idle, in-progress, completed)
- Handle task completion
- Reassign tasks if a worker fails (no heartbeat)
- Determine when all tasks are completed

** worker can have multiple tasks **

** Challenges **

Make mapper/reducers map thread safe so that it can be concurrently accessed by multiple threads
without locking the map

High Level Flow:

Worker Registers with Coordinator
Coordinator adds worker to map of workers
Worker requests task
Coordinator assigns a task to worker and marks task as in progress
Mapper writes intermediate key-value pairs to local disk in R partitioned files
Mapper completes and sends back the list of intermediate file names to coordinator
Coordinator marks those tasks as completed
Worker requests a reducer task
Coordinator sends reducer the file names for that partition
Reducer reads intermediate files, sorts them by key, and applies reduce function
Reducer completes and sends back to coordinator
Coordinator marks reducer task as completed
Coordinator checks if all tasks are completed

** Failure Handling **

Worker sends heartbeat to coordinator every few seconds
If coordinator does not receive heartbeat from worker for 10 seconds, it marks worker as failed
Coordinator reassigns tasks assigned to that worker to other idle workers
-> this is done by finding another waiting worker and assigning the same task to it

Reducer needs to wait until all M partitions are done before it can start reducing
-> barrier implementation?
*/
type Coordinator struct {
	workers            map[uuid.UUID]*WorkerMetadata
	tasks              map[uuid.UUID]map[uuid.UUID]*Task // map of worker ID to map of taskId to Task
	m                  sync.Mutex
	taskPool           chan Task
	nReduce            int
	nInitialInputFiles int
	wg                 sync.WaitGroup
	doneCh             chan struct{}
}

func (c *Coordinator) Register(args *RegisterArgs, reply *RegisterReply) error {
	if args == nil || reply == nil || c.workers[args.WorkerId] != nil {
		return nil
	}

	c.m.Lock()
	defer c.m.Unlock()
	c.workers[args.WorkerId] = &WorkerMetadata{
		id:            args.WorkerId,
		lastHeartbeat: time.Now(),
	}

	return nil
}

func (c *Coordinator) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	c.m.Lock()
	defer c.m.Unlock()

	select {
	case task, ok := <-c.taskPool:
		if !ok {
			return nil
		}
		task.status = InProgress
		reply.Task = task
		reply.NReduce = c.nReduce
		if c.tasks[args.WorkerId] == nil {
			c.tasks[args.WorkerId] = make(map[uuid.UUID]*Task)
		}
		c.tasks[args.WorkerId][task.id] = &task
	default:
		return nil
	}
	return nil
}

func (c *Coordinator) CompleteTask(args *CompleteTaskArgs, reply *CompleteTaskReply) error {
	c.m.Lock()
	defer c.m.Unlock()

	c.tasks[args.WorkerId][args.TaskId].status = Completed

	if c.tasks[args.WorkerId][args.TaskId].typ == MapTask {
		c.tasks[args.WorkerId][args.TaskId].fileNames = args.IntermediateFiles
	}

	// decrement wg to signal one task complete
	c.wg.Done()

	return nil
}

func (c *Coordinator) Heartbeat(args *HeartbeatArgs, reply *HeartbeatReply) error {
	c.m.Lock()
	defer c.m.Unlock()

	if args == nil || c.workers[args.WorkerId] == nil {
		return nil
	}

	c.workers[args.WorkerId].lastHeartbeat = time.Now()
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	select {
	case <-c.doneCh:
		close(c.doneCh)
		return true
	default:
		return false
	}
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		workers:            make(map[uuid.UUID]*WorkerMetadata),
		taskPool:           make(chan Task, len(files)), // buffer size equal to number of tasks
		m:                  sync.Mutex{},
		nInitialInputFiles: len(files),
		nReduce:            nReduce,
		wg:                 sync.WaitGroup{},
		doneCh:             make(chan struct{}),
	}

	for _, file := range files {
		c.taskPool <- Task{
			id:        uuid.New(),
			typ:       MapTask,
			status:    Idle,
			fileNames: []string{file},
		}
	}

	go c.mapTaskMonitor()
	go c.heartbeatMonitor()

	c.server()
	return &c
}

func (c *Coordinator) heartbeatMonitor() {

}

/*
*
Initially there are M map tasks and R reduce tasks.

# M is known when MakeCoordinator is invoked, and nReduce

Thus, mapping phase completes when M * R tasks intermediate files have been produced
*/
func (c *Coordinator) mapTaskMonitor() {
	c.wg.Add(c.nInitialInputFiles * c.nReduce)

	// barrier
	c.wg.Wait()

	// start reduce phase
	c.initReducePhase()
}

func (c *Coordinator) reduceTaskMonitor() {
	c.wg.Add(c.nReduce)

	// barrier
	c.wg.Wait()

	// all tasks done, cleanup
	close(c.taskPool)

	// signal done
	c.doneCh <- struct{}{}
}

func (c *Coordinator) initReducePhase() {
	// create R reduce tasks and add to task pool
	intermediateFilesByPartition := make(map[int][]string)

	// this is slow af, try to optimize later if possible
	for _, workerTasks := range c.tasks {
		for _, task := range workerTasks {
			if task.typ == MapTask {
				for _, fileName := range task.fileNames {
					reducePartition := extractReducePartition(fileName)
					intermediateFilesByPartition[reducePartition] = append(intermediateFilesByPartition[reducePartition], fileName)
				}
			}
		}
	}

	for partition, fileNames := range intermediateFilesByPartition {
		c.taskPool <- Task{
			id:        uuid.New(),
			typ:       ReduceTask,
			status:    Idle,
			fileNames: fileNames,
		}
		fmt.Printf("Created reduce task for partition %d with files: %v\n", partition, fileNames)
	}

	go c.reduceTaskMonitor()
}

func extractReducePartition(fileName string) int {
	var reducePartition int
	_, err := fmt.Sscanf(fileName, "mr-%*d-%d", &reducePartition)
	if err != nil {
		log.Fatalf("failed to extract reduce partition from file name %s: %v", fileName, err)
	}
	return reducePartition
}
