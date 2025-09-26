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
type MRState int

const (
	MapTask TaskType = iota
	ReduceTask
)

const (
	Idle TaskStatus = iota
	InProgress
	Completed
)

const (
	Mapping MRState = iota
	Reducing
	Done
)

type Task struct {
	id        uuid.UUID
	typ       TaskType
	status    TaskStatus
	fileNames []string // for map task, this is input file name; for reduce task, this is intermediate file names
	partition int      // only used for reduce tasks
}

type WorkerMetadata struct {
	id            uuid.UUID
	lastHeartbeat time.Time
	failed        bool
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
	wm                 sync.Mutex
	tm                 sync.Mutex
	taskPool           chan Task
	nReduce            int
	nInitialInputFiles int
	wg                 sync.WaitGroup
	doneCh             chan struct{}

	stateM sync.Mutex
	state  MRState

	completedReducers map[int]bool // used to keep track of completed reduce tasks to avoid retriggering
}

func (c *Coordinator) Register(args *RegisterArgs, reply *RegisterReply) error {
	if args == nil || reply == nil || c.workers[args.WorkerId] != nil {
		return nil
	}

	c.wm.Lock()
	defer c.wm.Unlock()
	c.workers[args.WorkerId] = &WorkerMetadata{
		id:            args.WorkerId,
		lastHeartbeat: time.Now(),
	}

	return nil
}

func (c *Coordinator) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	c.tm.Lock()
	defer c.tm.Unlock()

	select {
	case task, ok := <-c.taskPool:
		if !ok {
			return nil
		}

		task.status = InProgress
		reply.Task = task
		if task.typ == ReduceTask {
			reply.NReduce = c.nReduce
		}

		// first task assigned to worker
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
	c.tm.Lock()
	c.stateM.Lock()
	defer c.stateM.Unlock()
	defer c.tm.Unlock()

	// if the task is already marked as completed, ignore it
	if args != nil && c.tasks[args.WorkerId] != nil &&
		c.tasks[args.WorkerId][args.TaskId] != nil &&
		c.tasks[args.WorkerId][args.TaskId].status == Completed {
		return nil
	}

	c.tasks[args.WorkerId][args.TaskId].status = Completed

	if c.tasks[args.WorkerId][args.TaskId].typ == MapTask {
		c.tasks[args.WorkerId][args.TaskId].fileNames = args.IntermediateFiles
	}

	if c.tasks[args.WorkerId][args.TaskId].typ == ReduceTask {
		c.completedReducers[c.tasks[args.WorkerId][args.TaskId].partition] = true
	}

	// if all reduce tasks are completed, mark state as done
	if c.state == Reducing && len(c.completedReducers) == c.nReduce {
		c.state = Done
		c.doneCh <- struct{}{}
	}

	// decrement wg to signal one task complete
	// c.wg.Done()

	return nil
}

func (c *Coordinator) Heartbeat(args *HeartbeatArgs, reply *HeartbeatReply) error {
	c.wm.Lock()
	defer c.wm.Unlock()

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
		close(c.taskPool)
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
		tasks:              make(map[uuid.UUID]map[uuid.UUID]*Task), // map of worker ID to map of taskId to Task
		taskPool:           make(chan Task, len(files)),             // buffer size equal to number of tasks
		tm:                 sync.Mutex{},
		wm:                 sync.Mutex{},
		nInitialInputFiles: len(files),
		nReduce:            nReduce,
		wg:                 sync.WaitGroup{},
		doneCh:             make(chan struct{}),
		state:              Mapping,
		stateM:             sync.Mutex{},
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

/*
If state is in reducing phase and failed worker has only done reduce tasks, then just reassign reduce tasks to task pool and exit

If state is in reducing phase and failed worker has done map tasks, then:
- flush task pool
- notify all reduce workers to re-request tasks

Loop over all map tasks that worker has completed and redo them
*/
func (c *Coordinator) handleFailedWorkerTasks(failedWorkerId uuid.UUID) {
	c.tm.Lock()
	c.stateM.Lock()
	defer c.tm.Unlock()
	defer c.stateM.Unlock()

	// if in reducing phase and has only done reduce tasks, then just reassign reduce tasks to task pool and exit
	if c.state == Reducing {
		hasCompletedMapTasks := false
		for _, task := range c.tasks[failedWorkerId] {
			if task.typ == MapTask {
				hasCompletedMapTasks = true
			}
		}
		// no map tasks found
		if !hasCompletedMapTasks {
			for _, task := range c.tasks[failedWorkerId] {
				task.status = Idle
				c.taskPool <- *task
			}

			delete(c.tasks, failedWorkerId)
			return
		}
	}

	// flush task pool if in reduce phase and notify all reduce workers to re-request tasks
	if c.state == Reducing {
		c.notifyMapTaskFailure()
		for len(c.taskPool) > 0 {
			<-c.taskPool
		}
	}

	// loop over all map tasks that worker has completed and redo them
	for _, task := range c.tasks[failedWorkerId] {
		if task.typ == MapTask {
			task.status = Idle
			c.taskPool <- *task
		}
	}

	// since tasks are reassigned, delete the worker mapping as that worker has now technically not procesed any tasks
	delete(c.tasks, failedWorkerId)

	// start up the task monitor again because we are back in the mapping phase
	c.state = Mapping
	go c.mapTaskMonitor()
}

/*
This function is called when a worker that executed a map task fails
*/
func (c *Coordinator) notifyMapTaskFailure() {
	// call worker RPC to reset, should store rpc ip in worker metadata
}

func (c *Coordinator) heartbeatMonitor() {
	c.wm.Lock()
	defer c.wm.Unlock()
	for workerId, workerMetadata := range c.workers {
		if workerMetadata.failed {
			return
		}

		if time.Since(workerMetadata.lastHeartbeat) > 10*time.Second {
			workerMetadata.failed = true
			c.handleFailedWorkerTasks(workerId)
		} else {
			workerMetadata.lastHeartbeat = time.Now()
			return
		}
	}
}

func (c *Coordinator) mapTaskMonitor() {
	// c.wg.Add(c.nInitialInputFiles)

	// // barrier
	// c.wg.Wait()

	// hacky as fuck, but barrier/wg impl introduces edge cases
	for {
		c.tm.Lock()
		count := 0
		for _, workerTasks := range c.tasks {
			for _, task := range workerTasks {
				if task.typ == MapTask && task.status == Completed {
					count++
				}
			}
		}

		if count == c.nInitialInputFiles {
			break
		}
		c.tm.Unlock()
		time.Sleep(time.Second)
	}

	// start reduce phase
	c.initReducePhase()
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
		// do not assign task if that partition has already completed its reduce task
		if c.completedReducers[partition] {
			continue
		}
		c.taskPool <- Task{
			id:        uuid.New(),
			typ:       ReduceTask,
			status:    Idle,
			fileNames: fileNames,
			partition: partition,
		}
		fmt.Printf("Created reduce task for partition %d with files: %v\n", partition, fileNames)
	}

	c.stateM.Lock()
	defer c.stateM.Unlock()
	c.state = Reducing
}

func extractReducePartition(fileName string) int {
	var reducePartition int
	_, err := fmt.Sscanf(fileName, "mr-%*d-%d", &reducePartition)
	if err != nil {
		log.Fatalf("failed to extract reduce partition from file name %s: %v", fileName, err)
	}
	return reducePartition
}
