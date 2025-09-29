package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
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
	Id        uuid.UUID
	Typ       TaskType
	Status    TaskStatus
	FileNames []string // for map task, this is input file name; for reduce task, this is intermediate file names
	Partition int      // only used for reduce tasks
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

	reply.WorkerId = args.WorkerId

	return nil
}

func (c *Coordinator) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	c.tm.Lock()
	defer c.tm.Unlock()

	fmt.Printf("Coordinator state %v, len(taskPool)=%d\n", c.state, len(c.taskPool))

	select {
	case task, ok := <-c.taskPool:
		if !ok {
			return nil
		}

		task.Status = InProgress
		reply.Task = task

		if task.Typ == MapTask {
			reply.NReduce = c.nReduce
		}

		// first task assigned to worker
		if c.tasks[args.WorkerId] == nil {
			c.tasks[args.WorkerId] = make(map[uuid.UUID]*Task)
		}
		c.tasks[args.WorkerId][task.Id] = &task
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

	fmt.Printf("CompleteTask called: worker=%v, task=%v\n", args.WorkerId, args.TaskId)

	// if the task is already marked as completed, ignore it
	if args != nil && c.tasks[args.WorkerId] != nil &&
		c.tasks[args.WorkerId][args.TaskId] != nil &&
		c.tasks[args.WorkerId][args.TaskId].Status == Completed {
		fmt.Printf("Task %v from worker %v already completed, ignoring\n", args.TaskId, args.WorkerId)
		return nil
	}

	c.tasks[args.WorkerId][args.TaskId].Status = Completed
	fmt.Printf("Task %v from worker %v marked as Completed\n", args.TaskId, args.WorkerId)

	if c.tasks[args.WorkerId][args.TaskId].Typ == MapTask {
		c.tasks[args.WorkerId][args.TaskId].FileNames = args.IntermediateFiles
	}

	if c.tasks[args.WorkerId][args.TaskId].Typ == ReduceTask {
		c.completedReducers[c.tasks[args.WorkerId][args.TaskId].Partition] = true
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
		taskPool:           make(chan Task, len(files)+nReduce),     // buffer size equal to number of tasks
		tm:                 sync.Mutex{},
		wm:                 sync.Mutex{},
		nInitialInputFiles: len(files),
		nReduce:            nReduce,
		wg:                 sync.WaitGroup{},
		doneCh:             make(chan struct{}, 1),
		state:              Mapping,
		stateM:             sync.Mutex{},
		completedReducers:  make(map[int]bool),
	}

	for _, file := range files {
		c.taskPool <- Task{
			Id:        uuid.New(),
			Typ:       MapTask,
			Status:    Idle,
			FileNames: []string{file},
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
	fmt.Println("Handling failed worker tasks for worker:", failedWorkerId)
	c.tm.Lock()
	c.stateM.Lock()
	defer c.tm.Unlock()
	defer c.stateM.Unlock()

	for _, task := range c.tasks[failedWorkerId] {
		if c.state == Reducing && task.Typ == ReduceTask && task.Status != Completed {
			task.Status = Idle
			c.taskPool <- *task
		}
		if c.state == Mapping && task.Typ == MapTask && task.Status != Completed {
			task.Status = Idle
			c.taskPool <- *task
		}
	}

	fmt.Println(len(c.taskPool), "tasks in the task pool after handling failed worker tasks")
	fmt.Println("Finished handling failed worker tasks for worker:", failedWorkerId)
}

/*
This function is called when a worker that executed a map task fails
*/
func (c *Coordinator) notifyMapTaskFailure() {
	// call worker RPC to reset, should store rpc ip in worker metadata
}

func (c *Coordinator) heartbeatMonitor() {
	for {
		if c.state == Done {
			return
		}

		// fmt.Println("heartbeat check")
		c.wm.Lock()
		failedWorkers := []uuid.UUID{}
		for workerId, workerMetadata := range c.workers {
			if !workerMetadata.failed && time.Since(workerMetadata.lastHeartbeat) > 10*time.Second {
				workerMetadata.failed = true
				failedWorkers = append(failedWorkers, workerId)
			}
		}
		c.wm.Unlock()

		for _, workerId := range failedWorkers {
			c.handleFailedWorkerTasks(workerId)
		}

		time.Sleep(time.Second)
	}
}

func (c *Coordinator) mapTaskMonitor() {
	// c.wg.Add(c.nInitialInputFiles)

	// // barrier
	// c.wg.Wait()
	for {
		c.tm.Lock()
		count := 0
		for _, workerTasks := range c.tasks {
			for _, task := range workerTasks {
				if task.Typ == MapTask && task.Status == Completed {
					count++
				}
			}
		}
		fmt.Printf("mapTaskMonitor: %d/%d map tasks completed\n", count, c.nInitialInputFiles)

		if count == c.nInitialInputFiles {
			fmt.Println("All map tasks completed, initializing reduce phase")
			c.tm.Unlock()
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

	// this is slow, try to optimize later if possible
	for _, workerTasks := range c.tasks {
		for _, task := range workerTasks {
			if task.Typ == MapTask && task.Status == Completed {
				for _, fileName := range task.FileNames {
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
			Id:        uuid.New(),
			Typ:       ReduceTask,
			Status:    Idle,
			FileNames: fileNames,
			Partition: partition,
		}
		fmt.Printf("Created reduce task for partition %d with files: %v\n", partition, fileNames)
		fmt.Printf("p2.Coordinator state %v, len(taskPool)=%d\n", c.state, len(c.taskPool))
	}

	c.stateM.Lock()
	defer c.stateM.Unlock()
	c.state = Reducing
}

func extractReducePartition(fileName string) int {
	parts := strings.Split(fileName, "-")
	numStr := parts[len(parts)-1]
	reducePartition, err := strconv.Atoi(numStr)
	if err != nil {
		log.Fatalf("failed to extract reduce partition from file name %s: %v", fileName, err)
	}
	return reducePartition
}
