package mr

import (
	"context"
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

type Coordinator struct {
	workers            map[uuid.UUID]*WorkerMetadata
	tasks              map[uuid.UUID]map[uuid.UUID]*Task // map of worker ID to map of taskId to Task
	wm                 sync.Mutex
	tm                 sync.Mutex
	taskPool           chan Task
	nReduce            int
	nInitialInputFiles int
	wg                 sync.WaitGroup
	ctx                context.Context
	cancel             context.CancelFunc

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

	// if the task is already marked as completed, ignore it
	if args != nil && c.tasks[args.WorkerId] != nil &&
		c.tasks[args.WorkerId][args.TaskId] != nil &&
		c.tasks[args.WorkerId][args.TaskId].Status == Completed {
		return nil
	}

	c.tasks[args.WorkerId][args.TaskId].Status = Completed

	if c.tasks[args.WorkerId][args.TaskId].Typ == MapTask {
		c.tasks[args.WorkerId][args.TaskId].FileNames = args.IntermediateFiles
	}

	if c.tasks[args.WorkerId][args.TaskId].Typ == ReduceTask {
		c.completedReducers[c.tasks[args.WorkerId][args.TaskId].Partition] = true
	}

	// if all reduce tasks are completed, mark state as done
	if c.state == Reducing && len(c.completedReducers) == c.nReduce {
		c.state = Done
		c.cancel()
	}

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

func (c *Coordinator) Done() bool {
	select {
	case <-c.ctx.Done():
		close(c.taskPool)
		return true
	default:
		return false
	}
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	ctx, cancel := context.WithCancel(context.Background())

	c := Coordinator{
		workers:            make(map[uuid.UUID]*WorkerMetadata),
		tasks:              make(map[uuid.UUID]map[uuid.UUID]*Task), // map of worker ID to map of taskId to Task
		taskPool:           make(chan Task, len(files)),             // buffer size equal to number of tasks
		tm:                 sync.Mutex{},
		wm:                 sync.Mutex{},
		nInitialInputFiles: len(files),
		nReduce:            nReduce,
		wg:                 sync.WaitGroup{},
		ctx:                ctx,
		cancel:             cancel,
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

func (c *Coordinator) handleFailedWorkerTasks(failedWorkerId uuid.UUID) {
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
}

func (c *Coordinator) heartbeatMonitor() {
	for {
		select {
		case <-c.ctx.Done():
			return
		default:

		}

		c.wm.Lock()
		failedWorkers := []uuid.UUID{}
		for workerId, workerMetadata := range c.workers {
			if !workerMetadata.failed && time.Since(workerMetadata.lastHeartbeat) > 10*time.Second {
				workerMetadata.failed = true
				failedWorkers = append(failedWorkers, workerId)
			}
		}

		for _, workerId := range failedWorkers {
			c.handleFailedWorkerTasks(workerId)
		}

		c.wm.Unlock()
		time.Sleep(time.Second)
	}
}

func (c *Coordinator) mapTaskMonitor() {
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

		if count == c.nInitialInputFiles {
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
