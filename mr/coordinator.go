package mr

import (
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
	status TaskStatus
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



*/
type Coordinator struct {
	mappers  map[uuid.UUID]map[uuid.UUID]*Task
	reducers map[uuid.UUID]map[uuid.UUID]*Task
	mMutex   sync.Mutex
	rMutex   sync.Mutex
}

func (c *Coordinator) Register(args *RegisterArgs, reply *RegisterReply) error {
	// Your code here.
	return nil
}

func (c *Coordinator) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	// Your code here.

	return nil
}

func (c *Coordinator) CompleteTask(args *CompleteTaskArgs, reply *CompleteTaskReply) error {
	// Your code here.

	return nil
}

func (c *Coordinator) Heartbeat(args *HeartbeatArgs, reply *HeartbeatReply) error {
	// Your code here.

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
	ret := false

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.

	c.server()
	return &c
}
