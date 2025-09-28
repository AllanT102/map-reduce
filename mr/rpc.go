package mr

import (
	"os"
	"strconv"

	"github.com/google/uuid"
)

type RegisterArgs struct {
	WorkerId uuid.UUID
}

type RegisterReply struct {
	WorkerId uuid.UUID
}
type RequestTaskArgs struct {
	WorkerId uuid.UUID
}

type RequestTaskReply struct {
	Task      Task
	NReduce int
}

type CompleteTaskArgs struct {
	WorkerId uuid.UUID
	TaskId uuid.UUID
	IntermediateFiles[] string
}

type CompleteTaskReply struct {
	// empty for now
}

type HeartbeatArgs struct {
	WorkerId uuid.UUID
}

type HeartbeatReply struct {
	// empty for now
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
func coordinatorSock() string {
	s := "/var/tmp/416-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
