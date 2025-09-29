package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"

	"github.com/google/uuid"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

var workerId uuid.UUID

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	workerId = uuid.New()

	registerWorker()

	go heartbeat()

	for {
		reqTaskReply := requestTask()
		if reqTaskReply == nil {
			time.Sleep(time.Second)
			continue
		}

		if len(reqTaskReply.Task.FileNames) == 0 {
			time.Sleep(time.Second)
			continue
		}

		task := reqTaskReply.Task
		nReduce := reqTaskReply.NReduce

		switch task.Typ {
		case MapTask:
			executeMapTask(&task, nReduce, mapf)
		case ReduceTask:
			executeReduceTask(&task, reducef)
		}
		time.Sleep(time.Second)
	}
}

// Calls the Register RPC with the workerId
func registerWorker() {
	regArgs := RegisterArgs{WorkerId: workerId}
	regReply := RegisterReply{}
	ok := call("Coordinator.Register", &regArgs, &regReply)
	if !ok {
		log.Fatalf("Worker registration failed")
	}
}

// Runs the Heartbeat RPC every 2 seconds
func heartbeat() {
	for {
		hbArgs := HeartbeatArgs{WorkerId: workerId}
		hbReply := HeartbeatReply{}
		call("Coordinator.Heartbeat", &hbArgs, &hbReply)
		time.Sleep(2 * time.Second)
	}
}

// Requests a task from the coordinator
// Returns nil if no tasks available yet
func requestTask() *RequestTaskReply {
	reqArgs := RequestTaskArgs{WorkerId: workerId}
	reqReply := RequestTaskReply{}
	ok := call("Coordinator.RequestTask", &reqArgs, &reqReply)
	if !ok {
		return nil
	}
	return &reqReply
}

func executeMapTask(task *Task, nReduce int, mapf func(string, string) []KeyValue) {
	inputFile := task.FileNames[0]
	file, err := os.Open(inputFile)
	if err != nil {
		log.Fatalf("cannot open %v", inputFile)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", inputFile)
	}
	file.Close()
	kva := mapf(inputFile, string(content))

	partitionedFiles, err := writeIntermediateFiles(task.Id, kva, nReduce)
	if err != nil {
		log.Fatalf("Failed to write intermediate file: %v", err)
	}

	completeTask(task, partitionedFiles)
}

// writes intermedaite files to appropriate reducer's file
// returns list of intermediate file names on success
func writeIntermediateFiles(taskId uuid.UUID, kvs []KeyValue, nReduce int) ([]string, error) {
	encoders := make([]*json.Encoder, nReduce)
	tmpFiles := make([]*os.File, nReduce)
	finalNames := make([]string, nReduce)

	for i := 0; i < nReduce; i++ {
		finalName := fmt.Sprintf("mr-%s-%s-%d", taskId.String(), workerId.String(), i)
		tmpFile, err := os.CreateTemp("", finalName+"-*")
		if err != nil {
			return nil, fmt.Errorf("failed to create temp tile for %s: %v", finalName, err)
		}

		tmpFiles[i] = tmpFile
		encoders[i] = json.NewEncoder(tmpFile)
		finalNames[i] = finalName
	}

	for _, kv := range kvs {
		reduceIdx := ihash(kv.Key) % nReduce
		if err := encoders[reduceIdx].Encode(&kv); err != nil {
			return nil, fmt.Errorf("failed to encode key-value %v: %v", kv, err)
		}
	}

	for i, tmpFile := range tmpFiles {
		if err := tmpFile.Close(); err != nil {
			return nil, fmt.Errorf("failed to close temp file: %v", err)
		}
		if err := os.Rename(tmpFile.Name(), finalNames[i]); err != nil {
			return nil, fmt.Errorf("failed to rename temp file to %s: %v", finalNames[i], err)
		}
	}

	return finalNames, nil
}

// Notify coordinator of completed task
func completeTask(task *Task, filenames []string) {
	completeArgs := CompleteTaskArgs{
		WorkerId:          workerId,
		TaskId:            task.Id,
		IntermediateFiles: filenames,
	}
	completeReply := CompleteTaskReply{}
	call("Coordinator.CompleteTask", &completeArgs, &completeReply)
}

func executeReduceTask(task *Task, reducef func(string, []string) string) {
	intermediate := []KeyValue{}
	for _, fileName := range task.FileNames {
		f, err := os.Open(fileName)
		if err != nil {
			log.Fatalf("cannot open intermediate file %v: %v", fileName, err)
		}

		dec := json.NewDecoder(f)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				if err == io.EOF {
					break
				}
				log.Fatalf("decode error in %s: %v", fileName, err)
			}
			intermediate = append(intermediate, kv)
		}
		f.Close()
	}

	sort.Sort(ByKey(intermediate))

	fileName := fmt.Sprintf("mr-out-%d", task.Partition)
	outputFile, err := os.Create(fileName)
	if err != nil {
		log.Fatalf("cannot create output file %v: %v", outputFile, err)
	}
	defer outputFile.Close()

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(outputFile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	completeTask(task, nil)
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	return false
}
