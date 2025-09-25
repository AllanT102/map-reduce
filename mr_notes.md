## High Level Background of MapReduce
- distribute simple computations on large datasets on multiple machines

## How it works
1. Split input file into M pieces (16/64MB)
2. Spin up many copies of a program on cluster of machines
3. One copy is a master, all others are workers. Master assigns M map tasks and R reduce tasks by picking up idle workers.
4. Map task worker reads input, parses key/value pairs and passes it into a map function. Intermediate kv pairs are buffered in memory.
5. Buffered pairs are partitioned into local disk. Locations are sent back to the master.
6. Master notifies reducers. Reducers use RPCs to read the data from those locations. 
7. Reducers sort the keys to group identical keys. Use external sort if too many keys and can't fit in memory
8. Reducer iterates over all intermediate data, pass key and values to reducer. Output is appended to final output file for this reducer partition.
9. Master wakes up user program once all map/reduce tasks done.

R output files 

Coordinator Responsibilities
- keep track of state (idle, in progress, completed)
- keep track of identity of worker machines (non-idle tasks)
- store locations of intermediate file regions produced by map task
- information about these files a re pushed to in progress reduce tasks

## Implementing Fault Tolerance

Worker Failures

Coordinator Failures


## Lecture Guide on Lab 01

Understand sequential implementation of MapReduce 
Understand the distributed MapReduce scaffolding code
Learn RPC mechanism (pkg.go.dev/net/rpc)
- how does http.Servce() work? New Goroutine every request?
- Whuy unix socket vs TCP socket? Can you switch it?
- make sure Example RPC works before you proceed

## Implement MR-independent RPCs
- only coordinator has RPC right now
- can make worker an RPC server too

RPC Interface for Coordinator
- Register - args: worker_id - response: ack – alternative is for the coordinator to send back a registered worker_id
- Ping (health check)
- TaskRPC(worker_id) – response ()
- Retry?
- Shutdown?
- TaskComplete RPC - coordinator updates task status - args: metadata for intermediate files

Data Structures on Coordinator Side
worker_id to state
Time since last heartbeat 

On coordinator side, run goroutine to check heartbeat status on all workers to determine status. 

## Implementing the Map Phase

Worker sends Task RPC to coordinator and receives a task (file) – map task
Coordinator.initMapTasks()
- TaskId -> MapTaskStatus
- c.MapTasksMonitor periodically checks if the map phase is done.
- If taken too long, you can mark it as unassigned so someone else can pick it up


## Implementing Reduce Phase
- c.MapTasksMonitor
- Coordinator.initReduceTasks()
- Coordinator.TaskRPC
- coordinator.TaskCompleteRPC

for fault tolerance, c.

## Concurrency
on worker side, no race condtitions. But coordinator does. Assignment of tasks does.

## Testing 

bash test-mr.sh
maybeCrash()