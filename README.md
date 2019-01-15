# Crane
UIUC CS425 Project

## Design
Crane is a stream processing framework that works like Apache Storm. It is composed of three components: CraneClient, CraneMaster, CraneWorker (with multiple Cranetasks).
- CraneClient: The component to interact with users (upload user’s compiled tasks and topology)
- CraneMaster: The component to schedule and do book keeping for the whole system
- CraneWorker: The component to create goroutine to finish the task (either bolt or spout). There is only one CraneWorker on one machine, but there could be multiple tasks. Tasks communicate with each other directly

Using a figure to better illustrate:
![arch](https://github.com/ElaineAng/Crane/blob/master/crane-arch.png)

### During a normal execution
1. Sort the tasks: Once the CraneMaster received the topology submitted by client, it runs topological sort on the computational graph, and assigned tasks to all the CraneWorkers as even as possible. 
2. Then for each of the alive CraneWorker, CraneMaster send a RPC request to it, which contains part of the topology for that worker to run. Upon receiving that request, worker first do a GET to SDFS (Simple Distributed File System), pull the job binary, then loop through all the tasks (components), starting a RPC server for each of the component. 
3. Start the streaming: Once all CraneWorkers finish setup task routine, master will send a notification to the Spout task, asking it to start reading in data. Each of the task, knowing what function it is expected to execute, taking input CraneTuple, loading its corresponding function from the job binary, execute it, and send the resulting CraneTuple out to all its successors.
4. The Spout generates an EOF upon finishing reading in the data, that EOF gets propagated through the whole topology. When the last Bolt (sink) receives that EOF, it flushes all the processed data to local disk, and then to SDFS.

### Failure detection
All Crane nodes run a membership protocol. When a CraneWorker failed, some other node will detect that failure, and report the failing information to CraneMaster. Master, upon receiving that information, will first fence all alive worker nodes, cleaning up its remaining state, close connections, etc. Then master will re-compute the task-worker machine mapping based on alive machines, and then restart the whole computation from begining.

