package CraneNode

import (
	cmsg "Crane/CraneMessage"
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"google.golang.org/grpc"
)

// Master have a long running RPC server waiting for jobs (topos)
// It keeps a list of all available worker machines in the system
// Determine how to assign jobs to these machines
// And send RPC requests to all the worker machines


// this function split the tasks and also assign corresponding address to connect
func (m *CraneMaster) splitTasks() {

	numWorkers := len(m.workers)
	remain := len(m.allTasks) % numWorkers
	var sliceLength int
	if remain == 0 {
		sliceLength = len(m.allTasks) / numWorkers
	} else {
		sliceLength = (len(m.allTasks) + numWorkers - remain) / numWorkers
	}
	println("slice len:", sliceLength)
	//sliceLength := len(m.allTasks) / numWorkers
	upper := 0
	for i := 0; i <= numWorkers-1; i++ {
		if upper < len(m.allTasks) { // if there are tasks to assign
			if (i+1)*sliceLength >= len(m.allTasks) {
				upper = len(m.allTasks)
			} else {
				upper += sliceLength
			}
			m.taskMapping[m.workers[i].HostName] =
				m.allTasks[i*sliceLength : upper]
			for j, taskName := range m.taskMapping[m.workers[i].HostName] {
				item, ok := m.topology.Items[taskName]
				if !ok {
					println("missed node", taskName)
					panic("node is not exist, WTF")
				}
				myselfItem := item.Myself
				myselfItem.Ip = m.workers[i].HostIp
				myselfItem.Port = STARTPORT + int64(j)
				// starting from STARTPORT
				// assigning port and ip to myself
			}
		} else {
			m.taskMapping[m.workers[i].HostName] = nil
		}
	}

	for _, v := range m.topology.Items {
		if v.ComponentType == cmsg.CompType_SPOUT {
			m.spouts = append(m.spouts, *v.Myself)
		}

		for _, sendTo := range v.Grouping {
			dstItem := m.topology.Items[sendTo.ItemName].Myself
			sendTo.Ip = dstItem.Ip
			sendTo.Port = dstItem.Port
			// assigning sendTo port and ip
		}
	}
}

func (m *CraneMaster) sendTasks(
	worker cmsg.Node, tasks []*cmsg.TopoItem, wg *sync.WaitGroup) {
	defer wg.Done()

	var opts []grpc.DialOption
	opts = append(opts, grpc.WithInsecure(), grpc.WithBlock(),
		grpc.WithTimeout(2*time.Second))
	conn, err := grpc.Dial(
		fmt.Sprintf("%s:%d", worker.HostIp, worker.Port), opts...)
	if err != nil {
		log.Panicf("[Master] Error connecting to worker %s! %s",
			worker.HostName, err)
	}

	defer conn.Close()

	clnt := cmsg.NewCraneClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)

	defer cancel()

	topoReq := &cmsg.Topology{
		JobBinName: m.jobBinName,
		Components: tasks,
	}
	res, err := clnt.AddComponent(ctx, topoReq)
	if err != nil {
		log.Fatalf("[Master] AddComponent fail! %s", err)
	}

	// Send the response from worker to client
	(*m.clientConn).Send(res)

}

func (m *CraneMaster) assignComponent() *cmsg.OpResponse {
	m.splitTasks()

	var wg sync.WaitGroup
	if m.workers == nil {
		log.Fatalf("[Master] No active worker available!")
	}
	for _, w := range m.workers {
		wg.Add(1)
		var tasks []*cmsg.TopoItem
		for _, taskName := range m.taskMapping[w.HostName] {
			tasks = append(tasks, m.topology.Items[taskName])
		}
		go m.sendTasks(w, tasks, &wg)
	}
	wg.Wait()
	res := &cmsg.OpResponse{
		Status:     cmsg.OpStatus_OK,
		RetMessage: "All components started.",
	}

	return res
}

func (m *CraneMaster) updateTopology(topology *cmsg.Topology) {
	log.Print(topology)
	m.topology = NewWithTopo(topology)
	m.allTasks = m.topology.GetTopoSort()
	log.Print(m.allTasks)
}

func (m *CraneMaster) informSpout(spout cmsg.Component) *cmsg.OpResponse {
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithInsecure(), grpc.WithBlock(),
		grpc.WithTimeout(2*time.Second))
	conn, err := grpc.Dial(
		fmt.Sprintf("%s:%d", spout.Ip, spout.Port), opts...)
	if err != nil {
		log.Fatalf("[Master] Connecting to Spout fail. %s", err)
	}
	defer conn.Close()

	clnt := cmsg.NewTaskClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(),
		5*time.Second)
	defer cancel()

	opReq := &cmsg.OpRequest{
		Status: cmsg.OpStatus_STA,
	}
	res, err := clnt.StartSpout(ctx, opReq)
	if err != nil {
		log.Fatalf("[Master] Start Spout fail! %s", err)
	}
	return res
}

func (m *CraneMaster) stopWorker(wg *sync.WaitGroup, worker cmsg.Node) {
	defer wg.Done()

	var opts []grpc.DialOption
	opts = append(opts, grpc.WithInsecure(), grpc.WithBlock(),
		grpc.WithTimeout(2*time.Second))
	conn, err := grpc.Dial(
		fmt.Sprintf("%s:%d", worker.HostIp, worker.Port), opts...)
	if err != nil {
		log.Fatalf("[Master Fail Recovery] Connecting to worker fail."+
			" %s", err)
	}
	defer conn.Close()

	clnt := cmsg.NewCraneClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(),
		5*time.Second)
	defer cancel()

	opReq := &cmsg.OpRequest{
		Status: cmsg.OpStatus_STA,
	}
	res, err := clnt.FenceWorker(ctx, opReq)
	if err != nil {
		log.Fatalf("[Master] Start Spout fail! %s", err)
	}
	log.Printf("[Master Fail Recovery] %s", res.RetMessage)
	
}
