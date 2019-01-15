package CraneNode

import (
	cmsg "Crane/CraneMessage"
	ctup "Crane/CraneTuple"
	"context"
	"fmt"
	"io"
	"log"
	"runtime"
	"sync"
)

// Client(C) -> Master(S)
func (s *CraneServer) UploadTopo(
	topo *cmsg.Topology, srv cmsg.Crane_UploadTopoServer) error {
	var res *cmsg.OpResponse
	s.master.clientConn = &srv
	s.master.jobBinName = topo.JobBinName

	s.master.updateTopology(topo)
	res = &cmsg.OpResponse{
		Status:     cmsg.OpStatus_OK,
		RetMessage: "Finish Uploading Topology.",
	}
	(*s.master.clientConn).Send(res)

	s.master.workers = s.getActiveServerList()
	res = s.master.assignComponent()
	(*s.master.clientConn).Send(res)

	for _, spout := range s.master.spouts {
		res = s.master.informSpout(spout)
		(*s.master.clientConn).Send(res)
	}

	res = &cmsg.OpResponse{
		Status:     cmsg.OpStatus_FIN,
		RetMessage: "Job started. Awaiting finish",
	}
	return nil
}

// Master(C) -> Worker(S)
func (s *CraneServer) AddComponent(
	ctx context.Context, topo *cmsg.Topology,
) (*cmsg.OpResponse, error) {
	// Setup worker process
	s.worker.tasks = topo.Components
	var tasksAtWorker = ""

	// Pull job binary from SDFS to local
	ljp := fmt.Sprintf("/testfiles/%s", topo.JobBinName) //local job path

	jplugin := s.worker.GetJobBinary(topo.JobBinName, ljp)

	s.worker.tserver = []*TaskServer{}
	// Start rpc servers
	var wg sync.WaitGroup
	for _, tsk := range s.worker.tasks {
		wg.Add(1)
		tasksAtWorker += fmt.Sprintf("%s; ", tsk.FuncName)
		// Setup task server
		t := &TaskServer{
			task: tsk,
		}

		go t.InitTask(&wg, jplugin)
		s.worker.tserver = append(s.worker.tserver, t)
	}
	wg.Wait()
	res := &cmsg.OpResponse{
		Status: cmsg.OpStatus_OK,
		RetMessage: fmt.Sprintf("Worker %s finished init task %s",
			s.myself.HostName, tasksAtWorker),
	}
	return res, nil
}

// Worker(C) -> Master(S)
func (s *CraneServer) FailureReport(
	ctx context.Context, req *cmsg.FailureReportRequest,
) (*cmsg.OpResponse, error) {
	if s.role != MASTER {
		log.Fatalf("[Failure Report] %s not Master!",
			s.myself.HostName)
	}

	var wg sync.WaitGroup
	for _, w := range s.master.workers {
		wg.Add(1)
		go s.master.stopWorker(&wg, w)
	}
	wg.Wait()

	// Below is a copy paste from Server.UploadTopo
	s.master.workers = s.getActiveServerList()
	res := s.master.assignComponent()
	(*s.master.clientConn).Send(res)

	for _, spout := range s.master.spouts {
		res = s.master.informSpout(spout)
		(*s.master.clientConn).Send(res)
	}

	res = &cmsg.OpResponse{
		Status:     cmsg.OpStatus_FIN,
		RetMessage: "Job re-started. Awaiting finish",
	}

	return res, nil
}

// Task[Spout/Bolt](C) -> Task[Bolt](S)
func (t *TaskServer) EmitTuple(in cmsg.Task_EmitTupleServer) error {

	// Server receive incoming tuple
	for {
		tuple, err := in.Recv()
		if err == io.EOF {
			log.Printf("[Task %s] Other end send EOF",
				t.task.FuncName)
			finalRes := &cmsg.OpResponse{
				Status: cmsg.OpStatus_FIN,
				RetMessage: fmt.Sprintf("%s %s done",
					t.task.ComponentType, t.task.FuncName),
			}

			return in.SendAndClose(finalRes)
		}
		if err != nil {
			return err
		}

		// Get Crane representation of the tuple
		ctuple := ProtoToCraneTuple(*tuple)
		log.Printf("[Task %s] Old tuple: %v", t.task.FuncName, ctuple)

		fn := fmt.Sprintf("%sExecute", t.task.FuncName)
		curExe, err := t.jobPlugin.Lookup(fn)
		if err != nil {
			log.Fatalf("[Task] Lookup func %s failed! %s", fn, err)
		}

		// Execute the function and get output tuple
		newTuple, err := curExe.(func(ctup.CraneTuple) (
			ctup.CraneTuple, error))(ctuple)
		if err != nil {
			log.Fatalf("[Task] Task execution fail! %s", err)
		}
		log.Printf("[Task %s] New Tuple: %v", t.task.FuncName, newTuple)
		t.tupleChan <- newTuple
		// If there is a Grouping field defined,
		// Set up a EmitTuple Client and send to all the grouping
		runtime.Gosched()
	}
}

// Master(C) -> Task[spout](S)
func (t *TaskServer) StartSpout(ctx context.Context, req *cmsg.OpRequest,
) (*cmsg.OpResponse, error) {
	if req.Status == cmsg.OpStatus_STA {
		if t.task.Grouping == nil {
			log.Fatal("[Spout] No outgoing components!")
		}
		for _, comp := range t.task.Grouping {
			go t.startSpoutFunc(comp)
		}
	}
	ret := &cmsg.OpResponse{
		Status:     cmsg.OpStatus_OK,
		RetMessage: fmt.Sprintf("Spout %s started", t.task.FuncName),
	}

	return ret, nil

}

// Master(C) -> Worker(S)
func (s *CraneServer) FenceWorker(ctx context.Context, req *cmsg.OpRequest,
) (*cmsg.OpResponse, error) {
	s.worker.tasks = nil
	if s.worker.tserver != nil {
		for _, ts := range s.worker.tserver {
			ts.task = nil
			for _, v := range ts.clientConn {
				(*v).CloseAndRecv()
			}
			if ts.serverConn != nil {
				ts.serverConn.Close()
			}
			
			if ts.localFd != nil {
				ts.localFd.Close()
			}
			close(ts.tupleChan)
		}
		s.worker.tserver = nil
	}
	ret := &cmsg.OpResponse{
		Status: cmsg.OpStatus_OK,
		RetMessage: "Worker cleaned up.",
	}
	return ret, nil
}
