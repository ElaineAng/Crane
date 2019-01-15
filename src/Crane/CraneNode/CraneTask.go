package CraneNode

import (
	cmsg "Crane/CraneMessage"
	ctup "Crane/CraneTuple"
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"plugin"
	"runtime"
	"sync"
	"time"

	"google.golang.org/grpc"
)

func (t *TaskServer) SendNewTuple() {
	if t.task.Grouping != nil {
		// Setup client conn to next component
		for _, comp := range t.task.Grouping {
			_, exist := t.clientConn[comp.ItemName]
			if !exist {
				conn, cancel :=
					t.SetupEmitTupleClient(*comp)
				defer conn.Close()
				defer cancel()
			}
		}
	} else {
		// Open a file to write to
		if t.localFd == nil {
			var err error
			t.localFd, err = os.Create(
				fmt.Sprintf("/testfiles/%s.out", t.task.FuncName))
			if err != nil {
				log.Fatalf("Cannot open local file for sink! %s", err)
			}
			defer t.localFd.Close()
		}
	}

	for {
		if newTuple, ok := <-t.tupleChan; ok {
			if t.task.Grouping != nil {
				for _, comp := range t.task.Grouping {
					cout, exist := t.clientConn[comp.ItemName]
					if !exist {
						log.Fatal("[Task] Client conn not set!")
					}
					log.Printf("[Task] Send %v to %s",
						newTuple, comp.ItemName)
					(*cout).Send(CraneToProtoTuple(newTuple))
				}
			} else {
				// If grouping is nil, this is the sink Bolt
				// Set up a SDFS client and write to SDFS
				log.Printf("[Sink] Tuple is %v", newTuple)
				t.localFd.Write(CraneToByteArray(newTuple))
			}
			runtime.Gosched()
		}
	}
}

func (t *TaskServer) InitTask(wg *sync.WaitGroup, jp *plugin.Plugin) {
	defer wg.Done()

	// Setup local fields
	t.clientConn = map[string]*cmsg.Task_EmitTupleClient{}
	t.tupleChan = make(chan ctup.CraneTuple, 100)
	if t.jobPlugin == nil {
		t.jobPlugin = jp
	}
	conn, err := net.Listen(
		"tcp",
		fmt.Sprintf("%s:%d",
			t.task.Myself.Ip, t.task.Myself.Port))
	t.serverConn = conn
	if err != nil {
		log.Fatalf("listen to %s:%d failed!",
			t.task.Myself.Ip, t.task.Myself.Port)
	}
	log.Printf("[Task] %s run at %s:%d", t.task.FuncName, t.task.Myself.Ip,
		t.task.Myself.Port)
	grpcServer := grpc.NewServer()
	cmsg.RegisterTaskServer(grpcServer, t)
	go grpcServer.Serve(conn)
	go t.SendNewTuple()
}

func (t *TaskServer) SetupEmitTupleClient(
	dst cmsg.Component) (
	*grpc.ClientConn,
	context.CancelFunc) {
	log.Printf("[Task Emit Pair] %s -> %s:%d:%s",
		t.task.FuncName, dst.Ip, dst.Port, dst.ItemName)
	// Setup new rpc client if not exist
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithInsecure(),
		grpc.WithBlock(), grpc.WithTimeout(10*time.Second))
	conn, err := grpc.Dial(
		fmt.Sprintf("%s:%d", dst.Ip, dst.Port), opts...)
	if err != nil {
		log.Fatalf("[Task] Cannot "+"Connect to %s", dst.ItemName)
	}
	clnt := cmsg.NewTaskClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(),
		600*time.Second)

	cout, err := clnt.EmitTuple(ctx)
	if err != nil {
		log.Fatalf("[Task] EmitTuple fail! %s", err)
	}

	t.lock.Lock()
	t.clientConn[dst.ItemName] = &cout
	t.lock.Unlock()

	return conn, cancel
}

func (t *TaskServer) startSpoutFunc(
	comp *cmsg.Component) {
	conn, cancel := t.SetupEmitTupleClient(*comp)
	defer conn.Close()
	defer cancel()

	fn := fmt.Sprintf("%sNextTuple", t.task.FuncName)
	curExe, err := t.jobPlugin.Lookup(fn)
	if err != nil {
		log.Fatalf("[Task] Lookup func %s failed! %s", fn, err)
	}

	for {
		newTuple, err := curExe.(func() (*ctup.CraneTuple, error))()
		cout, exist := t.clientConn[comp.ItemName]
		if !exist {
			log.Fatalf("[Task] Client connection not setup!")
		}

		if newTuple == nil {
			if err != nil {
				log.Fatal("[Spout] Calling NextTuple fail!")
			}

			_, err = (*cout).CloseAndRecv()
			if err != nil {
				log.Fatal("[Spout] close and recv fail!")
			}
			break
		} else {
			ptuple := CraneToProtoTuple(*newTuple)
			log.Print(*newTuple)
			(*cout).Send(ptuple)
		}
		time.Sleep(5 * time.Second)
		runtime.Gosched()
	}
}
