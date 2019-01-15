package CraneNode

import (
	cmsg "Crane/CraneMessage"
	mmsg "Membership/MembershipMessage"
	msrv "Membership/MembershipServer"
	"context"
	"fmt"
	"log"
	"net"
	"runtime"
	"time"

	"google.golang.org/grpc"
)

func (s *CraneServer) StartServer(port int, master string) {
	s.mServer.JoinGroup(port)
	myself := msrv.GetSelfInfo("/etc/cs425/my_info.json", port)
	s.InitMyself(myself.Hostname, myself.IP, port)
	// Start different service depending on being master, standby or worker
	if s.role == MASTER {
		if myself.IP != master {
			log.Fatal("Inconsistent master info!!")
		}
		s.master = &CraneMaster{}
		s.master.InitMaster()
		log.Print("[Master] I am master.")
	} else if s.role == WORKER {
		s.worker = &CraneWorker{}
		log.Print("[Worker] I am worker.")
	} else {
		// Hot standby should periodically ping master
	}

	// Setup CraneServer grpc
	conn, err := net.Listen("tcp", fmt.Sprintf("%s:%d", myself.IP, port))
	if err != nil {
		log.Fatalf("[Server] Start grpc server fail! %s", err)
	}
	grpcServer := grpc.NewServer()
	cmsg.RegisterCraneServer(grpcServer, s)

	// Report failure to master
	if s.role != MASTER {
		go s.MonitorFailure(master, port)
	}
	grpcServer.Serve(conn)
}

// This function should only be called by master in Crane.
// The return list excluding calling node itself.
func (s *CraneServer) getActiveServerList() []cmsg.Node {
	// Ugly but I just don't want to change Membership struct.
	var iptoh = map[string]string{
		"172.22.158.128": "h1",
		"172.22.154.129": "h2",
		"172.22.156.129": "h3",
		"172.22.158.129": "h4",
		"172.22.154.130": "h5",
		"172.22.156.130": "h6",
		"172.22.158.130": "h7",
		"172.22.154.131": "h8",
		"172.22.156.131": "h9",
		"172.22.158.131": "h10",
	}
	var activeServers []cmsg.Node
	for _, server := range s.mServer.GetOthers() {
		if server.GetStatus() == mmsg.SrvStatus_ACTIVE {
			ipstr := server.GetAddr().IP.String()
			node := cmsg.Node{
				HostName: iptoh[ipstr],
				HostIp:   ipstr,
				Port:     int64(server.GetAddr().Port),
			}
			if s.myself.HostName != node.HostName {
				activeServers = append(activeServers, node)
			}
		}
	}
	return activeServers
}

func (s *CraneServer) MonitorFailure(ip string, port int) {
	for {
		if failIp, ok := <-s.mServer.Frc; ok {
			log.Printf("SDFS Server %s failed!", failIp)

			// Assuming all hosts listens to the same port
			failNode := &cmsg.Node{
				HostIp: failIp,
				Port:   int64(s.myself.Port),
			}

			var opts []grpc.DialOption
			opts = append(opts, grpc.WithInsecure(),
				grpc.WithBlock(), grpc.WithTimeout(5*time.Second))
			conn, err := grpc.Dial(
				fmt.Sprintf("%s:%d", ip, port), opts...)
			if err != nil {
				log.Fatal("[Failure report] Connect to master"+
					"failure! %s", err)
			}
			defer conn.Close()

			clnt := cmsg.NewCraneClient(conn)
			ctx, cancel := context.WithTimeout(
				context.Background(), 300*time.Second)
			defer cancel()
			req := &cmsg.FailureReportRequest{
				FailedNode: failNode,
			}
			_, err = clnt.FailureReport(ctx, req)
			if err != nil {
				log.Fatalf("Failure report failed! %s", err)
			} else {
				log.Printf("[Failure Report] Finish reporting."+
					" %s", err)
			}

		} else {
			log.Panic("Failure report channel is closed!")
		}
		runtime.Gosched()
	}

}
