package main

import (
	msrv "Membership/MembershipServer"
	snode "SDFS/SDFSNode"

	"bufio"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
)

func main() {
	fmt.Println("This is SDFS Server!")

	ip := flag.String("localIP", "0.0.0.0", "...")
	port := flag.Int("localPort", 8080, "...")
	masterHost := flag.String(
		"masterIP",
		"fa18-cs425-g39-06.cs.illinois.edu",
		"...",
	)

	selfConfig := flag.String("selfConfig", "/etc/cs425/my_info.json", "...")
	flag.Parse()

	// Init struct for membership server
	msrvSelf := &msrv.MemberInfo{}
	otherMember := &msrv.MembershipList{}
	igrc := &msrv.InterGoroutineComm{}
	mServer := msrv.MembershipServer{}
	mServer.SetMyself(msrvSelf)
	mServer.SetOthers(otherMember)
	mServer.SetConn(nil)
	mServer.SetIgrc(igrc)
	mServer.Frc = make(chan string)

	// Init struct for SDFS server
	s := snode.SDFSServer{}
	s.SetMemberServer(&mServer)

	conn, err := net.Listen("tcp", fmt.Sprintf("%s:%d", *ip, *port))
	if err != nil {
		log.Fatalf("listen to %s failed!", ip)
	} else {
		s.SetConn(conn)
	}

	// Determine if I am master
	selfInfo := msrv.GetSelfInfo(*selfConfig, *port)
	s.SetIsMaster(selfInfo.IP == *masterHost)

	// Set self port and ip addr
	sdfsSelf, err := net.ResolveTCPAddr("tcp",
		fmt.Sprintf("%s:%d", selfInfo.IP, selfInfo.Port))
	if err != nil {
		log.Panicf("Resolve tcp addr %s:%d failed! %s",
			selfInfo.IP, selfInfo.Port, err)
	}
	s.SetHostname(selfInfo.Hostname)
	s.SetMyself(sdfsSelf)

	// Set the master's port and ip addr
	// regardless of whether I am master
	master, err := net.ResolveTCPAddr("tcp",
		fmt.Sprintf("%s:%d", *masterHost, *port))
	if err != nil {
		log.Fatal("Resolve tcp addr failed! %s", err)
	} else {
		s.SetMaster(master)
	}

	// start handling command
	for {
		fmt.Print("Enter Command: ")
		reader := bufio.NewReader(os.Stdin)
		cmd, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				break
			} else {
				log.Panic(err)
			}
		}
		s.HandleCommand(cmd)
	}
}
