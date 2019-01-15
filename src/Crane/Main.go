package main

import (
	cnode "Crane/CraneNode"
	msrv "Membership/MembershipServer"
	"flag"
	"os"
)

// Start by ./Crane <M|W>

func main() {
	port := flag.Int("Port", 9000, "...")
	master := flag.String(
		"m", "fa18-cs425-g39-06.cs.illinois.edu", "...")
	flag.Parse()
	
	// Init struct for membership server
	msrvSelf := &msrv.MemberInfo{}
	otherMember := &msrv.MembershipList{}
	igrc := &msrv.InterGoroutineComm{}
	mServer := &msrv.MembershipServer{}
	mServer.SetMyself(msrvSelf)
	mServer.SetOthers(otherMember)
	mServer.SetConn(nil)
	mServer.SetIgrc(igrc)
	mServer.Frc = make(chan string)

	// Init struct for Crane master
	s := cnode.CraneServer{}
	s.SetMemberServer(mServer)
	s.SetRole(os.Args[1])
	s.StartServer(*port, *master)

}
