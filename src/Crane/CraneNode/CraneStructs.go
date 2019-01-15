package CraneNode

import (
	cmsg "Crane/CraneMessage"
	ctup "Crane/CraneTuple"
	msrv "Membership/MembershipServer"
	"net"
	"os"
	"plugin"
	"sync"
)

const STARTPORT int64 = 10000 // set default start port to 10000

type Role int

// I hate this
var SDFSPort = 8080
var CranePort = 9000

const (
	MASTER Role = 1 + iota
	WORKER
	STNDBY
)

type CraneServer struct {
	mServer   *msrv.MembershipServer // membership server
	myself    *cmsg.Node
	role      Role
	master    *CraneMaster // Set if master, otherwise nil
	worker    *CraneWorker // Set if worker, otherwise nil
	jobPlugin *plugin.Plugin
}

type CraneMaster struct {
	topology    *CraneTopology
	allTasks    []string            // sorted tasks
	workers     []cmsg.Node         // worker nodes
	taskMapping map[string][]string // worker hostName -> task names
	clientConn  *cmsg.Crane_UploadTopoServer
	jobBinName  string
	spouts      []cmsg.Component
}

type CraneWorker struct {
	tasks   []*cmsg.TopoItem
	tserver []*TaskServer
}

type TaskServer struct {
	task       *cmsg.TopoItem
	clientConn map[string]*cmsg.Task_EmitTupleClient // item_name -> client
	lock       sync.Mutex
	serverConn net.Listener
	localFd    *os.File // Local file holding temporary sink data
	jobPlugin  *plugin.Plugin
	tupleChan  chan ctup.CraneTuple
}

func (m *CraneMaster) InitMaster() {
	m.taskMapping = map[string][]string{}
	m.spouts = []cmsg.Component{}
}

func (s *CraneServer) SetMemberServer(m *msrv.MembershipServer) {
	s.mServer = m
}

func (s *CraneServer) SetRole(r string) {
	if r == "M" {
		s.role = MASTER
	} else if r == "W" {
		s.role = WORKER
	} else {
		s.role = STNDBY
	}
}

func (s *CraneServer) InitMyself(hn string, ip string, port int) {
	s.myself = &cmsg.Node{
		HostName: hn,
		HostIp:   ip,
		Port:     int64(port),
	}
}
