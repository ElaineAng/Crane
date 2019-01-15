package MembershipServer

import (
	mmsg "Membership/MembershipMessage"
	"fmt"
	"log"
	"math/rand"
	"net"
	"sync"
	"time"

	"github.com/emirpasic/gods/utils"

	tmap "github.com/emirpasic/gods/maps/treemap"

	"github.com/golang/protobuf/proto"
)

type Server interface {
	JoinGroup()
	LeaveGroup()
	Ping()
	Update()
	RecvMsg()
}

type MemberInfo struct {
	id        uint64
	addr      *net.UDPAddr
	timestamp uint64
	status    mmsg.SrvStatus
}

type MembershipList struct {
	members *tmap.Map // store (id, *MemberInfo)
	lock    sync.Mutex
}

type InterGoroutineComm struct {
	stopRecv chan int
	recvDone chan int
	stopPing chan int
	pingDone chan int
}

type MembershipServer struct {
	myself  *MemberInfo
	others  *MembershipList
	conn    *net.UDPConn
	igrc    *InterGoroutineComm
	ackList *map[uint64]bool
	Frc     chan string // Failure Report Channel
}

func (s *MembershipServer) SetMyself(mself *MemberInfo) {
	s.myself = mself
}

func (s *MembershipServer) SetOthers(no *MembershipList) {
	s.others = no
}

func (s *MembershipServer) GetOthers() []*MemberInfo {
	return s.others.ToList()
}
func (s *MembershipServer) SetConn(nc *net.UDPConn) {
	s.conn = nc
}

func (s *MembershipServer) SetIgrc(ni *InterGoroutineComm) {
	s.igrc = ni
}

// Start server set up the UDP listener, and necessary server struct
func (s *MembershipServer) StartServer(myInfo *hostConfig) {
	myAddr, ok := HostConfToUDP(myInfo)
	if !ok {
		log.Fatal("Host config is not correct")
	}

	// This connection closes when leave() gets called
	serverConn, err := net.ListenUDP("udp", myAddr)
	if err != nil {
		log.Fatalf("Error starting UDP server! %s\n", err)
	}
	s.conn = serverConn
	s.conn.SetReadBuffer(1024 * 1024 * 50)
	curTime := uint64(time.Now().Unix())
	nodeID := GetAddrHash(fmt.Sprintf("%s:%d", myAddr.IP, myAddr.Port))

	s.myself.id = nodeID
	s.myself.addr = myAddr
	s.myself.timestamp = curTime
	s.myself.status = mmsg.SrvStatus_ACTIVE

	// Only add myself to my membership list
	// Notice this is storing the pointer rather than a object copy
	s.others.members = tmap.NewWith(utils.UInt64Comparator)
	s.others.members.Put(nodeID, s.myself)

	// Initialize inter goroutine communication for sending signals
	s.igrc = &InterGoroutineComm{
		stopRecv: make(chan int),
		recvDone: make(chan int),
		pingDone: make(chan int),
		stopPing: make(chan int),
	}
	s.ackList = &map[uint64]bool{}
}

func (s *MembershipServer) SendMsg(addr *net.UDPAddr, msg []byte) bool {
	clientConn, err := net.DialUDP("udp", nil, addr)
	if err != nil {
		// TODO: maybe not using fatal?
		log.Fatalf("DialUDP fail! %s\n", err)
	}
	if err := clientConn.SetDeadline(time.Now().Add(time.Second)); err != nil {
		log.Fatalf("Set deadline fail! %s\n", err)
	}
	defer clientConn.Close()
	_, err = clientConn.Write(msg)
	if perr, ok := err.(net.Error); ok && perr.Timeout() {
		log.Printf("Write to %+v timeout!", clientConn)
		return false
	}
	return true
}

func (s *MembershipServer) SendJoinReq(hconf *hostConfig) bool {
	remoteAddr, ok := HostConfToUDP(hconf)
	if !ok {
		log.Fatal("Incorrect host config")
	}
	joinReq, success := PrepareMsg(s, mmsg.MsgType_JOIN)
	if !success {
		log.Fatal("Prepare send message fail!")
	}
	// If request timeout, join returns false, otherwise return true.
	return s.SendMsg(remoteAddr, joinReq)
}

func (s *MembershipServer) SendLeaveReq(wg *sync.WaitGroup, m *MemberInfo) {
	defer wg.Done()

	// Change my own status as 'LEAVE' in membership list
	// s.myself share the same pointer with self node in membership list
	info, found := s.others.Get(s.myself.id)
	if found {
		info.SetStatus(mmsg.SrvStatus_LEFT)
		info.SetTimestamp(uint64(time.Now().Unix()))
	} else {
		log.Panic("Can't find myself")
	}
	if leaveReq, success := PrepareMsg(s, mmsg.MsgType_LEAVE); !success {
		log.Fatal("Prepare leave message fail!")
	} else {
		s.SendMsg(m.GetAddr(), leaveReq)
	}
	return
}

func (s *MembershipServer) SendPingReq(wg *sync.WaitGroup, m *MemberInfo) {
	defer wg.Done()
	if pingReq, success := PrepareMsg(s, mmsg.MsgType_PING); !success {
		log.Fatal("Prepare ping message fail!")
	} else {
		_, found := s.others.Get(m.id)
		if found {
			(*s.ackList)[m.id] = false
		} else {
			log.Panic("Info not found!")
		}
		s.SendMsg(m.GetAddr(), pingReq)
		// log.Printf("Ping Sent to %s:%d!", info.addr.IP, info.addr.Port)

		time.Sleep(5 * time.Second)
		_, found = s.others.Get(m.id)
		if found {
			if !(*s.ackList)[m.id] {
				s.FailHandler(m.id)
			}
		} else {
			log.Panic("Info not found!")
		}
	}
}

func (s *MembershipServer) SendUpdateReq(wg *sync.WaitGroup, m *MemberInfo) {
	defer wg.Done()
	// Sleep random time from 0.1~0.5s to prevent message peak.
	time.Sleep(time.Duration(rand.Intn(5)*10) * time.Millisecond)
	if updateReq, success := PrepareMsg(s, mmsg.MsgType_UPDATE); !success {
		log.Fatal("Prepare update message fail!")
	} else {
		s.SendMsg(m.GetAddr(), updateReq)
	}
}

func (s *MembershipServer) Update() {
	// Get 4 successors in the membership list.
	successors := s.others.getSuccessorAsList(s.myself.GetId(), 4)

	// Send update to those successors.
	var wg sync.WaitGroup
	for _, succ := range successors {
		// Only send to those that are active
		if succ.GetStatus() == mmsg.SrvStatus_ACTIVE {
			wg.Add(1)
			go s.SendUpdateReq(&wg, succ)
		}
	}
	wg.Wait()
}

func (s *MembershipServer) Ping() {
	var wg sync.WaitGroup
	defer close(s.igrc.pingDone)
	for {
		select {
		default:
			// Get 3 successors in the membership list
			successors := s.others.getSuccessorAsList(
				s.myself.GetId(), 3)

			// Send ping to those successors
			for _, succ := range successors {
				if succ.GetStatus() == mmsg.SrvStatus_ACTIVE {
					wg.Add(1)
					go s.SendPingReq(&wg, succ)
				}
			}
			wg.Wait()
		case <-s.igrc.stopPing:
			return
		}
	}
}

func (s *MembershipServer) Ack(omsg mmsg.MembershipMessage) {
	ackRes, success := PrepareMsg(s, mmsg.MsgType_ACK)
	if !success {
		log.Fatal("Prepare ack message fail!")
	}
	remoteIp := omsg.GetSrcIp()
	remotePort := omsg.GetSrcPort()
	remoteAddr, err := net.ResolveUDPAddr(
		"udp",
		fmt.Sprintf("%s:%d", remoteIp, remotePort))
	if err != nil {
		log.Println(err)
	}
	s.SendMsg(remoteAddr, ackRes)
	// log.Printf("ack sent for %s:%d", remoteIp, remotePort)
}

func (s *MembershipServer) JoinGroup(port int) {
	fmt.Println("Join Group")

	var knownHostsConf = "/etc/cs425/known_hosts.json"
	var myInfoConf = "/etc/cs425/my_info.json"

	// Load static known_hosts and my_info file
	knownHosts := GetKnownHosts(knownHostsConf, port)
	myInfo := GetSelfInfo(myInfoConf, port)

	// Start the Membership server to receive message
	s.StartServer(myInfo)

	// Broadcast join
	for _, hConf := range knownHosts {
		if s.SendJoinReq(&hConf) {
			// fmt.Printf("Sent to %v\n", hConf)
		}
	}

	// Start the long running ping and recvmsg routine.
	// These two routine will terminate upon failure,
	// or leave the membership group.
	go s.RecvMsg()

	time.Sleep(time.Second)
	// Should check if it has successors now
	go s.Ping()

}

func (s *MembershipServer) LeaveGroup() {
	fmt.Println("Leave Group")
	// Broadcast the leave message to every node in membership list
	memberList := s.others.ToList()
	var wg sync.WaitGroup
	for _, m := range memberList {
		wg.Add(1)
		go s.SendLeaveReq(&wg, m)
	}
	wg.Wait()
	// close the connection
	s.conn.Close()
	s.conn = nil

	// stop the RecvMsg routine, and wait for it to stop
	close(s.igrc.stopRecv)
	close(s.igrc.stopPing)
	<-s.igrc.recvDone
	<-s.igrc.pingDone

}

func (s *MembershipServer) RecvMsg() {
	// Read from UDP socket, start different go routine to handle the msg

	defer close(s.igrc.recvDone)
	var newMsg mmsg.MembershipMessage
	var msgQueue [5]chan mmsg.MembershipMessage
	for i := range msgQueue {
		msgQueue[i] = make(chan mmsg.MembershipMessage, 20)
	}
	go s.JoinHandler(msgQueue[0])
	go s.LeaveHandler(msgQueue[1])
	go s.UpdateHandler(msgQueue[2])
	go s.PingHandler(msgQueue[3])
	go s.AckHandler(msgQueue[4])
	for {
		select {
		default:
			msg, success := ReadProtoMarshalFromUDP(s.conn)
			if !success {
				log.Panic("Server read from UDP fail!")
			}
			newMsg = mmsg.MembershipMessage{}
			if err := proto.Unmarshal(msg, &newMsg); err != nil {
				log.Fatalf("Unable to unmarshal incomming msg."+
					" %s\n", err)
			} else {
				switch newMsg.Type {
				case mmsg.MsgType_JOIN:
					msgQueue[0] <- newMsg
				case mmsg.MsgType_LEAVE:
					msgQueue[1] <- newMsg
				case mmsg.MsgType_UPDATE:
					msgQueue[2] <- newMsg
				case mmsg.MsgType_PING:
					msgQueue[3] <- newMsg
				case mmsg.MsgType_ACK:
					msgQueue[4] <- newMsg
				}
			}
		case <-s.igrc.stopRecv:
			return
		}
	}
}

func (s *MembershipServer) ListMember() {
	for _, member := range s.others.ToList() {
		log.Printf("[ListMember] Server: %s, "+
			"timestamp: %d, status: %s\n",
			member.addr.IP.String(), member.timestamp,
			member.status.String())
	}
}

func (s *MembershipServer) ListSelf() {
	if s.myself != nil {
		log.Printf("[ListSelf] Server host: %s:%d, id: %d\n",
			s.myself.addr.IP.String(), s.myself.addr.Port, s.myself.id)
	} else {
		log.Printf("[ListSelf] Server not join the system yet.")
	}
}
