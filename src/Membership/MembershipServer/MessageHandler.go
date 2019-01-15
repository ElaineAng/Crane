package MembershipServer

import (
	mmsg "Membership/MembershipMessage"
	"fmt"
	"log"
	"runtime"
)

func (s *MembershipServer) JoinHandler(joinQueue chan mmsg.MembershipMessage) {

	// Read msg from join queue
	for {
		if join, ok := <-joinQueue; ok {
			// Merge with join message
			// Expected to see membership list of len 1
			// Update, send the new membership list to 4 successors
			list := join.GetList()
			change := s.others.Merge(GetMemberListFromProto(list))
			log.Printf("[JOIN] Server id: %d at %s:%d\n",
				join.List[0].Id, join.SrcIp, join.SrcPort)
			if change {
				s.Update()
			}
		} else {
			log.Panic("Join channel is closed!")
		}
	}
}

func (s *MembershipServer) LeaveHandler(leaveQueue chan mmsg.MembershipMessage) {
	// Read msg from leave queue
	// this handler does not call update because leave msg is broadcast
	for {
		if leave, ok := <-leaveQueue; ok {
			// Merge with leave message
			// Expected to be a full membership list
			change := s.others.Merge(GetMemberListFromProto(leave.GetList()))
			id := GetAddrHash(fmt.Sprintf("%s:%d", leave.SrcIp, leave.SrcPort))
			if change {
				log.Printf("[LEAVE] Server id: %d at %s:%d\n",
					id, leave.SrcIp, leave.SrcPort)
				s.Update()
			}
		} else {
			log.Panic("Leave channel is closed!")
		}
	}

}

func (s *MembershipServer) UpdateHandler(
	updateQueue chan mmsg.MembershipMessage) {
	// Read msg from update channel
	for {
		if update, ok := <-updateQueue; ok {
			newMemberListProto := update.GetList()
			newMembers := GetMemberListFromProto(newMemberListProto)
			if changed := s.others.Merge(newMembers); changed {
				s.Update()
			}
		} else {
			log.Panic("Update channel is closed!")
		}
		runtime.Gosched()
	}
}

func (s *MembershipServer) PingHandler(pingQueue chan mmsg.MembershipMessage) {
	// Sits in infinite loop, return ack for ping
	for {
		if ping, ok := <-pingQueue; ok {
			//log.Printf("ping received from %s:%d", ping.SrcIp, ping.SrcPort)
			s.Ack(ping)
		} else {
			log.Panic("Ping channel is closed!")
		}
		runtime.Gosched()
	}
}

func (s *MembershipServer) AckHandler(ackQueue chan mmsg.MembershipMessage) {
	for {
		if ack, ok := <-ackQueue; ok {
			// log.Print("Ack handler activated!")
			id := GetAddrHash(fmt.Sprintf(
				"%s:%d", ack.GetSrcIp(), ack.GetSrcPort()))

			_, found := s.others.Get(id)
			if found {
				(*s.ackList)[id] = true
				// log.Printf("ack received from %s:%d",
				// 	info.addr.IP, info.addr.Port)
			} else {
				log.Println("Get ack from unknown server!")
			}
		} else {
			log.Panic("Ack channel is closed!")
		}
		runtime.Gosched()
	}
}

func (s *MembershipServer) FailHandler(failId uint64) {
	selfList := s.others
	info, found := selfList.Get(failId)
	if found {
		if info.GetStatus() == mmsg.SrvStatus_ACTIVE {
			info.SetStatus(mmsg.SrvStatus_FAIL)
			// Need to propagate the fail message to successors
			s.Frc <- info.GetAddr().IP.String()
			s.Update()
			log.Printf("[FAILED] Server id %d at %v+\n", failId, info.addr)
		}
	} else {
		log.Panic("Fail at a not exist server!")
	}
}
