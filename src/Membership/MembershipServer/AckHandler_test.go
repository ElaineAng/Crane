package MembershipServer

import (
	"Membership/MembershipMessage"
	"github.com/emirpasic/gods/maps/treemap"
	"github.com/emirpasic/gods/utils"
	"sync"
	"testing"
	"time"
)

func TestMembershipServer_AckHandler(t *testing.T) {
	t.Run("Fail one active server", func(t *testing.T) {
		list := MembershipList{treemap.NewWith(utils.UInt64Comparator), sync.Mutex{}}
		server := MembershipServer{myself: MemberInfo{id: 2, timestamp: 0, iteration: 0, status: MembershipMessage.SrvStatus_ACTIVE}, others: list,}
		server.others.Replace(&MemberInfo{id: 1, timestamp: 0, iteration: 1})
		ch := make(chan MembershipMessage.MembershipMessage)
		go server.AckHandler(ch)
		ackMsg := MembershipMessage.MembershipMessage{
			Type: MembershipMessage.MsgType_ACK,
			List: []*MembershipMessage.MembershipListItem{
				{Id: 1},
			},
		}
		ch <- ackMsg
		ch <- ackMsg
		ch <- ackMsg
		ch <- ackMsg
		time.Sleep(1000000)
		if v, found := server.others.Get(uint64(1)); found {
			if v.GetIteration() != 5 {
				t.Fail()
			} else {
				ch <- ackMsg
				time.Sleep(1000000)
				if v, found := server.others.Get(uint64(1)); found {
					if v.GetIteration() != 6 {
						t.Fail()
					}
				} else {
					t.Fail()
				}
			}
		} else {
			t.Fail()
		}
	})
}
