package MembershipServer

import (
	"Membership/MembershipMessage"
	"sync"
	"testing"

	"github.com/emirpasic/gods/maps/treemap"
	"github.com/emirpasic/gods/utils"
)

func TestMembershipServer_FailHandler(t *testing.T) {
	t.Run("Fail one active server", func(t *testing.T) {
		list := MembershipList{treemap.NewWith(utils.UInt64Comparator), sync.Mutex{}}
		server := MembershipServer{myself: MemberInfo{id: 2, timestamp: 0, iteration: 0, status: MembershipMessage.SrvStatus_ACTIVE}, others: list,}
		server.others.Replace(&MemberInfo{id: 1, timestamp: 0, iteration: 0})
		server.FailHandler(uint64(1))
		if v, found := server.others.Get(uint64(1)); found {
			if v.GetStatus() != MembershipMessage.SrvStatus_FAIL {
				t.Fail()
			}
		} else {
			t.Fail()
		}
	})
	t.Run("Fail self", func(t *testing.T) {
		list := MembershipList{treemap.NewWith(utils.UInt64Comparator), sync.Mutex{}}
		server := MembershipServer{myself: MemberInfo{id: 2, timestamp: 0, iteration: 0, status: MembershipMessage.SrvStatus_ACTIVE}, others: list,}
		server.others.Replace(&MemberInfo{id: 1, timestamp: 0, iteration: 0})
		server.FailHandler(uint64(2))
		if v, found := server.others.Get(uint64(2)); found {
			if v.GetStatus() != MembershipMessage.SrvStatus_FAIL {
				t.Fail()
			}
		} else {
			t.Fail()
		}
	})
}
