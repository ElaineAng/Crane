package MembershipServer

import (
	mmsg "Membership/MembershipMessage"
	"log"
	"net"
)

func (m *MemberInfo) GetAddr() *net.UDPAddr {
	return m.addr
}

func (m *MemberInfo) GetId() uint64 {
	return m.id
}

func (m *MemberInfo) GetStatus() mmsg.SrvStatus {
	return m.status
}

func (m *MemberInfo) SetStatus(newStatus mmsg.SrvStatus) {
	m.status = newStatus
}

func (m *MemberInfo) SetTimestamp(newTimestamp uint64) {
	m.timestamp = newTimestamp
}

// Get a memberinfo value from treemap, if not found, bool is false.
func (m *MembershipList) Get(id uint64) (*MemberInfo, bool) {
	m.lock.Lock()
	defer m.lock.Unlock()
	if v, found := m.members.Get(id); found {
		return v.(*MemberInfo), true
	}
	return &MemberInfo{}, false
}

// Replace an item in the map.
// If the item doesn't exist, put
// If it exists:
// - If new_ts >= old_ts, replace the item.
// - If new_ts < old_ts, ignore the new info.
func (m *MembershipList) Replace(member *MemberInfo) bool {
	m.lock.Lock()
	defer m.lock.Unlock()
	if v, found := m.members.Get(member.id); found {
		value := v.(*MemberInfo)
		if (member.timestamp >= value.timestamp) &&
			(member.status != value.status) {
			m.members.Put(member.id, member)
		} else {
			return false
		}
	} else {
		// If not found in current membership list,
		// put new member in current list.
		m.members.Put(member.id, member)
	}
	return true
}

// Replace line by line from the new membership list to current list.
func (m *MembershipList) Merge(newMembers []*MemberInfo) bool {
	changed := false
	for _, member := range newMembers {
		if m.Replace(member) {
			changed = true
		}
	}
	return changed
}

func (m *MembershipList) MergeExcept(newMembers []*MemberInfo, exceptID uint64) bool {
	changed := false
	for _, member := range newMembers {
		if member.id != exceptID {
			if m.Replace(member) {
				changed = true
			}
		}
	}
	return changed
}

func (m *MembershipList) ToList() []*MemberInfo {
	m.lock.Lock()
	defer m.lock.Unlock()
	result := make([]*MemberInfo, 0)
	if m.members == nil || m.members.Empty() {
		return result
	}
	itr := m.members.Iterator()
	itr.Begin()
	for {
		if !itr.Next() {
			break
		}
		result = append(result, itr.Value().(*MemberInfo))
	}
	return result
}

// Get n successor of node myId as list.
// The returned list doesn't contain self node
// If current MembershipList is shorter than n, cut the lenghth
func (m *MembershipList) getSuccessorAsList(myId uint64, n int) []*MemberInfo {

	m.lock.Lock()
	defer m.lock.Unlock()
	successors := make([]*MemberInfo, 0)

	counter := 0
	if _, found := m.members.Get(myId); found {
		itr := m.members.Iterator()
		itr.Begin()
		itr.Next()
		for itr.Key() != myId {
			itr.Next()
		}

		if memberLen := m.members.Size(); memberLen-1 < n {
			n = memberLen - 1
		}

		for counter < n {
			curMember := itr.Value().(*MemberInfo)
			if curMember.id != myId {
				successors = append(successors, curMember)
				counter += 1
			}
			if !itr.Next() {
				itr.Begin()
				itr.Next()
			}
		}
	} else {
		log.Panicln("Myself doesn't exist, should never happen!")
	}

	return successors
}
