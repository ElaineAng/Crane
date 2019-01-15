package MembershipServer

import (
	"sync"
	"testing"

	"github.com/emirpasic/gods/maps/treemap"
	"github.com/emirpasic/gods/utils"
)

func TestMembershipList_Replace(t *testing.T) {
	type fields struct {
		members *treemap.Map
		lock    sync.Mutex
	}
	type args struct {
		member *MemberInfo
	}
	tmap := treemap.NewWith(utils.UInt64Comparator)
	f := fields{tmap, sync.Mutex{}}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		{"Replace with non-exist item", f, args{&MemberInfo{id: 2, timestamp: 1, iteration: 0}}},
		{"Replace with smaller item", f, args{&MemberInfo{id: 1}}},
		{"Replace with larger timestamp but smaller iteration item", f, args{&MemberInfo{id: 2, timestamp: 2, iteration: 0}}},
		{"Replace with equal timestamp and larger iteration item", f, args{&MemberInfo{id: 2, timestamp: 2, iteration: 2}}},
		{"Replace with equal timestamp but smaller iteration item", f, args{&MemberInfo{id: 2, timestamp: 2, iteration: 1}}},
		{"Replace with smaller iteration and smaller timestamp item", f, args{&MemberInfo{id: 2, timestamp: 0, iteration: 0}}},
		{"Replace with larger iteration but smaller timestamp item", f, args{&MemberInfo{id: 2, timestamp: 0, iteration: 100}}},
		{"Replace with larger iteration and larger item", f, args{&MemberInfo{id: 2, timestamp: 3, iteration: 100}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &MembershipList{
				members: tt.fields.members,
				lock:    tt.fields.lock,
			}
			m.Replace(tt.args.member)
			t.Log("Current treemap:", m.members.Keys())
			t.Log("Inserting:", tt.args.member)
			id2, _ := m.members.Get(uint64(2))
			t.Log("id2 value:", id2)
		})
	}
}

func TestMembershipList_Merge(t *testing.T) {
	type fields struct {
		members *treemap.Map
		lock    sync.Mutex
	}
	type args struct {
		newMembers []*MemberInfo
	}
	tmap := treemap.NewWith(utils.UInt64Comparator)
	f := fields{tmap, sync.Mutex{}}
	tmap.Put(uint64(1), &MemberInfo{id: 1, timestamp: 1, iteration: 0})
	tmap.Put(uint64(2), &MemberInfo{id: 2, timestamp: 2, iteration: 0})
	tmap.Put(uint64(3), &MemberInfo{id: 3, timestamp: 3, iteration: 0})
	tmap.Put(uint64(4), &MemberInfo{id: 4, timestamp: 4, iteration: 0})
	tmap.Put(uint64(5), &MemberInfo{id: 5, timestamp: 5, iteration: 0})
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		{"Merge with list with same cardinality", f, args{
			[]*MemberInfo{
				{id: 1, timestamp: 1, iteration: 1},
				{id: 2, timestamp: 1, iteration: 1},
				{id: 3, timestamp: 2, iteration: 2},
				{id: 4, timestamp: 5, iteration: 1},
				{id: 5, timestamp: 5, iteration: 100},
			}}},
		{"Merge with list with larger cardinality", f, args{
			[]*MemberInfo{
				{id: 1, timestamp: 1, iteration: 1},
				{id: 2, timestamp: 1, iteration: 1},
				{id: 3, timestamp: 2, iteration: 2},
				{id: 4, timestamp: 5, iteration: 1},
				{id: 5, timestamp: 5, iteration: 100},
				{id: 6, timestamp: 0, iteration: 0},
			}}},
		{"Merge with list with smaller cardinality", f, args{
			[]*MemberInfo{
				{id: 1, timestamp: 4, iteration: 1},
				{id: 2, timestamp: 3, iteration: 1},
			}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &MembershipList{
				members: tt.fields.members,
				lock:    tt.fields.lock,
			}
			m.Merge(tt.args.newMembers)
			for _, i := range m.members.Values() {
				t.Log(*i.(*MemberInfo))
			}
		})
	}
}
