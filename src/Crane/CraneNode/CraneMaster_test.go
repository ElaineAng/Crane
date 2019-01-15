package CraneNode

import (
	cmsg "Crane/CraneMessage"
	"testing"
)

func TestCraneMaster_splitTasks(t *testing.T) {
	type fields struct {
		topology    *CraneTopology
		allTasks    []string
		workers     []cmsg.Node
		taskMapping map[string][]string
		clientConn  *cmsg.Crane_UploadTopoServer
		jobBinName  string
	}
	topo := &cmsg.Topology{
		Components: []*cmsg.TopoItem{
			//{ComponentType: cmsg.CompType_SPOUT, Myself: &cmsg.Component{ItemName: "a"}, Grouping: []*cmsg.Component{{ItemName: "b"}, {ItemName: "c"}}},
			//{ComponentType: cmsg.CompType_BOLT, Myself: &cmsg.Component{ItemName: "b"}, Grouping: []*cmsg.Component{{ItemName: "f"}, {ItemName: "e"}, {ItemName: "d"}}},
			//{ComponentType: cmsg.CompType_BOLT, Myself: &cmsg.Component{ItemName: "c"}, Grouping: []*cmsg.Component{{ItemName: "d"}}},
			//{ComponentType: cmsg.CompType_BOLT, Myself: &cmsg.Component{ItemName: "d"}, Grouping: []*cmsg.Component{{ItemName: "e"}}},
			//{ComponentType: cmsg.CompType_BOLT, Myself: &cmsg.Component{ItemName: "e"}, Grouping: []*cmsg.Component{{ItemName: "g"}, {ItemName: "j"}}},
			//{ComponentType: cmsg.CompType_BOLT, Myself: &cmsg.Component{ItemName: "f"}, Grouping: []*cmsg.Component{{ItemName: "e"}}},
			//{ComponentType: cmsg.CompType_BOLT, Myself: &cmsg.Component{ItemName: "g"}, Grouping: []*cmsg.Component{{ItemName: "h"}}},
			//{ComponentType: cmsg.CompType_BOLT, Myself: &cmsg.Component{ItemName: "h"}, Grouping: []*cmsg.Component{{ItemName: "i"}}},
			//{ComponentType: cmsg.CompType_BOLT, Myself: &cmsg.Component{ItemName: "i"}, Grouping: nil},
			//{ComponentType: cmsg.CompType_BOLT, Myself: &cmsg.Component{ItemName: "j"}, Grouping: nil},
			{ComponentType: cmsg.CompType_SPOUT, Myself: &cmsg.Component{ItemName: "FileReader"}, Grouping: []*cmsg.Component{{ItemName: "SplitLine"}}},
			{ComponentType: cmsg.CompType_BOLT, Myself: &cmsg.Component{ItemName: "SplitLine"}, Grouping: []*cmsg.Component{{ItemName: "MaskBadWords"}, {ItemName: "FilterBadWords"}}},
			{ComponentType: cmsg.CompType_BOLT, Myself: &cmsg.Component{ItemName: "MaskBadWords"}, Grouping: []*cmsg.Component{{ItemName: "AppendHello"}}},
			{ComponentType: cmsg.CompType_BOLT, Myself: &cmsg.Component{ItemName: "FilterBadWords"}, Grouping: []*cmsg.Component{{ItemName: "AppendHello"}}},
			{ComponentType: cmsg.CompType_BOLT, Myself: &cmsg.Component{ItemName: "AppendHello"}, Grouping: nil},
		},
	}
	tests := []struct {
		name   string
		fields fields
	}{
		{"simeple test",
			fields{
				NewTopo(),
				nil,
				[]cmsg.Node{
					{HostName: "host1", HostIp: "192.168.1.1", Port: 8888},
					{HostName: "host2", HostIp: "192.168.1.2", Port: 8888},
					{HostName: "host3", HostIp: "192.168.1.3", Port: 8888},
					{HostName: "host4", HostIp: "192.168.1.4", Port: 8888},
					{HostName: "host5", HostIp: "192.168.1.5", Port: 8888},
				},
				make(map[string][]string),
				nil,
				"gg",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &CraneMaster{
				topology:    tt.fields.topology,
				allTasks:    tt.fields.allTasks,
				workers:     tt.fields.workers,
				taskMapping: tt.fields.taskMapping,
				clientConn:  tt.fields.clientConn,
				jobBinName:  tt.fields.jobBinName,
			}
			m.updateTopology(topo)
			m.splitTasks()
			t.Log(m.allTasks)
			t.Log(m.taskMapping)
			t.Log(m.topology.Items)
		})
	}
}
