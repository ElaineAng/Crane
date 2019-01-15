package CraneNode

import (
	cmsg "Crane/CraneMessage"
)

// the topology object store the Graph by a map
type CraneTopology struct {
	Items map[string]*cmsg.TopoItem
	Graph map[string][]string
	src   []string
}

// get a empty topology
func NewTopo() *CraneTopology {
	return &CraneTopology{Items: make(map[string]*cmsg.TopoItem), Graph: make(map[string][]string)}
}

func NewWithTopo(topology *cmsg.Topology) *CraneTopology {
	c := NewTopo()
	c.Update(topology)
	return c
}

func (t *CraneTopology) Update(topology *cmsg.Topology) {
	for _, item := range topology.Components {
		t.Items[item.Myself.ItemName] = item
		t.Graph[item.Myself.ItemName] = nil
		for _, dst := range item.Grouping {
			t.Graph[item.Myself.ItemName] = append(t.Graph[item.Myself.ItemName], dst.ItemName)
		}
		if item.ComponentType == cmsg.CompType_SPOUT {
			t.src = append(t.src, item.Myself.ItemName)
		}
	}
}

func (t *CraneTopology) dfs(curr string, result *[]string, visited *map[string]bool) {
	(*visited)[curr] = true
	if neighbors, ok := t.Graph[curr]; ok {
		for _, dst := range neighbors {
			if _, ok = (*visited)[dst]; !ok {
				t.dfs(dst, result, visited)
			}
		}
	}
	*result = append(*result, curr)
}

func (t *CraneTopology) GetTopoSort() []string {
	visited := make(map[string]bool)
	result := make([]string, 0)
	for _, src := range t.src {
		t.dfs(src, &result, &visited)
	}
	for left, right := 0, len(result)-1; left < right; left, right = left+1, right-1 {
		result[left], result[right] = result[right], result[left]
	}
	return result
}
