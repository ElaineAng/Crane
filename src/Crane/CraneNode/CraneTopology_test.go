package CraneNode

import (
	cmsg "Crane/CraneMessage"
	"reflect"
	"testing"
)

func TestCraneTopology_GetTopoSort(t *testing.T) {
	type fields struct {
		Items map[string]*cmsg.TopoItem
		Graph map[string][]string
		src   []string
	}
	tests := []struct {
		name   string
		fields fields
		want   []string
	}{
		{"normal test", fields{map[string]*cmsg.TopoItem{}, map[string][]string{"a": {"b", "c"}, "b": {}}, []string{"a"}}, []string{"a", "c", "b"}},
		{"complex test", fields{map[string]*cmsg.TopoItem{}, map[string][]string{"a": {"b", "c"}, "b": {"f", "e", "d"}, "f": {"e"}, "d": {"e"}, "c": {"d"}, "e": {"g"}}, []string{"a"}}, []string{"a", "c", "b", "d", "f", "e", "g"}},
		{"empty test", fields{map[string]*cmsg.TopoItem{}, map[string][]string{}, []string{}}, []string{}},
		{"one test", fields{map[string]*cmsg.TopoItem{}, map[string][]string{"h": nil}, []string{"h"}}, []string{"h"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			topo := &CraneTopology{
				Items: tt.fields.Items,
				Graph: tt.fields.Graph,
				src:   tt.fields.src,
			}
			if got := topo.GetTopoSort(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("CraneTopology.GetTopoSort() = %v, want %v", got, tt.want)
			}
		})
	}
}
