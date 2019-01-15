package CraneNode

import (
	cmsg "Crane/CraneMessage"
	ctup "Crane/CraneTuple"
	"fmt"
	"reflect"
	"strconv"
)

func CraneToProtoTuple(ctuple ctup.CraneTuple) *cmsg.Tuple {
	var ty cmsg.TupleType
	var typeList []cmsg.TupleType
	var intList []int64
	var floatList []float64
	var stringList []string

	for _, data := range ctuple.TupleData {
		curType := reflect.TypeOf(data).Kind()

		if curType == reflect.Int || curType == reflect.Int64 {
			intList = append(intList, data.(int64))
			ty = cmsg.TupleType_INT
		} else if curType == reflect.Float64 {
			floatList = append(floatList, data.(float64))
			ty = cmsg.TupleType_FLOAT
		} else {
			stringList = append(stringList, data.(string))
			ty = cmsg.TupleType_STRING
		}
		typeList = append(typeList, ty)

	}
	ret := &cmsg.Tuple{
		TupleT:      typeList,
		IntField:    intList,
		DoubleField: floatList,
		StringField: stringList,
	}
	return ret
}

func ProtoToCraneTuple(ptuple cmsg.Tuple) ctup.CraneTuple {
	var data []interface{}
	i, f, s := 0, 0, 0

	for _, ty := range ptuple.TupleT {

		if ty == cmsg.TupleType_INT {
			data = append(data, ptuple.IntField[i])
			i += 1
		} else if ty == cmsg.TupleType_FLOAT {
			data = append(data, ptuple.DoubleField[f])
			f += 1
		} else {
			data = append(data, ptuple.StringField[s])
			s += 1
		}
	}
	ret := ctup.CraneTuple{
		TupleData: data,
	}
	return ret

}

func CraneToByteArray(ctuple ctup.CraneTuple) []byte {
	ret := []byte{}
	for _, t := range ctuple.TupleData {
		var newd []byte
		curType := reflect.TypeOf(t).Kind()
		if curType == reflect.Int || curType == reflect.Int64 {
			newd = []byte(strconv.Itoa(t.(int)))
		} else if curType == reflect.Float64 {
			newd = []byte(strconv.FormatFloat(
				t.(float64), 'E', -1, 64))
		} else {
			newd = []byte(fmt.Sprintf("%s ", t.(string)))
		}
		ret = append(ret, newd...)
	}

	return ret

}
