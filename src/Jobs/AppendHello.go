package main
import (
	ctup "Crane/CraneTuple"
)

func AppendHelloExecute(ctuple ctup.CraneTuple) (ctup.CraneTuple, error) {
	ctuple.TupleData = append(ctuple.TupleData, "Hello!!\n")
	return ctuple, nil
}
