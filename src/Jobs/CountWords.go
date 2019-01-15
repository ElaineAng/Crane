package main

import "Crane/CraneTuple"

var wordSum = 0
// calculate current sum of length of all the words
func CountWordsExecute(tuple CraneTuple.CraneTuple) ([]CraneTuple.CraneTuple, error) {
	if _, ok := tuple.TupleData[0].(string); ok {
		return []CraneTuple.CraneTuple{{[]interface{}{wordSum}}}, nil
	} else {
		return nil, TypeMismatch{"CountWords mismatch"}
	}
}
