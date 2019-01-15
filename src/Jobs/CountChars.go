package main

import "Crane/CraneTuple"

var charSum = 0
// calculate current sum of length of all the chars
func CountCharsExecute(tuple CraneTuple.CraneTuple) ([]CraneTuple.CraneTuple, error) {
	if word, ok := tuple.TupleData[0].(string); ok {
		charSum += len(word)
		return []CraneTuple.CraneTuple{{[]interface{}{charSum}}}, nil
	} else {
		return nil, TypeMismatch{"Sum mismatch"}
	}
}
