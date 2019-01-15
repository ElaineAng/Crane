package main

import (
	ctup "Crane/CraneTuple"
	"strings"
)

// split the line into multiple words
func SplitLineExecute(tuple ctup.CraneTuple) (
	ctup.CraneTuple, error) {
	
	var resData []interface{}
	for _, t := range tuple.TupleData {
		if line, ok := t.(string); ok {
			words := strings.Fields(line)
			for _, w := range words {
				resData = append(resData, w)
			}
		}
	}
	ret := ctup.CraneTuple{
		TupleData: resData,
	}
	return ret, nil
}
