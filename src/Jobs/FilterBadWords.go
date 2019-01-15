package main

import (
	ctup "Crane/CraneTuple"
	"strings"
)

func FilterBadWordsExecute(ctuple ctup.CraneTuple) (
	ctup.CraneTuple, error) {

	var newTuple []interface{}
	

	for _, t := range ctuple.TupleData {
		if strings.ToLower(t.(string)) != "shit" &&
			strings.ToLower(t.(string)) != "fuck" {
			newTuple = append(newTuple, t.(string))
		}
	}
	ret := ctup.CraneTuple{TupleData: newTuple}
	return ret, nil
}
