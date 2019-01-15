package main

import "Crane/CraneTuple"

var words = make(map[string]int64)
// count occurrence of every words
func CollectWordsExecute(tuple CraneTuple.CraneTuple) ([]CraneTuple.CraneTuple, error) {
	if word, ok := tuple.TupleData[0].(string); ok {
		words[word]++
		wordSum++
		var result []CraneTuple.CraneTuple
		if wordSum%1000 == 0 {
			for word, count := range words {
				result = append(result, CraneTuple.CraneTuple{[]interface{}{word, count}})
			}
		}
		return result, nil
	} else {
		return nil, TypeMismatch{"CollectWords mismatch"}
	}
}
