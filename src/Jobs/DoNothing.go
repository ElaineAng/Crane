package main

import (
	ctup "Crane/CraneTuple"
)

func DoNothingExecute(ctuple ctup.CraneTuple) (ctup.CraneTuple, error) {
	return ctuple, nil
}
