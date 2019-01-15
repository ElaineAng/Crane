package main

import (
	ctup "Crane/CraneTuple"
	"bufio"
	"os"
)


var f, _ = os.Open("/tmp/testfiles.txt")
var scanner = bufio.NewScanner(f)
// get lines from a file
func FileReaderNextTuple() (*ctup.CraneTuple, error) {

	if ok := scanner.Scan(); ok {
		line := scanner.Text()
		var tupleLine []interface{}
		tupleLine = append(tupleLine, line)
		retTuple := &ctup.CraneTuple{
			TupleData: tupleLine,
		}
		return retTuple, nil
	} else {
		f.Close()
		return nil, scanner.Err()
	}
}
