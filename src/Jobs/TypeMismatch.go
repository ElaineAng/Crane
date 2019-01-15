package main

type TypeMismatch struct {
	errMessage string
}

func (t TypeMismatch) Error() string {
	return t.errMessage
}

func main(){}
