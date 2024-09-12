package mongo

import "fmt"

func recoverFromPanic(err *error) {
	if r := recover(); r != nil {
		if e, ok := r.(error); ok {
			*err = e
		} else {
			*err = fmt.Errorf("panic: %v", r)
		}
	}
}
