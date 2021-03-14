package core

import (
	"testing"
)

func TestGob(t *testing.T) {

	stopCh := make(chan struct{})


	go func() {
		defer close(stopCh)
		stopCh <- struct{}{}
		stopCh <- struct{}{}
	}()

	for {
		select {
		case _, ok :=<-stopCh:
			if ok {
				println("has value")
			} else {
				println("stopped")
				return
			}
		}
	}
}