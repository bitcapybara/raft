package core

import (
	"sync"
	"testing"
	"time"
)

func TestStop(t *testing.T) {

	stopCh := make(chan struct{})


	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stopCh:
				println("接收到信号")
				return
			default:
			}
		}
	}()

	time.Sleep(time.Second * 2)
	close(stopCh)
	wg.Wait()


}