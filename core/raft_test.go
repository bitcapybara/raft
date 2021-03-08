package core

import (
	"testing"
	"time"
)

func TestStop(t *testing.T) {

	stopCh := make(chan struct{})
	close(stopCh)

	go func() {
		if _, ok := <- stopCh; ok {
			println("接收到信号")
		} else {
			println("未接收到信号")
		}
	}()

	time.Sleep(time.Second * 2)
}