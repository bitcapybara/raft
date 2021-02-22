package main

import "github.com/bitcapybara/go-raft/core"

func main() {
	node := core.NewNode(core.Config{})
	node.Run()
}
