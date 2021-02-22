package core

import "testing"

func initRaft() *raft {
	var config = Config{
		Peers: map[NodeId]NodeAddr{"node1": "127.0.0.1:3380", "node2":"127.0.0.1:3381"},
		Me: "node1",
		ElectionMinTimeout: 300,
		ElectionMaxTimeout: 500,
		HeartbeatTimeout: 40,
		MaxLogLength: 100,
	}

	return newRaft(config)
}

func TestIsLeader(t *testing.T) {

	var rf = initRaft()
	rf.setLeader(rf.me())

	rf.setRoleStage(Leader)
	if !rf.isLeader() {
		t.Errorf("isLeader() wrong...")
	}

	rf.setRoleStage(Follower)
	if rf.isLeader() {
		t.Errorf("isLeader() wrong...")
	}
}

func BenchmarkIsLeader(b *testing.B) {
	var rf = initRaft()
	rf.setLeader(rf.me())

	for i:=0; i< b.N; i++ {
		rf.setRoleStage(Leader)
		if !rf.isLeader() {
			b.Errorf("isLeader() wrong...")
		}
	}
}
