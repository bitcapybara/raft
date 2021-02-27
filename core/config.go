package core

// 构造 Node 对象时的配置参数
type Config struct {
	Fsm                Fsm
	RaftStatePersister RaftStatePersister
	SnapshotPersister  SnapshotPersister
	Transport          Transport
	Peers              map[NodeId]NodeAddr
	Me                 NodeId
	ElectionMinTimeout int
	ElectionMaxTimeout int
	HeartbeatTimeout   int
	MaxLogLength       int
}
