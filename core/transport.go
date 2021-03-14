package core

// 网络通信接口，由客户端实现
type Transport interface {

	AppendEntries(addr NodeAddr, args AppendEntry, res *AppendEntryReply) error

	RequestVote(addr NodeAddr, args RequestVote, res *RequestVoteReply) error

	InstallSnapshot(addr NodeAddr, args InstallSnapshot, res *InstallSnapshotReply) error
}
