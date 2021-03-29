package raft

// 网络通信接口，由客户端实现
type Transport interface {
	AppendEntries(addr NodeAddr, args AppendEntry, res *AppendEntryReply) error

	RequestVote(addr NodeAddr, args RequestVote, res *RequestVoteReply) error

	InstallSnapshot(addr NodeAddr, args InstallSnapshot, res *InstallSnapshotReply) error
}

// Transport 接口实现，开发测试用
type inMemTransport struct {
	aeRes map[NodeAddr]AppendEntryReply
	rvRes map[NodeAddr]RequestVoteReply
	isRes map[NodeAddr]InstallSnapshotReply
	err   error
}

func newInMemTransport() *inMemTransport {
	return &inMemTransport{}
}

func (tp *inMemTransport) AppendEntries(addr NodeAddr, args AppendEntry, res *AppendEntryReply) error {
	*res = tp.aeRes[addr]
	return tp.err
}

func (tp *inMemTransport) RequestVote(addr NodeAddr, args RequestVote, res *RequestVoteReply) error {
	*res = tp.rvRes[addr]
	return tp.err
}

func (tp *inMemTransport) InstallSnapshot(addr NodeAddr, args InstallSnapshot, res *InstallSnapshotReply) error {
	*res = tp.isRes[addr]
	return tp.err
}
