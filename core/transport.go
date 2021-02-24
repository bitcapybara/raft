package core

type Transport interface {

	AppendEntries(addr NodeAddr, args AppendEntry, res *AppendEntryReply) error

	RequestVote(addr NodeAddr, args RequestVote, res *RequestVoteReply) error

	InstallSnapshot(addr NodeAddr, args InstallSnapshot, res *InstallSnapshotReply) error
}
