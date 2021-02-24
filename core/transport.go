package core

type Transport interface {

	SendAppendEntries(addr NodeAddr, args AppendEntry, res *AppendEntryReply) error

	SendRequestVote(addr NodeAddr, args RequestVote, res *RequestVoteReply) error

	SendInstallSnapshot(addr NodeAddr, args InstallSnapshot, res *InstallSnapshotReply) error
}
