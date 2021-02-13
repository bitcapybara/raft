package raftlog

type Entry struct {
	lastIndex int     // 前一个日志的索引
	term      int     // 日志项所在term
	command   Command // 状态机命令
}

type Slice []Entry
