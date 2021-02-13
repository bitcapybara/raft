package core

type Entry struct {
	Index int    // 前一个日志的索引
	Term      int    // 日志项所在term
	Data      []byte // 状态机命令
}

type Slice []Entry
