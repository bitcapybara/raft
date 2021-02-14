package core

type entry struct {
	index int    // 此条目的索引
	term  int    // 日志项所在term
	data  []byte // 状态机命令
}

type entries []entry
