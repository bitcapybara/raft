package core

// 客户端实现此状态机来应用接收到的日志命令
type Fsm interface {
	Apply(data []byte) ([]byte, error)
}
