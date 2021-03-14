package core

// 客户端状态机接口
type Fsm interface {
	// 参数实际上是 Entry 的 Data 字段
	// 返回值是应用状态机后的结果
	Apply([]byte) error

	// 生成快照二进制数据
	Serialize() ([]byte, error)
}
