# go-raft
分布式 raft 共识算法 go 实现

### 使用指南
* 实现 Transport 接口用于发送网络请求
* 接收到网络请求后，需要调用Node的相应方法执行逻辑
* RaftPersister 持久化 raft 的状态信息
* SnapshotPersister 持久化快照数据
