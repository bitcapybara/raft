# go-raft
分布式 raft 共识算法 go 实现

### 一、需要实现的接口

#### Fsm

客户端状态机接口，在 raft 内部调用此接口来实现状态机的相关操作，比如应用日志，生成快照，安装快照等。

#### Transport

在 raft 内部调用此接口的各个方法用于网络通信，比如发送心跳，日志复制，领导者选举，发送快照等。

#### RaftStatePersister

在 raft 内部调用此接口来持久化和加载内部状态数据，包括 term，votedFor及日志条目。

#### SnapshotPersister

在 raft 内部调用此接口来持久化和加载快照数据。

#### Logger

在 raft 内部调用此接口来打印日志。

### 二、使用

1. 新建一个 `raft.Node` 对象，代表当前节点
2. 使用 `raft.Node.Run()` 方法开启 raft 循环
3. 开放 HTTP/RPC 接口，调用 `raft.Node` 的相应方法来接收来自其它节点的 raft 网络请求

### 三、示例

[simplefsm](https://github.com/bitcapybara/simplefsm) 项目是此 raft 库的一个示例，实现了一个极简的状态机，但已经包含了 raft 的所有功能。

