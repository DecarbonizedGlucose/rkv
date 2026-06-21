package errors

type rkvError string

func (e rkvError) Error() string { return string(e) }

var (
	// ErrNotLeader 表示当前节点不是 Raft Leader。
	// 客户端应通过 leader-id trailer 重定向到 Leader 节点后重试。
	ErrNotLeader error = rkvError("not leader")

	// ErrLeaseNotFound 表示操作的租约不存在，已过期或者 ID 错误。
	ErrLeaseNotFound error = rkvError("lease not found")

	// ErrUnavailable 表示 gRPC 连接层面的服务不可达（网络故障、节点宕机等）。
	// 与 ErrNotLeader 的区别：此错误不携带 leader-id，无法自动重定向。
	ErrUnavailable error = rkvError("server unavailable")

	// ErrInternal 表示服务端内部错误。细节不对外暴露，仅在服务端日志中记录。
	ErrInternal error = rkvError("internal server error")
)
