package simplewal

// storage.go
// 存储层的接口，供 wal 层使用

type storage interface {
	// 写入单条日志，不负责 sync
	WriteRecord(lsn LSN, payload []byte) error

	// WriteBatch 负责写入一批日志，不负责 Sync。
	// 当前批次日志中，LSN 从第一条日志的 LSN 开始递增
	WriteBatch(lsn LSN, payloads [][]byte) error

	Sync() error

	Replay(start LSN, action func(LSN, []byte) error) error

	Close() error

	Trim(before LSN) error

	// 当前仅支持修复文件尾部的数据不完整，不支持文件头错误，不支持 crc 错误
	Fix() error
}
