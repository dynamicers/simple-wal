package simplewal

// error.go
// 自定义的错误

import "errors"

var (
	/* 配置项错误 */
	ErrQueueNonPositive = errors.New("simplewal: QueueCapacity must be positive")

	ErrMaxPayloadBytesNonPositive = errors.New("simplewal: MaxPayloadBytes must be positive")

	ErrMaxBatchBytesNonPositive = errors.New("simplewal: MaxBatchBytes must be positive")

	ErrInvalidFileSizeThreshold = errors.New("simplewal: FileSizeThresholdBytes must be 0 or positive, and should be bigger than file header size")

	/* WAL 接口错误 */
	ErrAppendDisabled = errors.New("simplewal: current WAL is Append Disabled")

	ErrOfflineRequired = errors.New("simplewal: operation requires an offline WAL")

	ErrNilReplayAction = errors.New("simplewal: replay action must not be nil")

	ErrTrimUnsupported = errors.New("simplewal: trim unsupported")

	ErrClosed = errors.New("simplewal: WAL is already closed")

	/* 底层文件错误 */
	ErrLSNNotFound = errors.New("simplewal: replay start LSN not found")

	ErrLSNExhausted = errors.New("simplewal: LSN space exhausted")

	ErrCorruput = errors.New("simplewal: invalid or corrupt WAL")

	ErrPayloadTooLarge = errors.New("simplewal: payload exceeds size limit")
)
