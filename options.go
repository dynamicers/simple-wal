package simplewal

// options.go
// wal 配置项及默认配置

const (
	defaultQueueCapacity          = 40960
	defaultMaxPayloadBytes        = 4 << 20   // 4 MiB
	defaultMaxBatchBytes          = 4 << 20   // 4MiB
	defaultFileSizeThresholdBytes = 512 << 20 // 512MiB
)

type Options struct {
	CreateIfMissing        bool  // 打开 WAL 时，如果当前目录没有 wal 文件，新建文件
	UsedForAppend          bool  // true: 用于 Append; false: 用于离线 Replay 和 Trim
	QueueCapacity          int   // WAL 支持的待处理队列长度，必须是正数
	MaxPayloadBytes        int   // 允许的单条日志记录最大字节
	MaxBatchBytes          int   // 批提交时，最大的字节；如果单条日志超过该值，单独作为一批提交
	FileSizeThresholdBytes int64 // 底层日志文件进行分段的阈值大小，0 表示无限大，负数非法
}

func DefaultOptions() Options {
	return Options{
		CreateIfMissing:        false,
		UsedForAppend:          false,
		QueueCapacity:          defaultQueueCapacity,
		MaxPayloadBytes:        defaultMaxPayloadBytes,
		MaxBatchBytes:          defaultMaxBatchBytes,
		FileSizeThresholdBytes: defaultFileSizeThresholdBytes,
	}
}

func validOptions(opt Options) error {
	if opt.QueueCapacity <= 0 {
		return ErrQueueNonPositive
	}
	if opt.MaxPayloadBytes <= 0 {
		return ErrMaxPayloadBytesNonPositive
	}
	if opt.MaxBatchBytes <= 0 {
		return ErrMaxBatchBytesNonPositive
	}
	if opt.FileSizeThresholdBytes < 0 || (opt.FileSizeThresholdBytes > 0 && opt.FileSizeThresholdBytes < fileHeaderSize) {
		return ErrInvalidFileSizeThreshold
	}

	return nil
}
