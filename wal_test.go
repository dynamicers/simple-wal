package simplewal_test

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	wal "github.com/dynamicers/simple-wal"
)

func openWAL(t *testing.T, dir string, opt wal.Options) *wal.WAL {
	t.Helper()

	w, err := wal.Open(dir, opt)
	if err != nil {
		t.Fatalf("Open (%q) failed: %v", dir, err)
	}

	t.Cleanup(func() {
		if err := w.Close(); err != nil {
			t.Errorf("Cleanup Close falied: %v", err)
		}
	})

	return w
}

func writerOptions() wal.Options {
	opt := wal.DefaultOptions()
	opt.CreateIfMissing = true
	opt.UsedForAppend = true
	return opt
}

type replayRecord struct {
	lsn     wal.LSN
	payload string
}

func readRecords(t *testing.T, w *wal.WAL, start wal.LSN) []replayRecord {
	t.Helper()

	var records []replayRecord

	callback := func(lsn wal.LSN, payload []byte) error {
		records = append(records, replayRecord{
			lsn:     lsn,
			payload: string(payload),
		})
		return nil
	}
	err := w.Replay(start, callback)
	if err != nil {
		t.Fatalf("Replay start lsn %d failed: %v", start, err)
	}

	return records
}

// 测试基础的 Open, Append, Close, Replay 接口
// Append 日志后再 Replay
func TestAppendAndReplay(t *testing.T) {
	dir := t.TempDir()
	payload := bytes.Repeat([]byte("a"), 100)

	// 首次创建 WAL，同时 Append 日志
	opt := wal.DefaultOptions()
	opt.CreateIfMissing = true
	opt.UsedForAppend = true
	w := openWAL(t, dir, writerOptions())

	lsn, err := w.Append(payload)
	if err != nil {
		t.Fatalf("Append failed: %v", err)
	}
	if lsn != 0 {
		t.Fatalf("First LSN = %d, want 0", lsn)
	}

	if err := w.Close(); err != nil {
		t.Fatalf("Close wal failed: %v", err)
	}

	// 重新读取已有的日志，从头执行 Replay
	opt.CreateIfMissing = false
	opt.UsedForAppend = false
	reader := openWAL(t, dir, opt)

	var replayedLSNs []wal.LSN
	var replayedPayloads [][]byte

	callback := func(lsn wal.LSN, data []byte) error {
		replayedLSNs = append(replayedLSNs, lsn)
		replayedPayloads = append(replayedPayloads, bytes.Clone(data))
		return nil
	}
	err = reader.Replay(0, callback)
	if err != nil {
		t.Fatalf("Replay failed: %v", err)
	}

	if len(replayedLSNs) != 1 {
		t.Fatalf("Replayed %d records, want 1", len(replayedLSNs))
	}
	if replayedLSNs[0] != lsn {
		t.Errorf("Replayed LSN = %d, want %d", replayedLSNs[0], lsn)
	}
	if !bytes.Equal(replayedPayloads[0], payload) {
		t.Errorf("Replayed payload = %q, want %q", replayedPayloads[0], payload)
	}
}

// 多次串行 Append 日志后再 Replay
func TestAppendOneByOne(t *testing.T) {
	dir := t.TempDir()

	// 创建日志，并连续 Append
	opt := wal.DefaultOptions()
	opt.CreateIfMissing = true
	opt.UsedForAppend = true
	w := openWAL(t, dir, opt)

	const count = 8

	var wantLSNs []wal.LSN
	var wantPayloads [][]byte

	for i := 0; i < count; i++ {
		payload := bytes.Repeat([]byte{byte('a' + i)}, 100)

		lsn, err := w.Append(payload)
		if err != nil {
			t.Fatalf("The %dth Append failed: %v", i, err)
		}
		if lsn != wal.LSN(i) {
			t.Fatalf("The %dth record's LSN = %d, want %d", i, lsn, i)
		}

		wantLSNs = append(wantLSNs, lsn)
		wantPayloads = append(wantPayloads, payload)
	}

	if err := w.Close(); err != nil {
		t.Fatalf("Close wal failed: %v", err)
	}

	// 重新打开日志，从头开始执行 Replay
	opt.CreateIfMissing = false
	opt.UsedForAppend = false
	reader := openWAL(t, dir, opt)

	index := 0
	callback := func(lsn wal.LSN, data []byte) error {
		if index >= len(wantLSNs) {
			t.Errorf("Found %d th record's LSN = %d, want max record is %d", index, lsn, len(wantLSNs))
			return nil
		}

		if lsn != wantLSNs[index] {
			t.Errorf("The %d th record's LSN = %d, want %d", index, lsn, wantLSNs[index])
		}
		if !bytes.Equal(data, wantPayloads[index]) {
			t.Errorf("The %d th record's payload = %q, want %q", lsn, data, wantPayloads[index])
		}
		index++
		return nil
	}
	err := reader.Replay(0, callback)
	if err != nil {
		t.Fatalf("Replay failed: %v", err)
	}
	if index != count {
		t.Errorf("Replayed %d records, want %d", index, count)
	}
}

// 测试重复打开 WAL 追加日志时，LSN 会从上次最大 LSN 递增
func TestReopenForAppend(t *testing.T) {
	// 新建 WAL 用于 Append
	dir := t.TempDir()
	w := openWAL(t, dir, writerOptions())

	// 首次 Append
	if lsn, err := w.Append([]byte("first")); err != nil || lsn != 0 {
		t.Fatalf("first Append result is (%d, %v), wnat (0, nil)", lsn, err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	// 再次打开 WAL 用于 Append
	w = openWAL(t, dir, writerOptions())

	// 再次 Append
	if lsn, err := w.Append([]byte("second")); err != nil || lsn != 1 {
		t.Fatalf("second Append result is (%d, %v), want (1, nil)", lsn, err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	// 再次打开 WAL 用于 Replay
	w = openWAL(t, dir, wal.DefaultOptions())

	allRecords := readRecords(t, w, 0)
	if len(allRecords) != 2 {
		t.Fatalf("replayed %d records, want 2", len(allRecords))
	}
	if allRecords[0] != (replayRecord{0, "first"}) || allRecords[1] != (replayRecord{1, "second"}) {
		t.Fatalf("unexpected records: %+v", allRecords)
	}
}

func TestPayloadTooBig(t *testing.T) {
	dir := t.TempDir()

	opt := writerOptions()
	opt.MaxPayloadBytes = 100

	w := openWAL(t, dir, opt)

	// payload 过大应该 Append 失败
	if _, err := w.Append(make([]byte, 101)); !errors.Is(err, wal.ErrPayloadTooLarge) {
		t.Fatalf("oversized payload got error: %v, want %v", err, wal.ErrPayloadTooLarge)
	}

	// Append 失败后，LSN 不应该推进
	payload := bytes.Repeat([]byte("a"), 100)
	if lsn, err := w.Append(payload); err != nil || lsn != 0 {
		t.Fatalf("append got (%d, %v), want (0, nil)", lsn, err)
	}

	// 空 payload 可以正常 Append
	if lsn, err := w.Append(nil); err != nil || lsn != 1 {
		t.Fatalf("empyt append got (%d, %v), want (1, nil)", lsn, err)
	}
	if lsn, err := w.Append(make([]byte, 0)); err != nil || lsn != 2 {
		t.Fatalf("empyt append got (%d, %v), want (2, nil)", lsn, err)
	}

	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	// 重新打开 WAL 用于 Replay
	w = openWAL(t, dir, wal.DefaultOptions())
	records := readRecords(t, w, 0)
	if len(records) != 3 {
		t.Fatalf("replayed %d records, want 3", len(records))
	}
	if records[0] != (replayRecord{0, string(payload)}) ||
		records[1] != (replayRecord{1, ""}) || records[2] != (replayRecord{2, ""}) {
		t.Fatalf("unexpected records: %+v", records)
	}
}

// 测试 Replay 流程报错
func TestReplayErrors(t *testing.T) {
	dir := t.TempDir()
	w := openWAL(t, dir, writerOptions())

	if _, err := w.Append([]byte("first")); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Append([]byte("second")); err != nil {
		t.Fatal(err)
	}

	// Append 用的 WAL 不支持 Replay Trim 操作，避免潜在的冲突
	noop := func(wal.LSN, []byte) error {
		return nil
	}
	if err := w.Replay(0, noop); !errors.Is(err, wal.ErrOfflineRequired) {
		t.Fatalf("replay error is %v, want %v", err, wal.ErrOfflineRequired)
	}
	if err := w.Trim(1); !errors.Is(err, wal.ErrOfflineRequired) {
		t.Fatalf("trim error is %v, want %v", err, wal.ErrOfflineRequired)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	// 重新打开 WAL 用于离线操作，执行 Append 会报错
	w = openWAL(t, dir, wal.DefaultOptions())
	if _, err := w.Append([]byte("disabled")); !errors.Is(err, wal.ErrAppendDisabled) {
		t.Fatalf("append error is %v, want %v", err, wal.ErrAppendDisabled)
	}
	if err := w.Replay(0, nil); !errors.Is(err, wal.ErrNilReplayAction) {
		t.Fatalf("replay error is %v, want %v", err, wal.ErrNilReplayAction)
	}

	// 非法的 LSN 作为 start Replay 时，需要报错
	calls := 0
	err := w.Replay(2, func(wal.LSN, []byte) error {
		calls++
		return nil
	})
	if !errors.Is(err, wal.ErrLSNNotFound) || calls != 0 {
		t.Fatalf("invaild start LSN got error %v calls=%d, want error %v, calls=0", err, calls, wal.ErrLSNNotFound)
	}

	// Replay 过程中回调报错时，会立即结束整个 Replay，同时返回回调错误
	callbackErr := errors.New("stop replay")
	calls = 0
	err = w.Replay(0, func(wal.LSN, []byte) error {
		calls++
		return callbackErr
	})
	if !errors.Is(err, callbackErr) || calls != 1 {
		t.Fatalf("replay got error %v, calls=%d, want error %v, calls=1", err, calls, callbackErr)
	}

	// 之前的 Replay 失败，不应更影响后续 Replay
	if records := readRecords(t, w, 0); len(records) != 2 {
		t.Fatalf("replayed %d records, want 2", len(records))
	}
}

// 测试多个 WAL 实例重复打开同一个目录、关闭相关场景
func TestDirLockAndClose(t *testing.T) {
	dir := t.TempDir()
	w := openWAL(t, dir, writerOptions())

	// 重复使用同一个目录创建 WAL 应该报错
	for _, opt := range []wal.Options{
		writerOptions(),
		wal.DefaultOptions(),
	} {
		other, err := wal.Open(dir, opt)
		if other != nil {
			_ = other.Close()
		}
		if err == nil {
			t.Fatal("opening an already locked dir succeeded")
		}
	}

	// 允许重复 Close
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("repeated Close failed: %v", err)
	}

	// Close 后禁止 Append
	if _, err := w.Append([]byte("closed")); !errors.Is(err, wal.ErrClosed) {
		t.Fatalf("append after close got error %v, want %v", err, wal.ErrClosed)
	}

	// Close 后，可以正常使用该目录打开 WAL 实例
	_ = openWAL(t, dir, wal.DefaultOptions())
}

// 测试并发 Append
func TestConcurrentAppend(t *testing.T) {
	dir := t.TempDir()
	w := openWAL(t, dir, writerOptions())

	const count = 32

	type result struct {
		lsn     wal.LSN
		payload string
		err     error
	}

	results := make(chan result, count)
	start := make(chan struct{})

	// 并发 Append
	for i := 0; i < count; i++ {
		go func(i int) {
			<-start
			payload := fmt.Sprintf("%0100d", i)
			lsn, err := w.Append([]byte(payload))

			results <- result{lsn, payload, err}
		}(i)
	}

	close(start)

	want := make(map[wal.LSN]string, count)
	for i := 0; i < count; i++ {
		r := <-results
		if r.err != nil {
			t.Fatalf("applend failed: %v", r.err)
		}
		if _, exists := want[r.lsn]; exists {
			t.Fatalf("duplicate LSN %d", r.lsn)
		}
		want[r.lsn] = r.payload
	}

	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	// Replay 检查并发 Append 结果
	w = openWAL(t, dir, wal.DefaultOptions())
	records := readRecords(t, w, 0)

	if len(records) != count {
		t.Fatalf("replayed %d records, want %d", len(records), count)
	}

	for i, record := range records {
		if record.lsn != wal.LSN(i) {
			t.Errorf("record %d: LSN=%d, doesn't match", i, record.lsn)
		}
		payload, exists := want[record.lsn]
		if !exists || record.payload != payload {
			t.Errorf("record %d doesn't match Append result", i)
		}
	}
}

// 空 WAL 的 Append 和 Replay
func TestEmptyWAL(t *testing.T) {
	dir := t.TempDir()

	// 创建一个新的 WAL 实例，不执行任何 Append 就返回
	// 关闭 WAL 实例，日志文件中已经有文件头了，但是没有任何日志记录
	w := openWAL(t, dir, writerOptions())
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	// 重新打开 WAL 实例用于 Replay
	// Replay 空 WAL 时，应当返回 not found 错误
	w = openWAL(t, dir, wal.DefaultOptions())
	calls := 0
	err := w.Replay(0, func(wal.LSN, []byte) error {
		calls++
		return nil
	})
	if !errors.Is(err, wal.ErrLSNNotFound) || calls != 0 {
		t.Fatalf("empty replay got error: %v, calls=%d, want error: %v, calls=0", err, calls, wal.ErrLSNNotFound)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	// 重新打开 WAL 用于 Append 时，第一条记录 LSN 依然从 0 开始
	w = openWAL(t, dir, writerOptions())
	lsn, err := w.Append([]byte("first"))
	if err != nil || lsn != wal.LSN(0) {
		t.Fatalf("append on an empty WAL got error: %v, LNS=%d, want LSN=0", err, lsn)
	}
}

// 测试不合法参数
func TestInvalidOptions(t *testing.T) {
	badCases := []struct {
		name   string
		change func(*wal.Options)
		want   error
	}{
		{
			name: "zero queue capacity",
			change: func(opt *wal.Options) {
				opt.QueueCapacity = 0
			},
			want: wal.ErrQueueNonPositive,
		},
		{
			name: "negaitve queue capacity",
			change: func(opt *wal.Options) {
				opt.QueueCapacity = -1
			},
			want: wal.ErrQueueNonPositive,
		},
		{
			name: "zero payload limit",
			change: func(opt *wal.Options) {
				opt.MaxPayloadBytes = 0
			},
			want: wal.ErrMaxPayloadBytesNonPositive,
		},
		{
			name: "negative queue capacity",
			change: func(opt *wal.Options) {
				opt.MaxPayloadBytes = -1
			},
			want: wal.ErrMaxPayloadBytesNonPositive,
		},
	}

	for _, badcase := range badCases {
		t.Run(badcase.name, func(t *testing.T) {
			opt := wal.DefaultOptions()
			badcase.change(&opt)

			w, err := wal.Open(t.TempDir(), opt)
			if w != nil {
				_ = w.Close()
			}
			if !errors.Is(err, badcase.want) {
				t.Fatalf("using invalid option open error: %v, want %v", err, badcase.want)
			}
		})
	}

}

func TestSegmentedReplayAndTrim(t *testing.T) {
	dir := t.TempDir()

	opt := writerOptions()
	// 每条记录 116 字节，文件头 20 字节。
	// 串行提交时，每个分段容纳两条。
	opt.FileSizeThresholdBytes = 252

	w := openWAL(t, dir, opt)
	for i := 0; i < 6; i++ {
		payload := fmt.Sprintf("%0100d", i)
		if lsn, err := w.Append([]byte(payload)); err != nil || lsn != wal.LSN(i) {
			t.Fatalf("Append %d = (%d, %v)", i, lsn, err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	// 不开启轮转，也必须能读取已有多个分段。
	reader := openWAL(t, dir, wal.DefaultOptions())
	if records := readRecords(t, reader, 0); len(records) != 6 {
		t.Fatalf("records=%d, want 6", len(records))
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}

	// 开启分段配置，执行完整旧分段删除。
	offlineOpt := wal.DefaultOptions()
	offlineOpt.FileSizeThresholdBytes = 252
	reader = openWAL(t, dir, offlineOpt)

	if err := reader.Trim(4); err != nil {
		t.Fatal(err)
	}

	records := readRecords(t, reader, 4)
	if len(records) != 2 || records[0].lsn != 4 ||
		records[1].lsn != 5 {
		t.Fatalf("unexpected records after Trim: %+v",
			records)
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}

	// 关闭轮转，仍可对之前的多文件目录继续追加。
	w = openWAL(t, dir, writerOptions())
	if lsn, err := w.Append([]byte("next")); err != nil ||
		lsn != 6 {
		t.Fatalf("Append after Trim = (%d, %v)", lsn, err)
	}
}

// 测试修复文件尾部
func TestFixIncompleteTail(t *testing.T) {
	dir := t.TempDir()
	w := openWAL(t, dir, writerOptions())

	if _, err := w.Append([]byte("valid")); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	files, err := filepath.Glob(filepath.Join(dir, "*.wal"))
	if err != nil || len(files) != 1 {
		t.Fatalf("files=%v error=%v", files, err)
	}

	info, err := os.Stat(files[0])
	if err != nil {
		t.Fatal(err)
	}
	validSize := info.Size()

	f, err := os.OpenFile(files[0], os.O_WRONLY|os.O_APPEND,
		0)
	if err != nil {
		t.Fatal(err)
	}
	_, writeErr := f.Write([]byte{1, 2, 3}) // 不完整记录头
	closeErr := f.Close()
	if err := errors.Join(writeErr, closeErr); err != nil {
		t.Fatal(err)
	}

	reader := openWAL(t, dir, wal.DefaultOptions())

	if err := reader.Fix(); err != nil {
		t.Fatal(err)
	}
	if err := reader.Fix(); err != nil {
		t.Fatalf("repeated Fix: %v", err)
	}

	info, err = os.Stat(files[0])
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() != validSize {
		t.Fatalf("size=%d, want %d", info.Size(), validSize)
	}

	records := readRecords(t, reader, 0)
	if len(records) != 1 || records[0].payload != "valid" {
		t.Fatalf("unexpected records: %+v", records)
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}

	w = openWAL(t, dir, writerOptions())
	if lsn, err := w.Append([]byte("after fix")); err != nil || lsn != 1 {
		t.Fatalf("Append after Fix = (%d, %v)", lsn, err)
	}
}
