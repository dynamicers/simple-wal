package simplewal

import (
	"bytes"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func getValidSingleFileBytes(t *testing.T) []byte {
	t.Helper()

	dir := t.TempDir()
	opt := DefaultOptions()
	opt.CreateIfMissing = true
	opt.UsedForAppend = true

	w, err := Open(dir, opt)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		_ = w.Close()
	})

	for _, value := range []byte{'a', 'b'} {
		if _, err := w.Append(bytes.Repeat([]byte{value}, 100)); err != nil {
			t.Fatal(err)
		}
	}

	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	data, err := os.ReadFile(filepath.Join(dir, getFileName(0)))
	if err != nil {
		t.Fatal(err)
	}
	return data
}

// 测试不同情况的日志文件损坏
func TestSingleFileCorrpution(t *testing.T) {
	validData := getValidSingleFileBytes(t)
	fileHeaderSize := fileHeaderSize
	secondOffset := fileHeaderSize + recordHeaderSize + 100

	// 构造不同种类的文件损坏 case
	badCases := []struct {
		name             string
		mutate           func([]byte) []byte
		badFileHeader    bool
		validRecordCount int
	}{
		{
			name: "incomplete file header",
			mutate: func(b []byte) []byte {
				return b[:fileHeaderSize-1]
			},
			badFileHeader: true,
		},
		{
			name: "invalid magic",
			mutate: func(b []byte) []byte {
				b[0] ^= 1
				return b
			},
			badFileHeader: true,
		},
		{
			name: "invalid header crc",
			mutate: func(b []byte) []byte {
				b[fileHeaderSize-1] ^= 1
				return b
			},
			badFileHeader: true,
		},
		{
			name: "incomplete record header",
			mutate: func(b []byte) []byte {
				return b[:fileHeaderSize+recordHeaderSize-1]
			},
		},
		{
			name: "incomplete payload",
			mutate: func(b []byte) []byte {
				return b[:len(b)-1]
			},
			validRecordCount: 1,
		},
		{
			name: "checksum mismatch",
			mutate: func(b []byte) []byte {
				b[len(b)-1] ^= 1
				return b
			},
			validRecordCount: 1,
		},
		{
			name: "unexpected LSN with valid checksum",
			mutate: func(b []byte) []byte {
				record := b[secondOffset:]
				binary.LittleEndian.PutUint64(record[:8], 9)
				crc := recordChecksum(record[:12], record[recordHeaderSize:])
				binary.LittleEndian.PutUint32(record[12:16], crc)
				return b
			},
			validRecordCount: 1,
		},
		{
			name: "lenght exceeds",
			mutate: func(b []byte) []byte {
				binary.LittleEndian.PutUint32(b[secondOffset+8:secondOffset+12], ^uint32(0))
				return b
			},
			validRecordCount: 1,
		},
	}

	for _, badCase := range badCases {
		t.Run(badCase.name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, getFileName(0))
			brokenData := badCase.mutate(bytes.Clone(validData))

			if err := os.WriteFile(path, brokenData, 0o600); err != nil {
				t.Fatal(err)
			}

			// 工具函数，用来检查构造的 corrupt 日志没有被修改
			assertUnchanged := func() {
				t.Helper()

				got, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(got, brokenData) {
					t.Fatal("corrupt file was modified")
				}
			}

			// 打开 corrupt 的日志文件用来 Append，Open 时就应该直接报错
			opt := DefaultOptions()
			opt.UsedForAppend = true

			w, err := Open(dir, opt)
			if w != nil {
				_ = w.Close()
			}
			if !errors.Is(err, ErrCorruput) {
				t.Fatalf("open for append failed: %v, want %v", err, ErrCorruput)
			}

			// Open 不会自动修复 corrupt 的日志文件
			assertUnchanged()

			// 重新打开 WAL 用来 Replay
			w, err = Open(dir, DefaultOptions())

			// 文件头损坏时，Open 就应该直接报错，同时也不修改日志文件
			if badCase.badFileHeader {
				if w != nil {
					_ = w.Close()
				}
				if !errors.Is(err, ErrCorruput) {
					t.Fatalf("offline open error: %v, want %v", err, ErrCorruput)
				}
				assertUnchanged()
				return
			}

			// 非文件头损坏的 badcase 可以正常创建 WAL 实例，创建成功后注册清理函数关闭 WAL 实例
			if err != nil {
				t.Fatalf("offlien open filed: %v", err)
			}
			t.Cleanup(func() {
				if err := w.Close(); err != nil {
					t.Errorf("Close WAL failed: %v", err)
				}
			})

			// 非文件头损坏的 badcase 进行 Replay，Replay 一定会因为文件损坏报错
			// 正常的日志记录会执行 callback，损坏的日志记录不执行
			calls := 0
			callback := func(lsn LSN, payload []byte) error {
				if lsn != LSN(calls) {
					t.Errorf("callback LSN=%d, want %d", lsn, calls)
				}

				want := bytes.Repeat([]byte{byte('a' + calls)}, 100)

				if !bytes.Equal(payload, want) {
					t.Errorf("callback payload mismatch at LSN %d", lsn)
				}
				calls++
				return nil
			}
			err = w.Replay(0, callback)

			if !errors.Is(err, ErrCorruput) {
				t.Fatalf("replay error: %v, want %v", err, ErrCorruput)
			}
			if calls != badCase.validRecordCount {
				t.Errorf("callback %d times, want %d", calls, badCase.validRecordCount)
			}

			assertUnchanged()
		})
	}
}

// 测试 Trim 行为
func TestTrim(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, getFileName(0))
	validData := getValidSingleFileBytes(t)

	if err := os.WriteFile(path, validData, 0o600); err != nil {
		t.Fatal(err)
	}

	w, err := Open(dir, DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = w.Close() })

	// 单文件日志执行 Trim 一定会报错
	if err := w.Trim(1); !errors.Is(err, ErrTrimUnsupported) {
		t.Fatalf("trim error: %v, want %v", err, ErrTrimUnsupported)
	}

	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(after, validData) {
		t.Fatal("Trim should't modify wal file for single file WAL")
	}
}
