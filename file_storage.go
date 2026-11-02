package simplewal

// file_storage.go
// storage 接口的实现
// 支持底层使用多日志文件、支持日志文件的 trim

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"
)

type fileInfo struct {
	name     string
	firstLSN LSN
}

type fileStorage struct {
	rootDir string
	dir     *os.File

	files []fileInfo

	currentFile *os.File
	currentSize int64
	nextLSN     LSN

	fileSizeThresholdBytes int64
	writeBuffer            []byte
}

var _ storage = (*fileStorage)(nil)

func openFileStorage(
	rootDir string,
	opt Options,
) (storage, LSN, error) {
	s := &fileStorage{
		rootDir:                rootDir,
		fileSizeThresholdBytes: opt.FileSizeThresholdBytes,
	}

	ok := false
	defer func() {
		if !ok {
			_ = s.Close()
		}
	}()

	dir, err := os.Open(rootDir)
	if err != nil {
		return nil, 0, err
	}
	s.dir = dir

	info, err := dir.Stat()
	if err != nil {
		return nil, 0, err
	}
	if !info.IsDir() {
		return nil, 0, fmt.Errorf("simplewal: %q is not a directory", rootDir)
	}

	if err := syscall.Flock(
		int(dir.Fd()),
		syscall.LOCK_EX|syscall.LOCK_NB,
	); err != nil {
		return nil, 0, fmt.Errorf("simplewal: lock directory: %w", err)
	}

	if err := s.loadFiles(); err != nil {
		return nil, 0, err
	}

	if len(s.files) == 0 {
		if !opt.UsedForAppend || !opt.CreateIfMissing {
			return nil, 0, fmt.Errorf("simplewal: no WAL files: %w", os.ErrNotExist)
		}

		f, err := createFile(rootDir, dir, 0)
		if err != nil {
			return nil, 0, err
		}

		s.currentFile = f
		s.currentSize = fileHeaderSize
		s.files = []fileInfo{{
			name:     getFileName(0),
			firstLSN: 0,
		}}
	} else if opt.UsedForAppend {
		results, err := s.walk(nil, false)
		if err != nil {
			return nil, 0, err
		}

		last := s.files[len(s.files)-1]
		lastResult := results[len(results)-1]

		f, err := os.OpenFile(
			filepath.Join(rootDir, last.name),
			os.O_RDWR|os.O_APPEND,
			0,
		)
		if err != nil {
			return nil, 0, err
		}

		s.currentFile = f
		s.currentSize = lastResult.validEnd
		s.nextLSN = lastResult.nextLSN
	}

	if opt.UsedForAppend {
		// 目录同步一次
		if err := dir.Sync(); err != nil {
			return nil, 0, err
		}
	}

	ok = true
	return s, s.nextLSN, nil
}

// 只验证正式文件名称和文件头，不扫描文件内的日志记录。
// 遗留临时文件不参与日志读取。
func (s *fileStorage) loadFiles() error {
	entries, err := os.ReadDir(s.rootDir)
	if err != nil {
		return err
	}

	for _, entry := range entries {
		name := entry.Name()

		if !strings.HasSuffix(name, ".wal") {
			continue
		}

		if entry.Type()&os.ModeType != 0 {
			return fmt.Errorf("invalid file type %q: %w", name, ErrCorruput)
		}

		number := strings.TrimSuffix(name, ".wal")
		value, err := strconv.ParseUint(number, 10, 64)
		if err != nil || name != getFileName(LSN(value)) {
			return fmt.Errorf("invalid file name %q: %w", name, ErrCorruput)
		}

		f, err := os.Open(filepath.Join(s.rootDir, name))
		if err != nil {
			return err
		}

		firstLSN, headerErr := readFileHeader(f)
		closeErr := f.Close()
		if err := errors.Join(headerErr, closeErr); err !=
			nil {
			return fmt.Errorf("simplewal: %s: %w", name, err)
		}
		if firstLSN != LSN(value) {
			return fmt.Errorf("filename/header mismatch in %s: %w", name, ErrCorruput)
		}

		s.files = append(s.files, fileInfo{
			name:     name,
			firstLSN: firstLSN,
		})
	}

	sort.Slice(s.files, func(i, j int) bool {
		return s.files[i].firstLSN <
			s.files[j].firstLSN
	})

	return nil
}

// walk 校验文件间连续性并扫描记录。
// allowIncompleteTail 仅允许最后文件的不完整尾记录。
func (s *fileStorage) walk(
	action func(LSN, []byte) error,
	allowIncompleteTail bool,
) ([]scanResult, error) {
	results := make([]scanResult, 0, len(s.files))
	expected := s.files[0].firstLSN

	for i, file := range s.files {
		if file.firstLSN != expected {
			return nil, fmt.Errorf("file %s starts at %d, want %d: %w", file.name, file.firstLSN, expected, ErrCorruput)
		}

		f, err := os.Open(filepath.Join(s.rootDir, file.name))
		if err != nil {
			return nil, err
		}

		result, scanErr := scanRecords(f, file.firstLSN, action)
		closeErr := f.Close()

		if closeErr != nil {
			return nil, errors.Join(scanErr, closeErr)
		}

		if scanErr != nil {
			isLast := i == len(s.files)-1
			if !allowIncompleteTail || !isLast || !result.incomplete {
				return nil, fmt.Errorf("simplewal: %s: %w", file.name, scanErr)
			}
		}

		results = append(results, result)
		expected = result.nextLSN
	}

	return results, nil
}

func (s *fileStorage) rotate(firstLSN LSN) error {
	f, err := createFile(s.rootDir, s.dir, firstLSN)
	if err != nil {
		return err
	}

	old := s.currentFile
	s.currentFile = f
	s.currentSize = fileHeaderSize
	s.files = append(s.files, fileInfo{
		name:     getFileName(firstLSN),
		firstLSN: firstLSN,
	})

	// 之前的批次已经 Sync 成功。
	// 先让新文件归 storage 所有，确保关闭旧文件失败时也不会 泄漏。
	return old.Close()
}

func (s *fileStorage) WriteRecord(
	lsn LSN,
	payload []byte,
) error {
	return s.WriteBatch(lsn, [][]byte{payload})
}

func (s *fileStorage) WriteBatch(
	firstLSN LSN,
	payloads [][]byte,
) error {
	if len(payloads) == 0 {
		return nil
	}
	if s.currentFile == nil {
		return ErrAppendDisabled
	}
	if firstLSN != s.nextLSN {
		return fmt.Errorf("write starts at %d, want %d: %w", firstLSN, s.nextLSN, ErrCorruput)
	}

	buffer, err := encodeBatch(s.writeBuffer, firstLSN, payloads)
	if err != nil {
		return err
	}
	s.writeBuffer = buffer

	// 仅在写入时检查文件大小的阈值，读取时不用管
	// 写入时如果遇到文件需要 rota，不拆批次，保证同一批不跨文件
	limit := s.fileSizeThresholdBytes
	if limit > 0 && s.currentSize > fileHeaderSize &&
		(s.currentSize >= limit || int64(len(buffer)) > limit-s.currentSize) {
		if err := s.rotate(firstLSN); err != nil {
			return err
		}
	}

	const maxInt64 = int64(1<<63 - 1)
	if int64(len(buffer)) > maxInt64-s.currentSize {
		return fmt.Errorf("simplewal: file size overflows int64")
	}

	if err := writeAll(s.currentFile, buffer); err != nil {
		return err
	}

	s.currentSize += int64(len(buffer))
	s.nextLSN += LSN(len(payloads))
	return nil
}

func (s *fileStorage) Sync() error {
	if s.currentFile == nil {
		return ErrAppendDisabled
	}
	return s.currentFile.Sync()
}

func (s *fileStorage) Replay(
	start LSN,
	action func(LSN, []byte) error,
) error {
	if start < s.files[0].firstLSN {
		return ErrLSNNotFound
	}

	found := false
	_, err := s.walk(func(lsn LSN, payload []byte) error {
		if lsn < start {
			return nil
		}
		if lsn == start {
			found = true
		}
		return action(lsn, payload)
	}, false)

	if err != nil {
		return err
	}
	if !found {
		return ErrLSNNotFound
	}
	return nil
}

func (s *fileStorage) Trim(before LSN) error {
	if s.fileSizeThresholdBytes == 0 {
		return ErrTrimUnsupported
	}

	// 修改前先完成全量校验，文件损坏就直接报错
	results, err := s.walk(nil, false)
	if err != nil {
		return err
	}

	removeCount := 0
	for i := 0; i < len(s.files)-1; i++ {
		// 最后一条记录小于 before，等价于 nextLSN <= before
		if results[i].nextLSN > before {
			break
		}
		removeCount++
	}

	for i := 0; i < removeCount; i++ {
		oldest := s.files[0]
		if err := os.Remove(
			filepath.Join(s.rootDir, oldest.name),
		); err != nil {
			return err
		}

		// 先更新删除后的文件列表，再 sync dir
		// 如果 dir sync 失败，就先不重试了，直接报错
		s.files[0] = fileInfo{}
		s.files = s.files[1:]

		if err := s.dir.Sync(); err != nil {
			return err
		}
	}
	return nil
}

func (s *fileStorage) Fix() error {
	results, err := s.walk(nil, true)
	if err != nil {
		return err
	}

	lastResult := results[len(results)-1]
	if !lastResult.incomplete {
		return nil
	}

	last := s.files[len(s.files)-1]
	f, err := os.OpenFile(filepath.Join(s.rootDir, last.name), os.O_RDWR, 0)
	if err != nil {
		return err
	}

	truncateErr := f.Truncate(lastResult.validEnd)
	var syncErr error
	if truncateErr == nil {
		syncErr = f.Sync()
	}

	return errors.Join(truncateErr, syncErr, f.Close())
}

func (s *fileStorage) Close() error {
	var fileErr, dirErr error

	if s.currentFile != nil {
		fileErr = s.currentFile.Close()
		s.currentFile = nil
	}
	if s.dir != nil {
		dirErr = s.dir.Close()
		s.dir = nil
	}

	s.writeBuffer = nil
	return errors.Join(fileErr, dirErr)
}
