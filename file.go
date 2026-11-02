package simplewal

// file.go
// 提供底层日志文件的结构和辅助函数
// 文件结构规定了文件头的格式
// 辅助函数包括生成文件名、创建文件、读取文件头等

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
)

const (
	fileMagic      = "SWAL0001"
	fileHeaderSize = 20
)

func getFileName(firstLSN LSN) string {
	return fmt.Sprintf("%020d.wal", firstLSN)
}

func readFileHeader(file *os.File) (LSN, error) {

	fileInfo, err := file.Stat()
	if err != nil {
		return 0, fmt.Errorf("simplewal: stat log file failed: %w", err)
	}
	if !fileInfo.Mode().IsRegular() {
		return 0, fmt.Errorf("simplewal: log file is not a regular file")
	}
	if fileInfo.Size() < fileHeaderSize {
		return 0, fmt.Errorf("simplwal: incomplete file header, %w", ErrCorruput)
	}

	var header [fileHeaderSize]byte

	if _, err := file.ReadAt(header[:], 0); err != nil {
		return 0, fmt.Errorf("simplewal: read file header error: %w", err)
	}

	if !bytes.Equal(header[:8], []byte(fileMagic)) {
		return 0, fmt.Errorf("simplewal: invaild file magic or version: %w", ErrCorruput)
	}

	wantCRC := binary.LittleEndian.Uint32(header[16:20])
	gotCRC := crc32.Checksum(header[:16], recordCRCTable)
	if gotCRC != wantCRC {
		return 0, fmt.Errorf("simplewal: file header checksum mismatch: %w", ErrCorruput)
	}

	return LSN(binary.LittleEndian.Uint64(header[8:16])), nil
}

func createFile(
	rootDir string,
	dir *os.File,
	firstLSN LSN,
) (*os.File, error) {
	finalPath := filepath.Join(rootDir, getFileName(firstLSN))

	if _, err := os.Lstat(finalPath); err == nil {
		return nil, fmt.Errorf("simplewal: log file already exists: %s", finalPath)
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}

	file, err := os.CreateTemp(rootDir, ".simplewal-*.tmp")
	if err != nil {
		return nil, err
	}

	tempPath := file.Name()
	ok := false

	defer func() {
		if !ok {
			_ = file.Close()
		}
		_ = os.Remove(tempPath)
	}()

	var header [fileHeaderSize]byte
	copy(header[:8], fileMagic)
	binary.LittleEndian.PutUint64(header[8:16], uint64(firstLSN))
	binary.LittleEndian.PutUint32(header[16:20], crc32.Checksum(header[:16], recordCRCTable))

	// 写入文件头，同时 sync 持久化
	if err := writeAll(file, header[:]); err != nil {
		return nil, err
	}
	if err := file.Sync(); err != nil {
		return nil, err
	}

	// rename temp file 到最终文件，sync 目录持久化 dentry
	if err := os.Rename(tempPath, finalPath); err != nil {
		return nil, err
	}
	if err := dir.Sync(); err != nil {
		return nil, err
	}

	ok = true
	return file, nil
}
