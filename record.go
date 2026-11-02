package simplewal

// record.go
// 提供持久化的日志记录的结构和辅助函数
// 日志记录结构规定了在硬盘文件上日志记录的具体格式
// 辅助函数包括编码、写入、校验等

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
)

const recordHeaderSize = 16

var recordCRCTable = crc32.MakeTable(crc32.Castagnoli)

type scanResult struct {
	nextLSN    LSN
	validEnd   int64 // 最后一条完整日志结束的位置
	incomplete bool  // 日志不完整
}

func encodeBatch(buffer []byte, firstLSN LSN, payloads [][]byte) ([]byte, error) {
	if len(payloads) == 0 {
		return nil, nil
	}

	// LSN 不够分配时，当前批次直接全部失败，就不尽力写了，保持语义简单
	avaiable := uint64(^LSN(0) - firstLSN)
	if uint64(len(payloads)) > avaiable {
		return nil, ErrLSNExhausted
	}

	maxInt := int(^uint(0) >> 1)
	totalBytes := 0

	// 处理一些极端边界情况
	for _, payload := range payloads {
		if uint64(len(payload)) > uint64(^uint32(0)) {
			return nil, fmt.Errorf("simplewal: payload length exceed uint32")
		}
		if len(payload) > maxInt-recordHeaderSize {
			return nil, fmt.Errorf("simplewal: encoded record size overflows int")
		}
		recordBytes := recordHeaderSize + len(payload)
		if totalBytes > maxInt-recordBytes {
			return nil, fmt.Errorf("simplewal: encoded batch size overflows int")
		}

		totalBytes += recordBytes
	}

	if cap(buffer) < totalBytes {
		buffer = make([]byte, totalBytes)
	} else {
		buffer = buffer[:totalBytes]
	}

	// 对所有日志编码
	offset := 0
	for i, payload := range payloads {
		recordBytes := recordHeaderSize + len(payload)
		record := buffer[offset : offset+recordBytes]

		binary.LittleEndian.PutUint64(record[0:8], uint64(firstLSN+LSN(i)))
		binary.LittleEndian.PutUint32(record[8:12], uint32(len(payload)))
		copy(record[recordHeaderSize:], payload)

		checksum := recordChecksum(record[:12], record[recordHeaderSize:])
		binary.LittleEndian.PutUint32(record[12:16], checksum)

		offset += recordBytes
	}

	return buffer, nil
}

// scanRecords 从文件头开始读取、校验所有日志记录，并执行回调
// action 回调为 nil 时，只读取检验
// action 执行错误时，立即返回错误
func scanRecords(
	file *os.File,
	firstLSN LSN,
	action func(LSN, []byte) error,
) (scanResult, error) {
	result := scanResult{
		nextLSN:  firstLSN,
		validEnd: fileHeaderSize,
	}

	headerLSN, err := readFileHeader(file)
	if err != nil {
		return result, err
	}
	if headerLSN != firstLSN {
		return result, fmt.Errorf("simplewa: scan failed, filename differ file header LSN: %w", ErrCorruput)
	}

	fileInfo, err := file.Stat()
	if err != nil {
		return result, fmt.Errorf("simplewal: stat log file failed: %w", err)
	}
	if fileInfo.Size() < int64(fileHeaderSize) {
		return result, fmt.Errorf("simplewal: incomplete file header: %w", ErrCorruput)
	}

	reader := io.NewSectionReader(
		file,
		fileHeaderSize,
		fileInfo.Size()-fileHeaderSize,
	)

	var recordHeader [recordHeaderSize]byte

	for {
		_, err := io.ReadFull(reader, recordHeader[:])

		// ReadFull 返回 EOF 是正常的终止条件
		if err == io.EOF {
			return result, nil
		}
		if err != nil {
			if errors.Is(err, io.ErrUnexpectedEOF) {
				result.incomplete = true
				return result, fmt.Errorf("simplewal: incomplete record header at offset %d: %w", result.validEnd, ErrCorruput)
			}
			return result, fmt.Errorf("simplewal: read header at offset %d faield: %w", result.validEnd, err)
		}

		lsn := LSN(binary.LittleEndian.Uint64(recordHeader[0:8]))
		length := binary.LittleEndian.Uint32(recordHeader[8:12])
		wantCRC := binary.LittleEndian.Uint32(recordHeader[12:16])

		// make payload 数组前先检查一下文件中记录的 length 是否合法，提前规避一些非法大 length 的情况，省点内存
		remaining := fileInfo.Size() - result.validEnd - recordHeaderSize
		if int64(length) > remaining {
			result.incomplete = true
			return result, fmt.Errorf("simplewal: incomplete payload at offset %d, LSN %d: %w", result.validEnd, lsn, ErrCorruput)
		}

		payload := make([]byte, int(length))
		if _, err := io.ReadFull(reader, payload); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				result.incomplete = true
				return result, fmt.Errorf("simplewal: incomplete payload at offset %d: %w", result.validEnd, ErrCorruput)
			}

			return result, fmt.Errorf("simplewal: read payload at offset %d failed: %w", result.validEnd, err)
		}

		currentCRC := recordChecksum(recordHeader[:12], payload)
		if currentCRC != wantCRC {
			return result, fmt.Errorf("simplewal: checksum mismatch at offset %d: %w", result.validEnd, ErrCorruput)
		}

		if lsn != result.nextLSN || lsn == ^LSN(0) {
			return result, fmt.Errorf("simplewal: unexcpected LSN %d, want %d at offset %d: %w", lsn, result.nextLSN, result.validEnd, ErrCorruput)
		}

		if action != nil {
			if err := action(lsn, payload); err != nil {
				return result, fmt.Errorf("simplewal: replay failed at offset %d LSN(%d): %w", result.validEnd, lsn, err)
			}
		}

		result.nextLSN++
		result.validEnd += recordHeaderSize + int64(length)
	}
}

func recordChecksum(header, payload []byte) uint32 {
	checksum := crc32.Update(0, recordCRCTable, header)
	return crc32.Update(checksum, recordCRCTable, payload)
}

func writeAll(w io.Writer, data []byte) error {
	for len(data) > 0 {
		n, err := w.Write(data)
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		data = data[n:]
	}
	return nil
}
