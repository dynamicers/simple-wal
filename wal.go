package simplewal

// wal.go
// wal 逻辑层，对外提供 wal 接口
// 接口包括 Open Append Trim Fix Close 等

import (
	"errors"
	"sync"
)

type LSN uint64

type WAL struct {
	// WAL 同步原语
	lock     sync.Mutex
	notEmpty *sync.Cond
	notFull  *sync.Cond

	// 持有的底层资源
	store storage

	// 配置项
	usedForAppend   bool
	maxPayloadBytes int
	maxBatchBytes   int
	queueCapacity   int

	// 日志队列
	head         *appendBatch
	tail         *appendBatch
	ququeRecords int

	// 日志进入队列时分配的 LSN
	nextLSN LSN

	// WAL 的生命周期状态、错误状态
	closing  bool
	closed   bool
	writeErr error
	closeErr error

	//
	writerDone chan struct{}
	closeDone  chan struct{}
}

func Open(dir string, opt Options) (*WAL, error) {
	err := validOptions(opt)
	if err != nil {
		return nil, err
	}

	// store, nextLSN, err := openSingleFileStorage(dir, opt)
	store, nextLSN, err := openFileStorage(dir, opt)
	if err != nil {
		return nil, err
	}

	w := &WAL{
		store:           store,
		usedForAppend:   opt.UsedForAppend,
		maxPayloadBytes: opt.MaxPayloadBytes,
		maxBatchBytes:   opt.MaxBatchBytes,
		queueCapacity:   opt.QueueCapacity,
		nextLSN:         nextLSN,
		closeDone:       make(chan struct{}),
	}

	if w.usedForAppend {
		w.notEmpty = sync.NewCond(&w.lock)
		w.notFull = sync.NewCond(&w.lock)
		w.writerDone = make(chan struct{})

		go w.runWriter()
	}

	return w, nil
}

func (w *WAL) Append(payload []byte) (LSN, error) {
	if len(payload) > w.maxPayloadBytes {
		return 0, ErrPayloadTooLarge
	}

	recordBytes := recordHeaderSize + len(payload)

	w.lock.Lock()
	// defer w.lock.Unlock()

	for {
		if w.closing || w.closed {
			w.lock.Unlock()
			return 0, ErrClosed
		}
		if !w.usedForAppend {
			w.lock.Unlock()
			return 0, ErrAppendDisabled
		}
		if w.writeErr != nil {
			err := w.writeErr
			w.lock.Unlock()
			return 0, err
		}
		if w.nextLSN == ^LSN(0) {
			w.lock.Unlock()
			return 0, ErrLSNExhausted
		}

		if w.ququeRecords < w.queueCapacity {
			break
		}

		w.notFull.Wait()
	}

	lsn := w.nextLSN
	batch := w.tail

	if batch == nil ||
		recordBytes > w.maxBatchBytes ||
		batch.bytes > w.maxBatchBytes-recordBytes {
		batch = &appendBatch{
			firstLSN: lsn,
			done:     make(chan struct{}),
		}

		if w.tail == nil {
			w.head = batch
		} else {
			w.tail.next = batch
		}
		w.tail = batch
	}

	batch.payloads = append(batch.payloads, payload)
	batch.bytes += recordBytes

	w.nextLSN++
	w.ququeRecords++

	w.notEmpty.Signal()
	w.lock.Unlock()

	<-batch.done

	if batch.err != nil {
		return 0, batch.err
	}
	return lsn, nil
}

func (w *WAL) Replay(start LSN, action func(LSN, []byte) error) error {
	w.lock.Lock()
	defer w.lock.Unlock()

	if w.closing || w.closed {
		return ErrClosed
	}
	if w.usedForAppend {
		return ErrOfflineRequired
	}
	if action == nil {
		return ErrNilReplayAction
	}

	return w.store.Replay(start, action)
}

func (w *WAL) Trim(before LSN) error {
	w.lock.Lock()
	defer w.lock.Unlock()

	if w.closing || w.closed {
		return ErrClosed
	}
	if w.usedForAppend {
		return ErrOfflineRequired
	}

	return w.store.Trim(before)
}

func (w *WAL) Fix() error {
	w.lock.Lock()
	defer w.lock.Unlock()

	if w.closing || w.closed {
		return ErrClosed
	}
	if w.usedForAppend {
		return ErrOfflineRequired
	}

	return w.store.Fix()
}

// 允许重复 Close
func (w *WAL) Close() error {
	w.lock.Lock()
	// defer w.lock.Unlock()

	if w.closed {
		err := w.closeErr
		w.lock.Unlock()
		return err
	}

	if w.closing {
		done := w.closeDone
		w.lock.Unlock()

		<-done

		w.lock.Lock()
		err := w.closeErr
		w.lock.Unlock()

		return err
	}

	w.closing = true
	if w.usedForAppend {
		w.notEmpty.Broadcast()
		w.notFull.Broadcast()
	}

	// 等 writer 前，先释放锁
	w.lock.Unlock()
	if w.usedForAppend {
		<-w.writerDone
	}

	storageErr := w.store.Close()

	w.lock.Lock()
	w.closeErr = errors.Join(w.writeErr, storageErr)
	w.closed = true

	close(w.closeDone)

	err := w.closeErr
	w.lock.Unlock()

	return err
}
