package simplewal

// writer.go
// wal 层单独的写入组件，负责日志的批量提交和持久化

import "fmt"

type appendBatch struct {
	firstLSN LSN
	payloads [][]byte
	bytes    int // 编码后包含日志头的总字节
	done     chan struct{}
	err      error
	next     *appendBatch
}

func (w *WAL) runWriter() {
	defer close(w.writerDone)

	for {
		batch := w.takeBatch()
		if batch == nil {
			return
		}

		if err := w.commitBatch(batch); err != nil {
			w.failWriter(err, batch)
			return
		}

		finishBatch(batch, nil)
	}
}

func (w *WAL) takeBatch() *appendBatch {
	w.lock.Lock()
	defer w.lock.Unlock()

	for w.head == nil && !w.closing {
		w.notEmpty.Wait()
	}

	if w.head == nil {
		return nil
	}

	batch := w.head
	w.head = batch.next
	batch.next = nil

	if w.head == nil {
		w.tail = nil
	}

	w.ququeRecords -= len(batch.payloads)

	w.notFull.Broadcast()

	return batch
}

func (w *WAL) commitBatch(batch *appendBatch) error {
	if err := w.store.WriteBatch(batch.firstLSN, batch.payloads); err != nil {
		return fmt.Errorf("simplewal: write batch starting at %d failed: %w", batch.firstLSN, err)
	}

	if err := w.store.Sync(); err != nil {
		return fmt.Errorf("simplewal: sync batch starting at %d failed:%w", batch.firstLSN, err)
	}

	return nil
}

func (w *WAL) failWriter(err error, current *appendBatch) {
	w.lock.Lock()

	w.writeErr = err

	pending := w.head
	w.head = nil
	w.tail = nil
	w.ququeRecords = 0

	w.notFull.Broadcast()

	w.lock.Unlock()

	finishBatch(current, err)

	for pending != nil {
		next := pending.next
		pending.next = nil

		finishBatch(pending, err)
		pending = next
	}
}

func finishBatch(batch *appendBatch, err error) {
	for i := range batch.payloads {
		batch.payloads[i] = nil
	}
	batch.payloads = nil

	batch.err = err
	close(batch.done)
}
