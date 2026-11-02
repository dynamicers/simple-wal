package simplewal_test

import (
	"fmt"
	"os"
	"sync"
	"testing"

	wal "github.com/dynamicers/simple-wal"
)

func BenchmarkConcurrentAppend(b *testing.B) {
	rootDir := os.Getenv("WAL_BENCH_DIR")
	if rootDir == "" {
		b.Skip("Please set WAL_BENCH_DIR env first")
	}

	for _, workerCount := range []int{1, 4, 16, 64, 128, 256} {
		b.Run(fmt.Sprintf("%d workers", workerCount), func(b *testing.B) {
			b.StopTimer()

			dir, err := os.MkdirTemp(rootDir, "simplewal-bench-")
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() {
				if err := os.RemoveAll(dir); err != nil {
					b.Errorf("clean bench temp dir failed: %v", err)
				}
			})

			opt := wal.DefaultOptions()
			opt.CreateIfMissing = true
			opt.UsedForAppend = true

			w, err := wal.Open(dir, opt)
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() {
				if err := w.Close(); err != nil {
					b.Errorf("close wal failed: %v", err)
				}
			})

			// 用固定大小的 payload 测试，固定 100 bytes
			payload := make([]byte, 100)
			var ready, finish sync.WaitGroup
			ready.Add(workerCount)
			finish.Add(workerCount)

			start := make(chan struct{})
			errs := make(chan error, workerCount)

			for workerId := 0; workerId < workerCount; workerId++ {
				task := func(workerId int) {
					defer finish.Done()
					ready.Done()
					<-start

					// 每个 worker 重复 Append，所有 worker 平分 b.N
					for i := workerId; i < b.N; i += workerCount {
						if _, err := w.Append(payload); err != nil {
							errs <- err
							return
						}
					}
				}
				go task(workerId)
			}

			ready.Wait()

			b.SetBytes(int64(len(payload)))
			b.ReportAllocs()
			b.ResetTimer()
			b.StartTimer()

			close(start)
			finish.Wait()

			b.StopTimer()
			close(errs)

			for err := range errs {
				b.Fatalf("append error: %v", err)
			}
		})
	}
}
