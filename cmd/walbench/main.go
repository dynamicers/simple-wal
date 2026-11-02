package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"math"
	"os"
	"sort"
	"sync"
	"time"

	simplewal "github.com/dynamicers/simple-wal"
)

const latenctyCapacityPerWorker = 64 * 1024

type config struct {
	dir         string
	workerCount int
	duration    time.Duration
	size        int
}

type workerResult struct {
	latencies []time.Duration
	err       error
}

func main() {
	cfg := config{}

	flag.StringVar(
		&cfg.dir,
		"dir",
		"",
		"existiing dir to save wal log file",
	)
	flag.IntVar(
		&cfg.workerCount,
		"workers",
		16,
		"number of concurrent Append callers",
	)
	flag.DurationVar(
		&cfg.duration,
		"duration",
		10*time.Second,
		"duration for submmitting logs",
	)
	flag.IntVar(
		&cfg.size,
		"size",
		100,
		"payload bytes per log entry",
	)
	flag.Parse()

	if err := run(cfg); err != nil {
		fmt.Fprintln(os.Stderr, "walbench: ", err)
		os.Exit(1)
	}
}

func run(cfg config) error {
	opt := simplewal.DefaultOptions()
	opt.CreateIfMissing = true
	opt.UsedForAppend = true

	if cfg.dir == "" {
		return errors.New("-dir is required")
	}
	if cfg.workerCount <= 0 {
		return errors.New("-workers must be positive")
	}
	if cfg.duration <= 0 {
		return errors.New("-duration must be positive")
	}
	if cfg.size <= 0 || cfg.size > opt.MaxPayloadBytes {
		return fmt.Errorf("-size must be between 1 and %d", opt.MaxPayloadBytes)
	}

	dir, err := os.MkdirTemp(cfg.dir, "simplewal-bench-")
	if err != nil {
		return fmt.Errorf("create bench dir failed: %w", err)
	}

	fmt.Printf("WAL dir: %s\n", dir)

	w, err := simplewal.Open(dir, opt)
	if err != nil {
		return err
	}
	defer w.Close()

	payload := bytes.Repeat([]byte{0xAB}, cfg.size)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var ready sync.WaitGroup
	ready.Add(cfg.workerCount)

	start := make(chan struct{})
	results := make(chan workerResult, cfg.workerCount)

	var deadline time.Time

	for worker := 0; worker < cfg.workerCount; worker++ {
		task := func() {
			result := workerResult{
				latencies: make([]time.Duration, 0, latenctyCapacityPerWorker),
			}
			defer func() {
				results <- result
			}()

			ready.Done()
			<-start

			for {
				select {
				case <-ctx.Done():
					return
				default:
				}

				begin := time.Now()
				if !begin.Before(deadline) {
					return
				}

				_, err := w.Append(payload)
				latency := time.Since(begin)

				if err != nil {
					result.err = err
					cancel()
					return
				}

				result.latencies = append(result.latencies, latency)
			}
		}

		go task()
	}

	ready.Wait()
	begin := time.Now()
	deadline = begin.Add(cfg.duration)
	close(start)

	allResults := make([]workerResult, 0, cfg.workerCount)
	var appendErr error
	var count int

	for i := 0; i < cfg.workerCount; i++ {
		result := <-results
		allResults = append(allResults, result)
		count += len(result.latencies)

		if appendErr == nil && result.err != nil {
			appendErr = result.err
		}
	}

	elapsed := time.Since(begin)
	closeErr := w.Close()
	if err := errors.Join(appendErr, closeErr); err != nil {
		return err
	}
	if count == 0 {
		return errors.New("no log entry was written")
	}

	latencies := make([]time.Duration, 0, count)
	var totalNS float64

	for i := range allResults {
		for _, latency := range allResults[i].latencies {
			totalNS += float64(latency.Nanoseconds())
		}
		latencies = append(latencies, allResults[i].latencies...)
		allResults[i].latencies = nil
	}

	sort.Slice(latencies, func(i, j int) bool {
		return latencies[i] < latencies[j]
	})

	seconds := elapsed.Seconds()
	payloadMiB := float64(count) * float64(cfg.size) / (1024 * 1024)
	fmt.Printf("workers: %d\n", cfg.workerCount)
	fmt.Printf("payload: %d bytes\n", cfg.size)
	fmt.Printf("submit duration: %s\n", cfg.duration)
	fmt.Println("--------------------------")
	fmt.Printf("actual elapsed:  %s\n", elapsed)
	fmt.Printf("total log count: %d\n", count)
	fmt.Printf("QPS:             %.2f logs/s\n", float64(count)/seconds)
	fmt.Printf("BW:              %.3f MiB/s\n", payloadMiB/seconds)
	fmt.Printf("latency avg:     %.3f us\n", totalNS/float64(count)/1000)
	fmt.Printf("latency min:     %s\n", latencies[0])
	fmt.Printf("latency p50:     %s\n", percentile(latencies, 0.5))
	fmt.Printf("latency p95:     %s\n", percentile(latencies, 0.95))
	fmt.Printf("latency p99:     %s\n", percentile(latencies, 0.99))
	fmt.Printf("latency max:     %s\n", latencies[len(latencies)-1])
	fmt.Println()

	return nil
}

func percentile(sorted []time.Duration, p float64) time.Duration {
	index := int(math.Ceil(float64(len(sorted))*p)) - 1
	return sorted[index]
}
