 //go:build !windows
// +build !windows

package mindb

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestLongRunningStability(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping long running test in short mode")
	}

	s, tmpDir := setupTest(t)
	defer cleanupTestStorage(t, tmpDir)
	defer s.Close()

	testBucket := "stability-test"
	err := s.CreateBucket(testBucket)
	if err != nil {
		t.Fatal(err)
	}

	duration := 2 * time.Minute
	if testing.Short() {
		duration = 30 * time.Second
	}

	var (
		operations  int64
		errors      int64
		createdKeys []string
		keysMutex   sync.RWMutex
	)

	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	startTime := time.Now()

	progressDone := make(chan struct{})
	go func() {
		defer close(progressDone)
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				elapsed := time.Since(startTime)
				ops := atomic.LoadInt64(&operations)
				errs := atomic.LoadInt64(&errors)

				select {
				case <-ctx.Done():
					return
				default:
					t.Logf("Progress: %v elapsed, %d ops, %d errors",
						elapsed.Round(time.Second), ops, errs)

					if err := s.HealthCheck(); err != nil {
						t.Logf("Health check failed: %v", err)
					}
				}

			case <-ctx.Done():
				return
			}
		}
	}()

	for {
		select {
		case <-ctx.Done():
			goto cleanup
		default:
		}

		opNum := atomic.AddInt64(&operations, 1)

		data := generateTestData(rand.Intn(1*MB) + KB)
		key := fmt.Sprintf("stability-%d", opNum)

		obj := &ObjectData{
			Key:  key,
			Data: data,
		}

		if err := s.PutObject(testBucket, obj); err != nil {
			atomic.AddInt64(&errors, 1)
			t.Logf("Put error: %v", err)
			continue
		}

		keysMutex.Lock()
		createdKeys = append(createdKeys, key)
		keysMutex.Unlock()

		retrievedObj, err := s.GetObject(testBucket, key)
		if err != nil {
			atomic.AddInt64(&errors, 1)
			t.Logf("Get error: %v", err)
			continue
		}

		if len(retrievedObj.Data) != len(data) {
			atomic.AddInt64(&errors, 1)
			t.Logf("Data length mismatch: expected %d, got %d",
				len(data), len(retrievedObj.Data))
		}

		if opNum%20 == 0 {
			keysMutex.RLock()
			if len(createdKeys) > 50 {

				deleteKey := createdKeys[len(createdKeys)-50]
				keysMutex.RUnlock()

				if err := s.DeleteObject(testBucket, deleteKey); err != nil {
					if !isNotFoundError(err) {
						atomic.AddInt64(&errors, 1)
						t.Logf("Delete error: %v", err)
					}
				}
			} else {
				keysMutex.RUnlock()
			}
		}

		if opNum%100 == 0 {
			time.Sleep(50 * time.Millisecond)
		}
	}

cleanup:

	<-progressDone

	finalOps := atomic.LoadInt64(&operations)
	finalErrors := atomic.LoadInt64(&errors)
	actualDuration := time.Since(startTime)

	t.Logf("Stability test completed:")
	t.Logf("  Duration: %v", actualDuration)
	t.Logf("  Total operations: %d", finalOps)
	t.Logf("  Total errors: %d", finalErrors)
	t.Logf("  Error rate: %.2f%%", float64(finalErrors)/float64(finalOps)*100)
	t.Logf("  Average ops/sec: %.2f", float64(finalOps)/actualDuration.Seconds())

	if err := s.HealthCheck(); err != nil {
		t.Errorf("Final health check failed: %v", err)
	}
}

func TestStressTest(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping stress test in short mode")
	}

	s, tmpDir := setupTest(t)
	defer cleanupTestStorage(t, tmpDir)
	defer s.Close()

	testBucket := "stress-test"
	err := s.CreateBucket(testBucket)
	if err != nil {
		t.Fatal(err)
	}

	duration := 10 * time.Second
	numWorkers := 5
	objectSizes := []int{1 * KB, 64 * KB}

	var (
		totalOps       int64
		totalErrors    int64
		wg             sync.WaitGroup
		createdObjects sync.Map
	)

	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	startTime := time.Now()

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			localOps := 0
			localErrors := 0
			localCreated := make([]string, 0, 100)

			for {
				select {
				case <-ctx.Done():
					atomic.AddInt64(&totalOps, int64(localOps))
					atomic.AddInt64(&totalErrors, int64(localErrors))
					return
				default:
				}

				opType := rand.Intn(10)

				switch {
				case opType < 6:
					size := objectSizes[rand.Intn(len(objectSizes))]
					data := generateTestData(size)
					key := fmt.Sprintf("stress-%d-%d", workerID, localOps)

					obj := &ObjectData{
						Key:  key,
						Data: data,
					}

					if err := s.PutObject(testBucket, obj); err != nil {
						localErrors++
						t.Logf("Put error for %s: %v", key, err)
					} else {
						createdObjects.Store(key, true)
						localCreated = append(localCreated, key)
					}

				case opType < 8:
					if len(localCreated) > 0 {
						key := localCreated[rand.Intn(len(localCreated))]
						_, err := s.GetObject(testBucket, key)
						if err != nil && !isNotFoundError(err) {
							localErrors++
							t.Logf("Get error for %s: %v", key, err)
						}
					}

				case opType < 10:
					if len(localCreated) > 5 {
						idx := rand.Intn(len(localCreated))
						key := localCreated[idx]

						err := s.DeleteObject(testBucket, key)
						if err != nil && !isNotFoundError(err) {
							localErrors++
							t.Logf("Delete error for %s: %v", key, err)
						} else {
							localCreated = append(localCreated[:idx], localCreated[idx+1:]...)
							createdObjects.Delete(key)
						}
					}
				}

				localOps++

				if localOps%50 == 0 {
					time.Sleep(10 * time.Millisecond)
				}
			}
		}(i)
	}

	wg.Wait()

	actualDuration := time.Since(startTime)
	opsPerSecond := float64(totalOps) / actualDuration.Seconds()
	errorRate := float64(totalErrors) / float64(totalOps) * 100

	t.Logf("Stress test results:")
	t.Logf("  Duration: %v", actualDuration)
	t.Logf("  Total operations: %d", totalOps)
	t.Logf("  Operations/second: %.2f", opsPerSecond)
	t.Logf("  Total errors: %d", totalErrors)
	t.Logf("  Error rate: %.2f%%", errorRate)

	if errorRate > 10.0 {
		t.Errorf("Error rate too high: %.2f%%", errorRate)
	}

	metrics := s.GetMetrics()
	t.Logf("Storage metrics:")
	t.Logf("  Read ops: %d", metrics.ReadOps)
	t.Logf("  Write ops: %d", metrics.WriteOps)
	t.Logf("  Delete ops: %d", metrics.DeleteOps)
	t.Logf("  Error count: %d", metrics.ErrorCount)
	t.Logf("  Avg read latency: %v", time.Duration(metrics.AvgReadLatency))
	t.Logf("  Avg write latency: %v", time.Duration(metrics.AvgWriteLatency))
}

func isNotFoundError(err error) bool {
	if err == nil {
		return false
	}
	errStr := err.Error()
	return errStr == "object not found" ||
		errStr == "bucket not found" ||
		contains(errStr, "no such file or directory")
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) &&
		(s == substr ||
			(len(s) > len(substr) &&
				(s[:len(substr)] == substr ||
					s[len(s)-len(substr):] == substr ||
					indexOf(s, substr) >= 0)))
}

func indexOf(s, substr string) int {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return i
		}
	}
	return -1
}
