 //go:build !windows
// +build !windows

package mindb

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"os"
	"runtime"
	"sync"
	"testing"
)

func generateBenchmarkData(size int) []byte {
	data := make([]byte, size)
	rand.Read(data)
	return data
}

func BenchmarkPutObject(b *testing.B) {
	sizes := []int{
		1 * KB,   // 1KB
		64 * KB,  // 64KB
		1 * MB,   // 1MB
		10 * MB,  // 10MB
		100 * MB, // 100MB
	}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("Size_%s", formatBytes(size)), func(b *testing.B) {
			s, tmpDir := setupBanchTestStorage(b)
			defer cleanupBanchTestStorage(b, tmpDir)
			defer s.Close()

			testBucket := "benchmark-bucket"
			err := s.CreateBucket(testBucket)
			if err != nil {
				b.Fatal(err)
			}

			testData := generateBenchmarkData(size)

			b.ResetTimer()
			b.SetBytes(int64(size))

			for i := 0; i < b.N; i++ {
				obj := &ObjectData{
					Key:  fmt.Sprintf("test-object-%d", i),
					Data: testData,
				}

				if err := s.PutObject(testBucket, obj); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkGetObject(b *testing.B) {
	sizes := []int{
		1 * KB,
		64 * KB,
		1 * MB,
		10 * MB,
		100 * MB,
	}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("Size_%s", formatBytes(size)), func(b *testing.B) {
			s, tmpDir := setupBanchTestStorage(b)
			defer cleanupBanchTestStorage(b, tmpDir)
			defer s.Close()

			testBucket := "benchmark-bucket"
			err := s.CreateBucket(testBucket)
			if err != nil {
				b.Fatal(err)
			}

			testData := generateBenchmarkData(size)
			obj := &ObjectData{
				Key:  "benchmark-object",
				Data: testData,
			}
			err = s.PutObject(testBucket, obj)
			if err != nil {
				b.Fatal(err)
			}

			b.ResetTimer()
			b.SetBytes(int64(size))

			for i := 0; i < b.N; i++ {
				_, err := s.GetObject(testBucket, "benchmark-object")
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkMultipartUpload(b *testing.B) {
	s, tmpDir := setupBanchTestStorage(b)
	defer cleanupBanchTestStorage(b, tmpDir)
	defer s.Close()

	testBucket := "benchmark-bucket"
	err := s.CreateBucket(testBucket)
	if err != nil {
		b.Fatal(err)
	}

	partSize := 5 * MB
	numParts := 10
	totalSize := partSize * numParts

	b.ResetTimer()
	b.SetBytes(int64(totalSize))

	for i := 0; i < b.N; i++ {
		uploadID, err := s.CreateMultipartUpload(testBucket,
			fmt.Sprintf("multipart-test-%d", i), "application/octet-stream", nil)
		if err != nil {
			b.Fatal(err)
		}

		var parts []MultipartPart
		for j := 1; j <= numParts; j++ {
			partData := generateBenchmarkData(partSize)
			etag, err := s.UploadPart(testBucket, fmt.Sprintf("multipart-test-%d", i),
				uploadID, j, partData)
			if err != nil {
				b.Fatal(err)
			}

			parts = append(parts, MultipartPart{
				PartNumber: j,
				ETag:       etag,
			})
		}

		_, err = s.CompleteMultipartUpload(testBucket, fmt.Sprintf("multipart-test-%d", i),
			uploadID, parts)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkConcurrentPutObject(b *testing.B) {
	concurrencyLevels := []int{1, 2, 4, 8, 16, 32}
	objectSize := 1 * MB

	for _, concurrency := range concurrencyLevels {
		b.Run(fmt.Sprintf("Concurrency_%d", concurrency), func(b *testing.B) {
			s, tmpDir := setupBanchTestStorage(b)
			defer cleanupBanchTestStorage(b, tmpDir)
			defer s.Close()

			testBucket := "benchmark-bucket"
			err := s.CreateBucket(testBucket)
			if err != nil {
				b.Fatal(err)
			}

			testData := generateBenchmarkData(objectSize)

			b.ResetTimer()
			b.SetBytes(int64(objectSize))

			var wg sync.WaitGroup
			semaphore := make(chan struct{}, concurrency)

			for i := 0; i < b.N; i++ {
				wg.Add(1)
				go func(id int) {
					defer wg.Done()
					semaphore <- struct{}{}
					defer func() { <-semaphore }()

					obj := &ObjectData{
						Key:  fmt.Sprintf("concurrent-test-%d", id),
						Data: testData,
					}

					if err := s.PutObject(testBucket, obj); err != nil {
						b.Error(err)
					}
				}(i)
			}

			wg.Wait()
		})
	}
}

func BenchmarkConcurrentGetObject(b *testing.B) {
	concurrencyLevels := []int{1, 2, 4, 8, 16, 32}
	objectSize := 1 * MB

	for _, concurrency := range concurrencyLevels {
		b.Run(fmt.Sprintf("Concurrency_%d", concurrency), func(b *testing.B) {
			s, tmpDir := setupBanchTestStorage(b)
			defer cleanupBanchTestStorage(b, tmpDir)
			defer s.Close()

			testBucket := "benchmark-bucket"
			err := s.CreateBucket(testBucket)
			if err != nil {
				b.Fatal(err)
			}

			testData := generateBenchmarkData(objectSize)
			obj := &ObjectData{
				Key:  "benchmark-object",
				Data: testData,
			}
			err = s.PutObject(testBucket, obj)
			if err != nil {
				b.Fatal(err)
			}

			b.ResetTimer()
			b.SetBytes(int64(objectSize))

			var wg sync.WaitGroup
			semaphore := make(chan struct{}, concurrency)

			for i := 0; i < b.N; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					semaphore <- struct{}{}
					defer func() { <-semaphore }()

					_, err := s.GetObject(testBucket, "benchmark-object")
					if err != nil {
						b.Error(err)
					}
				}()
			}

			wg.Wait()
		})
	}
}

func BenchmarkPutObjectStream(b *testing.B) {
	sizes := []int{
		1 * MB,
		10 * MB,
		100 * MB,
	}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("Size_%s", formatBytes(size)), func(b *testing.B) {
			s, tmpDir := setupBanchTestStorage(b)
			defer cleanupBanchTestStorage(b, tmpDir)
			defer s.Close()

			testBucket := "benchmark-bucket"
			err := s.CreateBucket(testBucket)
			if err != nil {
				b.Fatal(err)
			}

			testData := generateBenchmarkData(size)
			b.ResetTimer()
			b.SetBytes(int64(size))

			for i := 0; i < b.N; i++ {
				reader := bytes.NewReader(testData)
				_, err := s.PutObjectStream(testBucket, fmt.Sprintf("stream-test-%d", i),
					reader, int64(size), "application/octet-stream", nil, nil)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkGetObjectStream(b *testing.B) {
	sizes := []int{
		1 * MB,
		10 * MB,
		100 * MB,
	}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("Size_%s", formatBytes(size)), func(b *testing.B) {
			s, tmpDir := setupBanchTestStorage(b)
			defer cleanupBanchTestStorage(b, tmpDir)
			defer s.Close()

			testBucket := "benchmark-bucket"
			err := s.CreateBucket(testBucket)
			if err != nil {
				b.Fatal(err)
			}

			testData := generateBenchmarkData(size)
			reader := bytes.NewReader(testData)
			_, err = s.PutObjectStream(testBucket, "stream-benchmark",
				reader, int64(size), "application/octet-stream", nil, nil)
			if err != nil {
				b.Fatal(err)
			}

			b.ResetTimer()
			b.SetBytes(int64(size))

			for i := 0; i < b.N; i++ {
				stream, _, err := s.GetObjectStream(testBucket, "stream-benchmark")
				if err != nil {
					b.Fatal(err)
				}

				buf := make([]byte, 64*KB)
				for {
					_, err := stream.Read(buf)
					if err != nil {
						if err.Error() == "EOF" {
							break
						}
						b.Fatal(err)
					}
				}
				stream.Close()
			}
		})
	}
}

func BenchmarkMemoryUsage(b *testing.B) {
	s, tmpDir := setupBanchTestStorage(b)
	defer cleanupBanchTestStorage(b, tmpDir)
	defer s.Close()

	testBucket := "memory-test"
	err := s.CreateBucket(testBucket)
	if err != nil {
		b.Fatal(err)
	}

	objectSize := 10 * MB
	testData := generateBenchmarkData(objectSize)

	var m1, m2 runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&m1)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		obj := &ObjectData{
			Key:  fmt.Sprintf("memory-test-%d", i),
			Data: testData,
		}

		if err := s.PutObject(testBucket, obj); err != nil {
			b.Fatal(err)
		}

		if i%100 == 0 {
			runtime.GC()
		}
	}

	runtime.GC()
	runtime.ReadMemStats(&m2)

	b.ReportMetric(float64(m2.Alloc-m1.Alloc)/float64(b.N), "bytes/op")
	b.ReportMetric(float64(m2.TotalAlloc-m1.TotalAlloc)/float64(b.N), "total-bytes/op")
}

func BenchmarkDiskIO(b *testing.B) {
	configs := []struct {
		name       string
		directIO   bool
		mmap       bool
		bufferSize int
	}{
		{"Default", false, false, 64 * KB},
		{"DirectIO", true, false, 64 * KB},
		{"Mmap", false, true, 64 * KB},
		{"LargeBuffer", false, false, 1 * MB},
		{"SmallBuffer", false, false, 4 * KB},
	}

	objectSize := 10 * MB

	for _, config := range configs {
		b.Run(config.name, func(b *testing.B) {
			s, tmpDir := setupBanchTestStorage(b)
			defer cleanupBanchTestStorage(b, tmpDir)
			defer s.Close()

			storageConfig := s.GetConfig()
			storageConfig.UseDirectIO = config.directIO
			storageConfig.UseMmap = config.mmap
			storageConfig.BufferSize = config.bufferSize
			s.SetConfig(storageConfig)

			testBucket := "io-test"
			err := s.CreateBucket(testBucket)
			if err != nil {
				b.Fatal(err)
			}

			testData := generateBenchmarkData(objectSize)

			b.ResetTimer()
			b.SetBytes(int64(objectSize))

			for i := 0; i < b.N; i++ {
				obj := &ObjectData{
					Key:  fmt.Sprintf("io-test-%d", i),
					Data: testData,
				}

				if err := s.PutObject(testBucket, obj); err != nil {
					b.Fatal(err)
				}

				_, err := s.GetObject(testBucket, fmt.Sprintf("io-test-%d", i))
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func formatBytes(bytes int) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%dB", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f%cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}

func setupBanchTestStorage(t testing.TB) (*DB, string) {
	tmpDir, err := os.MkdirTemp("", "filesystem_benchmark_*")
	if err != nil {
		t.Fatal(err)
	}

	s, err := New(tmpDir)
	if err != nil {
		t.Fatal(err)
	}

	return s, tmpDir
}

func cleanupBanchTestStorage(t testing.TB, tmpDir string) {
	os.RemoveAll(tmpDir)
}
