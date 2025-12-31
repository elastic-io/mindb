package main

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/elastic-io/mindb"
)

// SimpleLogger implements the mindb.Logger interface
type SimpleLogger struct{}

func (l *SimpleLogger) Info(args ...interface{}) {
	log.Println("[INFO]", fmt.Sprint(args...))
}

func (l *SimpleLogger) Infof(format string, args ...interface{}) {
	log.Printf("[INFO] "+format, args...)
}

func (l *SimpleLogger) Warn(args ...interface{}) {
	log.Println("[WARN]", fmt.Sprint(args...))
}

func (l *SimpleLogger) Warnf(format string, args ...interface{}) {
	log.Printf("[WARN] "+format, args...)
}

func (l *SimpleLogger) Error(args ...interface{}) {
	log.Println("[ERROR]", fmt.Sprint(args...))
}

func (l *SimpleLogger) Errorf(format string, args ...interface{}) {
	log.Printf("[ERROR] "+format, args...)
}

func (l *SimpleLogger) Debug(args ...interface{}) {
	log.Println("[DEBUG]", fmt.Sprint(args...))
}

func (l *SimpleLogger) Debugf(format string, args ...interface{}) {
	log.Printf("[DEBUG] "+format, args...)
}

func (l *SimpleLogger) Fatal(args ...interface{}) {
	log.Fatal("[FATAL]", fmt.Sprint(args...))
}

type BenchmarkResult struct {
	Operation   string
	Duration    time.Duration
	Operations  int
	BytesPerSec float64
	OpsPerSec   float64
	AvgLatency  time.Duration
	MinLatency  time.Duration
	MaxLatency  time.Duration
	Errors      int
}

type Benchmark struct {
	storage    *mindb.DB
	logger     *SimpleLogger
	bucketName string
}

func NewBenchmark(storagePath string) (*Benchmark, error) {
	logger := &SimpleLogger{}

	// Configure for performance
	config := mindb.DefaultConfig()
	config.MaxConcurrentUploads = runtime.NumCPU() * 8
	config.MaxConcurrentDownloads = runtime.NumCPU() * 16
	config.BufferSize = 1024 * 1024 // 1MB buffer
	config.UseMmap = true
	config.EnableChecksumVerify = false // Disable for performance testing

	storage, err := mindb.New(storagePath)
	if err != nil {
		return nil, err
	}

	storage.SetConfig(config)

	bucketName := "benchmark-bucket"
	if err := storage.CreateBucket(bucketName); err != nil {
		if fmt.Sprintf("%v", err) != "bucket already exists" {
			return nil, err
		}
	}

	return &Benchmark{
		storage:    storage,
		logger:     logger,
		bucketName: bucketName,
	}, nil
}

func (b *Benchmark) Close() error {
	return b.storage.Close()
}

// Generate random data
func generateData(size int) []byte {
	data := make([]byte, size)
	rand.Read(data)
	return data
}

// Sequential write benchmark
func (b *Benchmark) BenchmarkSequentialWrite(objectCount, objectSize int) *BenchmarkResult {
	b.logger.Infof("Starting sequential write benchmark: %d objects, %d bytes each", objectCount, objectSize)

	start := time.Now()
	var totalBytes int64
	var minLatency, maxLatency time.Duration = time.Hour, 0
	var errors int

	for i := 0; i < objectCount; i++ {
		data := generateData(objectSize)
		key := fmt.Sprintf("seq-write-%d", i)

		objStart := time.Now()
		objectData := &mindb.ObjectData{
			Key:         key,
			Data:        data,
			ContentType: "application/octet-stream",
		}

		if err := b.storage.PutObject(b.bucketName, objectData); err != nil {
			b.logger.Errorf("Failed to put object %s: %v", key, err)
			errors++
			continue
		}

		latency := time.Since(objStart)
		if latency < minLatency {
			minLatency = latency
		}
		if latency > maxLatency {
			maxLatency = latency
		}

		totalBytes += int64(objectSize)

		if (i+1)%100 == 0 {
			b.logger.Infof("Completed %d/%d writes", i+1, objectCount)
		}
	}

	duration := time.Since(start)

	return &BenchmarkResult{
		Operation:   "Sequential Write",
		Duration:    duration,
		Operations:  objectCount,
		BytesPerSec: float64(totalBytes) / duration.Seconds(),
		OpsPerSec:   float64(objectCount) / duration.Seconds(),
		AvgLatency:  duration / time.Duration(objectCount),
		MinLatency:  minLatency,
		MaxLatency:  maxLatency,
		Errors:      errors,
	}
}

// Concurrent write benchmark
func (b *Benchmark) BenchmarkConcurrentWrite(objectCount, objectSize, concurrency int) *BenchmarkResult {
	b.logger.Infof("Starting concurrent write benchmark: %d objects, %d bytes each, %d goroutines",
		objectCount, objectSize, concurrency)

	start := time.Now()
	var totalBytes int64
	var errors int
	var mu sync.Mutex
	var wg sync.WaitGroup

	latencies := make([]time.Duration, 0, objectCount)

	objectChan := make(chan int, objectCount)
	for i := 0; i < objectCount; i++ {
		objectChan <- i
	}
	close(objectChan)

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			for objID := range objectChan {
				data := generateData(objectSize)
				key := fmt.Sprintf("conc-write-%d-%d", workerID, objID)

				objStart := time.Now()
				objectData := &mindb.ObjectData{
					Key:         key,
					Data:        data,
					ContentType: "application/octet-stream",
				}

				if err := b.storage.PutObject(b.bucketName, objectData); err != nil {
					b.logger.Errorf("Worker %d failed to put object %s: %v", workerID, key, err)
					mu.Lock()
					errors++
					mu.Unlock()
					continue
				}

				latency := time.Since(objStart)

				mu.Lock()
				totalBytes += int64(objectSize)
				latencies = append(latencies, latency)
				mu.Unlock()
			}
		}(i)
	}

	wg.Wait()
	duration := time.Since(start)

	var minLatency, maxLatency time.Duration = time.Hour, 0
	for _, lat := range latencies {
		if lat < minLatency {
			minLatency = lat
		}
		if lat > maxLatency {
			maxLatency = lat
		}
	}

	return &BenchmarkResult{
		Operation:   fmt.Sprintf("Concurrent Write (%d workers)", concurrency),
		Duration:    duration,
		Operations:  objectCount,
		BytesPerSec: float64(totalBytes) / duration.Seconds(),
		OpsPerSec:   float64(objectCount) / duration.Seconds(),
		AvgLatency:  duration / time.Duration(objectCount),
		MinLatency:  minLatency,
		MaxLatency:  maxLatency,
		Errors:      errors,
	}
}

// Sequential read benchmark
func (b *Benchmark) BenchmarkSequentialRead(objectCount int) *BenchmarkResult {
	b.logger.Infof("Starting sequential read benchmark: %d objects", objectCount)

	start := time.Now()
	var totalBytes int64
	var minLatency, maxLatency time.Duration = time.Hour, 0
	var errors int

	for i := 0; i < objectCount; i++ {
		key := fmt.Sprintf("seq-write-%d", i)

		objStart := time.Now()
		objectData, err := b.storage.GetObject(b.bucketName, key)
		if err != nil {
			b.logger.Errorf("Failed to get object %s: %v", key, err)
			errors++
			continue
		}

		latency := time.Since(objStart)
		if latency < minLatency {
			minLatency = latency
		}
		if latency > maxLatency {
			maxLatency = latency
		}

		totalBytes += int64(len(objectData.Data))

		if (i+1)%100 == 0 {
			b.logger.Infof("Completed %d/%d reads", i+1, objectCount)
		}
	}

	duration := time.Since(start)

	return &BenchmarkResult{
		Operation:   "Sequential Read",
		Duration:    duration,
		Operations:  objectCount,
		BytesPerSec: float64(totalBytes) / duration.Seconds(),
		OpsPerSec:   float64(objectCount) / duration.Seconds(),
		AvgLatency:  duration / time.Duration(objectCount),
		MinLatency:  minLatency,
		MaxLatency:  maxLatency,
		Errors:      errors,
	}
}

// Concurrent read benchmark
func (b *Benchmark) BenchmarkConcurrentRead(objectCount, concurrency int) *BenchmarkResult {
	b.logger.Infof("Starting concurrent read benchmark: %d objects, %d goroutines", objectCount, concurrency)

	start := time.Now()
	var totalBytes int64
	var errors int
	var mu sync.Mutex
	var wg sync.WaitGroup

	latencies := make([]time.Duration, 0, objectCount)

	objectChan := make(chan int, objectCount)
	for i := 0; i < objectCount; i++ {
		objectChan <- i
	}
	close(objectChan)

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			for objID := range objectChan {
				key := fmt.Sprintf("seq-write-%d", objID)

				objStart := time.Now()
				objectData, err := b.storage.GetObject(b.bucketName, key)
				if err != nil {
					b.logger.Errorf("Worker %d failed to get object %s: %v", workerID, key, err)
					mu.Lock()
					errors++
					mu.Unlock()
					continue
				}

				latency := time.Since(objStart)

				mu.Lock()
				totalBytes += int64(len(objectData.Data))
				latencies = append(latencies, latency)
				mu.Unlock()
			}
		}(i)
	}

	wg.Wait()
	duration := time.Since(start)

	var minLatency, maxLatency time.Duration = time.Hour, 0
	for _, lat := range latencies {
		if lat < minLatency {
			minLatency = lat
		}
		if lat > maxLatency {
			maxLatency = lat
		}
	}

	return &BenchmarkResult{
		Operation:   fmt.Sprintf("Concurrent Read (%d workers)", concurrency),
		Duration:    duration,
		Operations:  objectCount,
		BytesPerSec: float64(totalBytes) / duration.Seconds(),
		OpsPerSec:   float64(objectCount) / duration.Seconds(),
		AvgLatency:  duration / time.Duration(objectCount),
		MinLatency:  minLatency,
		MaxLatency:  maxLatency,
		Errors:      errors,
	}
}

// Mixed workload benchmark
func (b *Benchmark) BenchmarkMixedWorkload(duration time.Duration, readRatio float64, objectSize int) *BenchmarkResult {
	b.logger.Infof("Starting mixed workload benchmark: %v duration, %.0f%% reads, %d byte objects",
		duration, readRatio*100, objectSize)

	start := time.Now()
	var totalOps, readOps, writeOps int
	var totalBytes int64
	var errors int
	var mu sync.Mutex

	// Pre-populate some objects for reading
	for i := 0; i < 1000; i++ {
		data := generateData(objectSize)
		key := fmt.Sprintf("mixed-%d", i)
		objectData := &mindb.ObjectData{
			Key:         key,
			Data:        data,
			ContentType: "application/octet-stream",
		}
		b.storage.PutObject(b.bucketName, objectData)
	}

	var wg sync.WaitGroup
	stopChan := make(chan struct{})

	// Start workers
	for i := 0; i < runtime.NumCPU(); i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			for {
				select {
				case <-stopChan:
					return
				default:
					if rand.Float64() < readRatio {
						// Read operation
						key := fmt.Sprintf("mixed-%d", rand.Intn(1000))
						if objectData, err := b.storage.GetObject(b.bucketName, key); err == nil {
							mu.Lock()
							readOps++
							totalOps++
							totalBytes += int64(len(objectData.Data))
							mu.Unlock()
						} else {
							mu.Lock()
							errors++
							mu.Unlock()
						}
					} else {
						// Write operation
						data := generateData(objectSize)
						key := fmt.Sprintf("mixed-new-%d-%d", workerID, time.Now().UnixNano())
						objectData := &mindb.ObjectData{
							Key:         key,
							Data:        data,
							ContentType: "application/octet-stream",
						}
						if err := b.storage.PutObject(b.bucketName, objectData); err == nil {
							mu.Lock()
							writeOps++
							totalOps++
							totalBytes += int64(objectSize)
							mu.Unlock()
						} else {
							mu.Lock()
							errors++
							mu.Unlock()
						}
					}
				}
			}
		}(i)
	}

	// Run for specified duration
	time.Sleep(duration)
	close(stopChan)
	wg.Wait()

	actualDuration := time.Since(start)

	return &BenchmarkResult{
		Operation:   fmt.Sprintf("Mixed Workload (%.0f%% reads)", readRatio*100),
		Duration:    actualDuration,
		Operations:  totalOps,
		BytesPerSec: float64(totalBytes) / actualDuration.Seconds(),
		OpsPerSec:   float64(totalOps) / actualDuration.Seconds(),
		AvgLatency:  actualDuration / time.Duration(totalOps),
		Errors:      errors,
	}
}

// Multipart upload benchmark
func (b *Benchmark) BenchmarkMultipartUpload(fileSize, partSize int) *BenchmarkResult {
	b.logger.Infof("Starting multipart upload benchmark: %d MB file, %d MB parts",
		fileSize/(1024*1024), partSize/(1024*1024))

	start := time.Now()
	key := fmt.Sprintf("multipart-test-%d", time.Now().UnixNano())

	// Create multipart upload
	uploadID, err := b.storage.CreateMultipartUpload(b.bucketName, key, "application/octet-stream", nil)
	if err != nil {
		return &BenchmarkResult{
			Operation: "Multipart Upload",
			Errors:    1,
		}
	}

	// Upload parts
	partCount := (fileSize + partSize - 1) / partSize
	var parts []mindb.MultipartPart
	var totalBytes int64

	for i := 0; i < partCount; i++ {
		currentPartSize := partSize
		if i == partCount-1 {
			currentPartSize = fileSize - i*partSize
		}

		data := generateData(currentPartSize)
		etag, err := b.storage.UploadPart(b.bucketName, key, uploadID, i+1, data)
		if err != nil {
			b.storage.AbortMultipartUpload(b.bucketName, key, uploadID)
			return &BenchmarkResult{
				Operation: "Multipart Upload",
				Errors:    1,
			}
		}

		parts = append(parts, mindb.MultipartPart{
			PartNumber: i + 1,
			ETag:       etag,
		})

		totalBytes += int64(currentPartSize)
		b.logger.Infof("Uploaded part %d/%d", i+1, partCount)
	}

	// Complete multipart upload
	finalETag, err := b.storage.CompleteMultipartUpload(b.bucketName, key, uploadID, parts)
	if err != nil {
		return &BenchmarkResult{
			Operation: "Multipart Upload",
			Errors:    1,
		}
	}

	duration := time.Since(start)
	b.logger.Infof("Multipart upload completed with ETag: %s", finalETag)

	return &BenchmarkResult{
		Operation:   "Multipart Upload",
		Duration:    duration,
		Operations:  1,
		BytesPerSec: float64(totalBytes) / duration.Seconds(),
		OpsPerSec:   1.0 / duration.Seconds(),
		AvgLatency:  duration,
		MinLatency:  duration,
		MaxLatency:  duration,
		Errors:      0,
	}
}

// Print benchmark results
func printResult(result *BenchmarkResult) {
	fmt.Printf("\n=== %s ===\n", result.Operation)
	fmt.Printf("Duration:        %v\n", result.Duration)
	fmt.Printf("Operations:      %d\n", result.Operations)
	fmt.Printf("Ops/sec:         %.2f\n", result.OpsPerSec)
	fmt.Printf("Throughput:      %.2f MB/s\n", result.BytesPerSec/(1024*1024))
	fmt.Printf("Avg Latency:     %v\n", result.AvgLatency)
	if result.MinLatency > 0 {
		fmt.Printf("Min Latency:     %v\n", result.MinLatency)
		fmt.Printf("Max Latency:     %v\n", result.MaxLatency)
	}
	if result.Errors > 0 {
		fmt.Printf("Errors:          %d\n", result.Errors)
	}
	fmt.Println()
}

func main() {
	// Parse command line arguments
	storagePath := "./performance-data"
	if len(os.Args) > 1 {
		storagePath = os.Args[1]
	}

	// Clean up previous test data
	os.RemoveAll(storagePath)

	// Create benchmark instance
	benchmark, err := NewBenchmark(storagePath)
	if err != nil {
		log.Fatal("Failed to create benchmark:", err)
	}
	defer benchmark.Close()

	fmt.Printf("Starting performance benchmarks...\n")
	fmt.Printf("Storage path: %s\n", storagePath)
	fmt.Printf("CPU cores: %d\n", runtime.NumCPU())
	fmt.Printf("GOMAXPROCS: %d\n", runtime.GOMAXPROCS(0))

	// Get initial metrics
	initialMetrics := benchmark.storage.GetMetrics()
	fmt.Printf("Initial metrics: %+v\n", initialMetrics)

	// Run benchmarks
	results := []*BenchmarkResult{}

	// Small objects
	fmt.Println("\n--- Small Objects (1KB) ---")
	results = append(results, benchmark.BenchmarkSequentialWrite(1000, 1024))
	results = append(results, benchmark.BenchmarkSequentialRead(1000))
	results = append(results, benchmark.BenchmarkConcurrentWrite(1000, 1024, runtime.NumCPU()))
	results = append(results, benchmark.BenchmarkConcurrentRead(1000, runtime.NumCPU()*2))

	// Medium objects
	fmt.Println("\n--- Medium Objects (1MB) ---")
	results = append(results, benchmark.BenchmarkSequentialWrite(100, 1024*1024))
	results = append(results, benchmark.BenchmarkSequentialRead(100))
	results = append(results, benchmark.BenchmarkConcurrentWrite(100, 1024*1024, runtime.NumCPU()))
	results = append(results, benchmark.BenchmarkConcurrentRead(100, runtime.NumCPU()*2))

	// Large objects
	fmt.Println("\n--- Large Objects (10MB) ---")
	results = append(results, benchmark.BenchmarkSequentialWrite(10, 10*1024*1024))
	results = append(results, benchmark.BenchmarkSequentialRead(10))

	// Mixed workload
	fmt.Println("\n--- Mixed Workload ---")
	results = append(results, benchmark.BenchmarkMixedWorkload(30*time.Second, 0.8, 64*1024))
	// Multipart upload
	fmt.Println("\n--- Multipart Upload ---")
	results = append(results, benchmark.BenchmarkMultipartUpload(100*1024*1024, 5*1024*1024)) // 100MB file, 5MB parts

	// Print all results
	fmt.Println("\n" + strings.Repeat("=", 80))
	fmt.Println("BENCHMARK RESULTS SUMMARY")
	fmt.Println(strings.Repeat("=", 80))

	for _, result := range results {
		printResult(result)
	}

	// Get final metrics
	finalMetrics := benchmark.storage.GetMetrics()
	fmt.Printf("Final metrics:\n")
	fmt.Printf("  Read Ops:       %d\n", finalMetrics.ReadOps)
	fmt.Printf("  Write Ops:      %d\n", finalMetrics.WriteOps)
	fmt.Printf("  Delete Ops:     %d\n", finalMetrics.DeleteOps)
	fmt.Printf("  List Ops:       %d\n", finalMetrics.ListOps)
	fmt.Printf("  Read Bytes:     %d (%.2f MB)\n", finalMetrics.ReadBytes, float64(finalMetrics.ReadBytes)/(1024*1024))
	fmt.Printf("  Write Bytes:    %d (%.2f MB)\n", finalMetrics.WriteBytes, float64(finalMetrics.WriteBytes)/(1024*1024))
	fmt.Printf("  Error Count:    %d\n", finalMetrics.ErrorCount)
	fmt.Printf("  Active Reads:   %d\n", finalMetrics.ActiveReads)
	fmt.Printf("  Active Writes:  %d\n", finalMetrics.ActiveWrites)

	// Storage statistics
	stats, err := benchmark.storage.GetStats()
	if err == nil {
		fmt.Printf("\nStorage statistics:\n")
		fmt.Printf("  Buckets:        %d\n", stats.BucketCount)
		fmt.Printf("  Objects:        %d\n", stats.ObjectCount)
		fmt.Printf("  Total Size:     %d bytes (%.2f MB)\n", stats.TotalSize, float64(stats.TotalSize)/(1024*1024))
	}

	// Disk usage
	total, free, used, err := benchmark.storage.GetDiskUsage()
	if err == nil {
		fmt.Printf("\nDisk usage:\n")
		fmt.Printf("  Total:          %.2f GB\n", float64(total)/(1024*1024*1024))
		fmt.Printf("  Used:           %.2f GB\n", float64(used)/(1024*1024*1024))
		fmt.Printf("  Free:           %.2f GB\n", float64(free)/(1024*1024*1024))
		fmt.Printf("  Usage:          %.2f%%\n", float64(used)/float64(total)*100)
	}

	fmt.Println("\nBenchmark completed!")
}
