//go:build windows
// +build windows

package mindb

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"hash"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
	"unsafe"

	"golang.org/x/sys/windows"
)

// Windows-specific constants
const (
	FILE_FLAG_NO_BUFFERING    = 0x20000000
	FILE_FLAG_SEQUENTIAL_SCAN = 0x08000000

	// Standard constants from main db.go
	dirPerm    = 0755
	filePerm   = 0644
	maxRetries = 3
	retryDelay = 100 * time.Millisecond
	md5sum     = "md5"

	// Directory constants
	bucketsDir   = "buckets"
	multipartDir = ".db.sys/multipart"
	tmpDir       = ".db.sys/tmp"
	metadataDir  = ".db.sys/buckets"

	// File constants
	metadataExt  = ".json"
	partExt      = ".part"
	uploadConfig = "upload.json"

	// Cleanup constants
	MaxMultipartLifetime = 7 * 24 * time.Hour
	CleanupInterval      = 1 * time.Hour
)

// Windows-specific DB implementation with all core functionality
type Config struct {
	MaxConcurrentUploads   int
	MaxConcurrentDownloads int

	MetadataCacheSize int
	MetadataCacheTTL  time.Duration

	UseDirectIO bool
	UseMmap     bool
	BufferSize  int

	EnableChecksumVerify bool
	ChecksumAlgorithm    string

	CleanupInterval time.Duration
	TempFileMaxAge  time.Duration
}

type Metrics struct {
	ReadOps   int64
	WriteOps  int64
	DeleteOps int64
	ListOps   int64

	ReadBytes  int64
	WriteBytes int64

	ErrorCount int64

	AvgReadLatency  int64
	AvgWriteLatency int64

	ActiveReads  int64
	ActiveWrites int64
}

type DB struct {
	basePath string

	bucketLocks sync.Map
	uploadLocks sync.Map
	objectLocks sync.Map

	metrics *Metrics

	bufferPool sync.Pool
	hasherPool sync.Pool

	cleanupTicker *time.Ticker
	cleanupDone   chan struct{}
	ctx           context.Context
	cancel        context.CancelFunc

	config *Config

	log Logger
}

func DefaultConfig() *Config {
	return &Config{
		MaxConcurrentUploads:   runtime.NumCPU() * 4,
		MaxConcurrentDownloads: runtime.NumCPU() * 8,
		MetadataCacheSize:      10000,
		MetadataCacheTTL:       5 * time.Minute,
		UseDirectIO:            false,
		UseMmap:                true,
		BufferSize:             64 * KB, // 64KB
		EnableChecksumVerify:   true,
		ChecksumAlgorithm:      md5sum,
		CleanupInterval:        30 * time.Minute,
		TempFileMaxAge:         2 * time.Hour,
	}
}

func NewWithLogger(path string, log Logger) (*DB, error) {
	if log == nil {
		return nil, fmt.Errorf("logger is required")
	}

	if err := os.MkdirAll(path, dirPerm); err != nil {
		return nil, fmt.Errorf("failed to create base directory: %w", err)
	}

	systemDirs := []string{
		filepath.Join(path, bucketsDir),
		filepath.Join(path, multipartDir),
		filepath.Join(path, tmpDir),
		filepath.Join(path, metadataDir),
	}

	for _, dir := range systemDirs {
		if err := os.MkdirAll(dir, dirPerm); err != nil {
			return nil, fmt.Errorf("failed to create system directory %s: %w", dir, err)
		}
	}

	ctx, cancel := context.WithCancel(context.Background())

	s := &DB{
		basePath:    path,
		cleanupDone: make(chan struct{}),
		ctx:         ctx,
		cancel:      cancel,
		config:      DefaultConfig(),
		metrics:     &Metrics{},
		log:         log,
	}

	if s.config.BufferSize <= 0 {
		s.config.BufferSize = 64 * KB
	}

	s.bufferPool = sync.Pool{
		New: func() interface{} {
			return make([]byte, s.config.BufferSize)
		},
	}

	s.hasherPool = sync.Pool{
		New: func() interface{} {
			return md5.New()
		},
	}

	s.startOptimizedCleanupRoutine()

	return s, nil
}

func New(path string) (*DB, error) {
	return NewWithLogger(path, &nopLogger{})
}

// Windows-specific file operations with direct I/O support
func openWithDirectIO(filename string, perm os.FileMode) (*os.File, error) {
	access := uint32(syscall.GENERIC_READ | syscall.GENERIC_WRITE)
	shareMode := uint32(syscall.FILE_SHARE_READ | syscall.FILE_SHARE_WRITE)
	creationDisposition := uint32(syscall.CREATE_ALWAYS)
	flags := uint32(FILE_FLAG_NO_BUFFERING | FILE_FLAG_SEQUENTIAL_SCAN)

	filenamePtr, err := syscall.UTF16PtrFromString(filename)
	if err != nil {
		return nil, err
	}

	h, err := syscall.CreateFile(
		filenamePtr,
		access,
		shareMode,
		nil,
		creationDisposition,
		flags,
		0,
	)
	if err != nil {
		return nil, err
	}

	return os.NewFile(uintptr(h), filename), nil
}

// GetDiskUsage returns disk usage information using Windows API
func (db *DB) GetDiskUsage() (total, free, used uint64, err error) {
	var freeBytesAvailable, totalBytes, freeBytes uint64

	// Use kernel32.GetDiskFreeSpaceEx for Windows
	err = windows.GetDiskFreeSpaceEx(
		windows.StringToUTF16Ptr(db.basePath),
		(*uint64)(unsafe.Pointer(&freeBytesAvailable)),
		(*uint64)(unsafe.Pointer(&totalBytes)),
		(*uint64)(unsafe.Pointer(&freeBytes)),
	)

	if err != nil {
		return 0, 0, 0, fmt.Errorf("GetDiskFreeSpaceEx failed: %w", err)
	}

	return totalBytes, freeBytesAvailable, totalBytes - freeBytesAvailable, nil
}

// HealthCheck performs health check for Windows
func (db *DB) HealthCheck() error {
	if _, err := os.Stat(db.basePath); err != nil {
		return fmt.Errorf("base path not accessible: %w", err)
	}

	// Windows-specific disk space check using GetDiskFreeSpaceEx
	var freeBytesAvailable, totalBytes, freeBytes uint64
	err := windows.GetDiskFreeSpaceEx(
		windows.StringToUTF16Ptr(db.basePath),
		(*uint64)(unsafe.Pointer(&freeBytesAvailable)),
		(*uint64)(unsafe.Pointer(&totalBytes)),
		(*uint64)(unsafe.Pointer(&freeBytes)),
	)
	if err != nil {
		return fmt.Errorf("failed to get disk stats: %w", err)
	}

	usedSpace := totalBytes - freeBytes
	usagePercent := float64(usedSpace) / float64(totalBytes) * 100

	if usagePercent > 95 {
		return fmt.Errorf("disk usage too high: %.2f%%", usagePercent)
	}

	metrics := db.GetMetrics()
	totalOps := metrics.ReadOps + metrics.WriteOps + metrics.DeleteOps + metrics.ListOps
	if totalOps > 0 {
		errorRate := float64(metrics.ErrorCount) / float64(totalOps) * 100
		if errorRate > 5 {
			return fmt.Errorf("error rate too high: %.2f%%", errorRate)
		}
	}

	return nil
}

// Windows-specific optimized file reading without mmap
func (db *DB) readFileOptimized(path string) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	info, err := file.Stat()
	if err != nil {
		return nil, err
	}

	size := info.Size()
	if size == 0 {
		return nil, nil
	}

	data := make([]byte, size)
	buf := db.getBuffer()
	defer db.putBuffer(buf)

	if len(buf) == 0 {
		buf = make([]byte, 64*KB)
	}

	var offset int64
	for offset < size {
		n, err := file.ReadAt(buf, offset)
		if err != nil && err != io.EOF {
			return nil, err
		}

		copy(data[offset:], buf[:n])
		offset += int64(n)

		if err == io.EOF {
			break
		}
	}

	return data, nil
}

// Windows-specific mmap fallback (not supported on Windows in the same way)
func (db *DB) readFileMmap(path string) ([]byte, error) {
	// Windows doesn't support mmap in the same way as Unix, use optimized reading instead
	return db.readFileOptimized(path)
}

// GetObjectStream returns a stream for reading object data (Windows implementation)
func (db *DB) GetObjectStream(bucket, key string) (io.ReadCloser, *ObjectData, error) {
	defer func() {
		if r := recover(); r != nil {
			db.log.Error("Recovered from panic in GetObjectStream: ", r, " ", string(debug.Stack()))
		}
	}()

	atomic.AddInt64(&db.metrics.ReadOps, 1)

	objectPath := db.getObjectPath(bucket, key)
	metadataPath := db.getObjectMetadataPath(bucket, key)

	objLock := db.getObjectLock(bucket, key)
	objLock.RLock()
	defer objLock.RUnlock()

	file, err := os.Open(objectPath)
	if err != nil {
		if os.IsNotExist(err) {
			atomic.AddInt64(&db.metrics.ErrorCount, 1)
			return nil, nil, fmt.Errorf("object not found")
		}
		atomic.AddInt64(&db.metrics.ErrorCount, 1)
		return nil, nil, fmt.Errorf("failed to open object: %w", err)
	}

	objectData := &ObjectData{}
	if metaData, err := db.readFileOptimized(metadataPath); err == nil {
		if err := objectData.UnmarshalJSON(metaData); err != nil {
			file.Close()
			atomic.AddInt64(&db.metrics.ErrorCount, 1)
			return nil, nil, fmt.Errorf("failed to parse metadata: %w", err)
		}
	} else {
		info, err := file.Stat()
		if err != nil {
			file.Close()
			atomic.AddInt64(&db.metrics.ErrorCount, 1)
			return nil, nil, fmt.Errorf("failed to get file info: %w", err)
		}

		objectData = &ObjectData{
			Key:          key,
			ContentType:  "application/octet-stream",
			LastModified: info.ModTime(),
			ETag:         "",
			Size:         info.Size(),
		}
	}

	return file, objectData, nil
}

// PutObjectStream writes object data from a stream (Windows implementation)
func (db *DB) PutObjectStream(bucket, key string, reader io.Reader, size int64, contentType string, metadata map[string]string, progressWriter io.Writer) (string, error) {
	start := time.Now()
	atomic.AddInt64(&db.metrics.ActiveWrites, 1)
	defer func() {
		atomic.AddInt64(&db.metrics.ActiveWrites, -1)
		atomic.AddInt64(&db.metrics.WriteOps, 1)
		latency := time.Since(start).Nanoseconds()
		atomic.StoreInt64(&db.metrics.AvgWriteLatency, latency)

		if r := recover(); r != nil {
			db.log.Error("Recovered from panic in PutObjectStream: ", r, " ", string(debug.Stack()))
		}
	}()

	objectPath := db.getObjectPath(bucket, key)
	metadataPath := db.getObjectMetadataPath(bucket, key)

	objLock := db.getObjectLock(bucket, key)
	objLock.Lock()
	defer objLock.Unlock()

	if err := os.MkdirAll(filepath.Dir(objectPath), dirPerm); err != nil {
		atomic.AddInt64(&db.metrics.ErrorCount, 1)
		return "", fmt.Errorf("failed to create object directory: %w", err)
	}

	tmpFile := filepath.Join(db.basePath, tmpDir, fmt.Sprintf("stream_%d_%d_%s",
		time.Now().UnixNano(), os.Getpid(), filepath.Base(objectPath)))

	var file *os.File
	var err error

	if db.config.UseDirectIO && size > MB {
		file, err = openWithDirectIO(tmpFile, filePerm)
	} else {
		file, err = os.Create(tmpFile)
	}

	if err != nil {
		atomic.AddInt64(&db.metrics.ErrorCount, 1)
		return "", fmt.Errorf("failed to create temp file: %w", err)
	}

	defer func() {
		file.Close()
		if err != nil {
			os.Remove(tmpFile)
		}
	}()

	buf := db.getBuffer()
	defer db.putBuffer(buf)

	if len(buf) == 0 {
		buf = make([]byte, 64*KB)
	}

	hasher := db.getHasher()
	defer db.putHasher(hasher)

	var multiWriter io.Writer
	if progressWriter != nil {
		multiWriter = io.MultiWriter(file, hasher, progressWriter)
	} else {
		multiWriter = io.MultiWriter(file, hasher)
	}

	var written int64
	if size > 0 {
		written, err = io.CopyBuffer(multiWriter, io.LimitReader(reader, size), buf)
	} else {
		written, err = io.CopyBuffer(multiWriter, reader, buf)
	}

	if err != nil {
		atomic.AddInt64(&db.metrics.ErrorCount, 1)
		return "", fmt.Errorf("failed to write object data: %w", err)
	}

	if err = file.Sync(); err != nil {
		atomic.AddInt64(&db.metrics.ErrorCount, 1)
		return "", fmt.Errorf("failed to sync file: %w", err)
	}

	file.Close()

	etag := hex.EncodeToString(hasher.Sum(nil))

	if err = db.verifyDataIntegrity(tmpFile, etag); err != nil {
		atomic.AddInt64(&db.metrics.ErrorCount, 1)
		return "", fmt.Errorf("data integrity check failed: %w", err)
	}

	if err = os.Rename(tmpFile, objectPath); err != nil {
		atomic.AddInt64(&db.metrics.ErrorCount, 1)
		return "", fmt.Errorf("failed to move temp file: %w", err)
	}

	objectData := &ObjectData{
		Key:          key,
		ContentType:  contentType,
		LastModified: time.Now(),
		ETag:         etag,
		Metadata:     metadata,
		Size:         written,
	}

	metaData, err := objectData.MarshalJSON()
	if err != nil {
		atomic.AddInt64(&db.metrics.ErrorCount, 1)
		return "", fmt.Errorf("failed to marshal metadata: %w", err)
	}

	if err = db.atomicWriteWithVerify(metadataPath, metaData, CalculateETag(metaData)); err != nil {
		os.Remove(objectPath)
		atomic.AddInt64(&db.metrics.ErrorCount, 1)
		return "", fmt.Errorf("failed to write metadata: %w", err)
	}

	atomic.AddInt64(&db.metrics.WriteBytes, written)
	return etag, nil
}

// Windows-specific atomic write with verification
func (db *DB) atomicWriteWithVerify(filePath string, data []byte, expectedChecksum string) error {
	dir := filepath.Dir(filePath)
	if err := os.MkdirAll(dir, dirPerm); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	tmpFile := filepath.Join(db.basePath, tmpDir, fmt.Sprintf("tmp_%d_%d_%s",
		time.Now().UnixNano(), os.Getpid(), filepath.Base(filePath)))

	var file *os.File
	var err error

	if db.config.UseDirectIO {
		file, err = openWithDirectIO(tmpFile, filePerm)
	} else {
		file, err = os.Create(tmpFile)
	}

	if err != nil {
		return fmt.Errorf("failed to create temp file: %w", err)
	}

	defer func() {
		file.Close()
		if err != nil {
			os.Remove(tmpFile)
		}
	}()

	if _, err = file.Write(data); err != nil {
		return fmt.Errorf("failed to write temp file: %w", err)
	}

	if err = file.Sync(); err != nil {
		return fmt.Errorf("failed to sync temp file: %w", err)
	}

	file.Close()

	if err = db.verifyDataIntegrity(tmpFile, expectedChecksum); err != nil {
		return fmt.Errorf("data integrity check failed: %w", err)
	}

	if err = os.Rename(tmpFile, filePath); err != nil {
		return fmt.Errorf("failed to move temp file: %w", err)
	}

	return nil
}

// Windows-specific cleanup routine
func (db *DB) startOptimizedCleanupRoutine() {
	db.cleanupTicker = time.NewTicker(db.config.CleanupInterval)

	go func() {
		defer func() {
			if r := recover(); r != nil {
				db.log.Error("Recovered from panic in cleanup routine: ", r, " ", string(debug.Stack()))

				time.Sleep(time.Minute)
				db.startOptimizedCleanupRoutine()
			}
		}()

		for {
			select {
			case <-db.cleanupTicker.C:

				var wg sync.WaitGroup

				wg.Add(1)
				go func() {
					defer wg.Done()
					if err := db.cleanupExpiredUploads(); err != nil {
						db.log.Error("Failed to cleanup expired multipart uploads: ", err)
					}
				}()

				wg.Add(1)
				go func() {
					defer wg.Done()
					if err := db.cleanupTempFiles(); err != nil {
						db.log.Error("Failed to cleanup temp files: ", err)
					}
				}()

				wg.Add(1)
				go func() {
					defer wg.Done()
					if err := db.cleanupEmptyDirectories(); err != nil {
						db.log.Error("Failed to cleanup empty directories: ", err)
					}
				}()

				wg.Add(1)
				go func() {
					defer wg.Done()
					db.cleanupLockMaps()
				}()

				wg.Wait()

			case <-db.ctx.Done():
				return
			}
		}
	}()
}

// Windows-specific cleanup functions
func (db *DB) cleanupExpiredUploads() error {
	defer func() {
		if r := recover(); r != nil {
			db.log.Error("Recovered from panic in cleanupExpiredUploads: ", r, " ", string(debug.Stack()))
		}
	}()

	multipartPath := filepath.Join(db.basePath, multipartDir)
	now := time.Now()

	return filepath.WalkDir(multipartPath, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return nil
		}

		if !d.IsDir() || path == multipartPath {
			return nil
		}

		infoFile := filepath.Join(path, uploadConfig)
		if _, err := os.Stat(infoFile); os.IsNotExist(err) {
			return nil
		}

		data, err := db.readFileOptimized(infoFile)
		if err != nil {
			return nil
		}

		uploadInfo := &MultipartUploadInfo{}
		if err := uploadInfo.UnmarshalJSON(data); err != nil {
			return nil
		}

		if now.Sub(uploadInfo.CreatedAt) > MaxMultipartLifetime {
			db.log.Info("Cleaning up expired multipart upload: ", uploadInfo.UploadID)

			func() {
				defer func() {
					if r := recover(); r != nil {
						db.log.Error("Recovered from panic while cleaning upload ", uploadInfo.UploadID, ": ", r, " ", string(debug.Stack()))
					}
				}()

				if err := os.RemoveAll(path); err != nil {
					db.log.Warn("Failed to remove expired upload directory: ", err)
				}
			}()
		}

		return nil
	})
}

func (db *DB) cleanupTempFiles() error {
	tmpPath := filepath.Join(db.basePath, tmpDir)
	now := time.Now()

	return filepath.WalkDir(tmpPath, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}

		if info, err := d.Info(); err == nil {
			if now.Sub(info.ModTime()) > db.config.TempFileMaxAge {
				os.Remove(path)
			}
		}

		return nil
	})
}

func (db *DB) cleanupEmptyDirectories() error {
	bucketsPath := filepath.Join(db.basePath, bucketsDir)

	return filepath.WalkDir(bucketsPath, func(path string, d fs.DirEntry, err error) error {
		if err != nil || !d.IsDir() || path == bucketsPath {
			return nil
		}

		entries, err := os.ReadDir(path)
		if err != nil {
			return nil
		}

		if len(entries) == 0 {

			if filepath.Dir(path) != bucketsPath {
				os.Remove(path)
			}
		}

		return nil
	})
}

func (db *DB) cleanupLockMaps() {
	db.bucketLocks.Range(func(key, value interface{}) bool {
		return true
	})
}

// Windows-specific utility functions
func (db *DB) getBucketLock(bucket string) *sync.RWMutex {
	lock, _ := db.bucketLocks.LoadOrStore(bucket, &sync.RWMutex{})
	return lock.(*sync.RWMutex)
}

func (db *DB) getUploadLock(uploadID string) *sync.RWMutex {
	lock, _ := db.uploadLocks.LoadOrStore(uploadID, &sync.RWMutex{})
	return lock.(*sync.RWMutex)
}

func (db *DB) getObjectLock(bucket, key string) *sync.RWMutex {
	objectKey := bucket + "/" + key
	lock, _ := db.objectLocks.LoadOrStore(objectKey, &sync.RWMutex{})
	return lock.(*sync.RWMutex)
}

func (db *DB) getBuffer() []byte {
	if buf := db.bufferPool.Get(); buf != nil {
		buffer := buf.([]byte)

		if cap(buffer) >= db.config.BufferSize && db.config.BufferSize > 0 {
			return buffer[:db.config.BufferSize]
		}
	}

	if db.config.BufferSize <= 0 {
		db.config.BufferSize = 64 * KB
	}
	return make([]byte, db.config.BufferSize)
}

func (db *DB) putBuffer(buf []byte) {
	if buf != nil && cap(buf) >= db.config.BufferSize && db.config.BufferSize > 0 {

		clear(buf)
		db.bufferPool.Put(buf[:0])
	}
}

func (db *DB) getHasher() hash.Hash {
	if hasher := db.hasherPool.Get(); hasher != nil {
		h := hasher.(hash.Hash)
		h.Reset()
		return h
	}
	return md5.New()
}

func (db *DB) putHasher(hasher hash.Hash) {
	db.hasherPool.Put(hasher)
}

// Windows-specific path functions
func (db *DB) getObjectPath(bucket, key string) string {
	hash := md5.Sum([]byte(key))
	hashStr := hex.EncodeToString(hash[:])

	return filepath.Join(db.basePath, bucketsDir, bucket,
		hashStr[:2], hashStr[2:4], key)
}

func (db *DB) getObjectMetadataPath(bucket, key string) string {
	return db.getObjectPath(bucket, key) + metadataExt
}

func (db *DB) getMultipartPath(uploadID string) string {
	return filepath.Join(db.basePath, multipartDir, uploadID)
}

// Windows-specific atomic operations
func (db *DB) atomicWrite(filePath string, data []byte) error {
	checksum := CalculateETag(data)
	return db.atomicWriteWithVerify(filePath, data, checksum)
}

func (db *DB) atomicWriteWithoutVerify(filePath string, data []byte) error {
	dir := filepath.Dir(filePath)
	if err := os.MkdirAll(dir, dirPerm); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	tmpFile := filepath.Join(db.basePath, tmpDir, fmt.Sprintf("tmp_%d_%d_%s",
		time.Now().UnixNano(), os.Getpid(), filepath.Base(filePath)))

	var file *os.File
	var err error

	if db.config.UseDirectIO {
		file, err = openWithDirectIO(tmpFile, filePerm)
	} else {
		file, err = os.Create(tmpFile)
	}

	if err != nil {
		return fmt.Errorf("failed to create temp file: %w", err)
	}

	defer func() {
		file.Close()
		if err != nil {
			os.Remove(tmpFile)
		}
	}()

	if _, err = file.Write(data); err != nil {
		return fmt.Errorf("failed to write temp file: %w", err)
	}

	if err = file.Sync(); err != nil {
		return fmt.Errorf("failed to sync temp file: %w", err)
	}

	file.Close()

	if err = os.Rename(tmpFile, filePath); err != nil {
		return fmt.Errorf("failed to move temp file: %w", err)
	}

	return nil
}

// Windows-specific data integrity verification
func (db *DB) verifyDataIntegrity(path string, expectedChecksum string) error {
	if !db.config.EnableChecksumVerify || expectedChecksum == "" {
		return nil
	}

	expectedChecksum = strings.TrimSpace(strings.Trim(expectedChecksum, `"`))

	if strings.Contains(expectedChecksum, "-") {
		parts := strings.Split(expectedChecksum, "-")
		if len(parts) == 2 {
			if partCount, err := strconv.Atoi(parts[1]); err == nil && partCount > 1 {

				db.log.Debug("Skipping integrity check for multipart ETag: ", expectedChecksum)
				return nil
			}
		}
	}

	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("failed to open file for verification: %w", err)
	}
	defer file.Close()

	hasher := db.getHasher()
	defer db.putHasher(hasher)

	buf := db.getBuffer()
	defer db.putBuffer(buf)

	if len(buf) == 0 {
		return fmt.Errorf("buffer is empty, cannot proceed with verification")
	}

	if _, err := io.CopyBuffer(hasher, file, buf); err != nil {
		return fmt.Errorf("failed to calculate checksum: %w", err)
	}

	actualChecksum := hex.EncodeToString(hasher.Sum(nil))

	db.log.Debug("Checksum verification - Expected: '", expectedChecksum, "', Actual: '", actualChecksum, "'")

	if actualChecksum != expectedChecksum {
		return fmt.Errorf("data corruption detected: expected '%s', got '%s'",
			expectedChecksum, actualChecksum)
	}

	return nil
}

// Windows-specific retry mechanism
func (db *DB) withRetry(operation func() error) error {
	var lastErr error
	for i := 0; i < maxRetries; i++ {
		if err := operation(); err != nil {
			lastErr = err
			db.log.Warn("Operation failed (attempt ", i+1, "/", maxRetries, "): ", err)
			if i < maxRetries-1 {
				time.Sleep(retryDelay * time.Duration(i+1))
			}
			continue
		}
		return nil
	}
	return fmt.Errorf("operation failed after %d retries: %w", maxRetries, lastErr)
}

// Windows-specific cleanup empty directory path
func (db *DB) cleanupEmptyDirPath(dirPath string) {
	for i := 0; i < 2; i++ {
		if entries, err := os.ReadDir(dirPath); err == nil && len(entries) == 0 {
			if err := os.Remove(dirPath); err != nil {
				break
			}
			dirPath = filepath.Dir(dirPath)
		} else {
			break
		}
	}
}

// Close cleans up Windows-specific resources
func (db *DB) Close() error {
	defer func() {
		if r := recover(); r != nil {
			db.log.Error("Recovered from panic in Close: ", r, " ", string(debug.Stack()))
		}
	}()

	db.cancel()

	if db.cleanupTicker != nil {
		db.cleanupTicker.Stop()
		close(db.cleanupDone)
	}

	return nil
}

// Windows-specific GetConfig
func (db *DB) GetConfig() *Config {
	return db.config
}

// Windows-specific SetConfig
func (db *DB) SetConfig(config *Config) {
	if config == nil {
		return
	}

	db.config = config

	db.bufferPool = sync.Pool{
		New: func() interface{} {
			return make([]byte, db.config.BufferSize)
		},
	}
}

// Windows-specific GetMetrics
func (db *DB) GetMetrics() *Metrics {
	return &Metrics{
		ReadOps:         atomic.LoadInt64(&db.metrics.ReadOps),
		WriteOps:        atomic.LoadInt64(&db.metrics.WriteOps),
		DeleteOps:       atomic.LoadInt64(&db.metrics.DeleteOps),
		ListOps:         atomic.LoadInt64(&db.metrics.ListOps),
		ReadBytes:       atomic.LoadInt64(&db.metrics.ReadBytes),
		WriteBytes:      atomic.LoadInt64(&db.metrics.WriteBytes),
		ErrorCount:      atomic.LoadInt64(&db.metrics.ErrorCount),
		AvgReadLatency:  atomic.LoadInt64(&db.metrics.AvgReadLatency),
		AvgWriteLatency: atomic.LoadInt64(&db.metrics.AvgWriteLatency),
		ActiveReads:     atomic.LoadInt64(&db.metrics.ActiveReads),
		ActiveWrites:    atomic.LoadInt64(&db.metrics.ActiveWrites),
	}
}

// Windows-specific ResetMetrics
func (db *DB) ResetMetrics() {
	atomic.StoreInt64(&db.metrics.ReadOps, 0)
	atomic.StoreInt64(&db.metrics.WriteOps, 0)
	atomic.StoreInt64(&db.metrics.DeleteOps, 0)
	atomic.StoreInt64(&db.metrics.ListOps, 0)
	atomic.StoreInt64(&db.metrics.ReadBytes, 0)
	atomic.StoreInt64(&db.metrics.WriteBytes, 0)
	atomic.StoreInt64(&db.metrics.ErrorCount, 0)
}

type Logger interface {
	// Debug logs the provided arguments at [DebugLevel].
	// Spaces are added between arguments when neither is a string.
	Debug(args ...interface{})
	// Info logs the provided arguments at [InfoLevel].
	// Spaces are added between arguments when neither is a string.
	Info(args ...interface{})

	// Warn logs the provided arguments at [WarnLevel].
	// Spaces are added between arguments when neither is a string.
	Warn(args ...interface{})

	// Error logs the provided arguments at [ErrorLevel].
	// Spaces are added between arguments when neither is a string.
	Error(args ...interface{})

	// Fatal constructs a message with the provided arguments and calls os.Exit.
	// Spaces are added between arguments when neither is a string.
	Fatal(args ...interface{})

	// Debugf formats the message according to the format specifier
	// and logs it at [DebugLevel].
	Debugf(template string, args ...interface{})

	// Infof formats the message according to the format specifier
	// and logs it at [InfoLevel].
	Infof(template string, args ...interface{})

	// Warnf formats the message according to the format specifier
	// and logs it at [WarnLevel].
	Warnf(template string, args ...interface{})

	// Errorf formats the message according to the format specifier
	// and logs it at [ErrorLevel].
	Errorf(template string, args ...interface{})
}

// Windows-specific Logger interface implementation
type nopLogger struct{}

func (l *nopLogger) Debug(args ...interface{})                   {}
func (l *nopLogger) Info(args ...interface{})                    {}
func (l *nopLogger) Warn(args ...interface{})                    {}
func (l *nopLogger) Error(args ...interface{})                   {}
func (l *nopLogger) Fatal(args ...interface{})                   {}
func (l *nopLogger) Debugf(template string, args ...interface{}) {}
func (l *nopLogger) Infof(template string, args ...interface{})  {}
func (l *nopLogger) Warnf(template string, args ...interface{})  {}
func (l *nopLogger) Errorf(template string, args ...interface{}) {}

// Windows-specific ObjectExists method
func (db *DB) ObjectExists(bucket, key string) (bool, error) {
	start := time.Now()
	atomic.AddInt64(&db.metrics.ActiveReads, 1)
	defer func() {
		atomic.AddInt64(&db.metrics.ActiveReads, -1)
		latency := time.Since(start).Nanoseconds()

		atomic.StoreInt64(&db.metrics.AvgReadLatency, latency)

		if r := recover(); r != nil {
			db.log.Error("Recovered from panic in ObjectExists: ", r, " ", string(debug.Stack()))
		}
	}()

	exists, err := db.BucketExists(bucket)
	if err != nil {
		return false, fmt.Errorf("failed to check bucket existence: %w", err)
	}
	if !exists {
		return false, nil
	}

	objectPath := db.getObjectPath(bucket, key)

	objLock := db.getObjectLock(bucket, key)
	objLock.RLock()
	defer objLock.RUnlock()

	_, err = os.Stat(objectPath)
	if os.IsNotExist(err) {
		return false, nil
	}
	if err != nil {
		atomic.AddInt64(&db.metrics.ErrorCount, 1)
		return false, fmt.Errorf("failed to check object existence: %w", err)
	}

	return true, nil
}

// Windows-specific DeleteObject method
func (db *DB) DeleteObject(bucket, key string) error {
	defer func() {
		if r := recover(); r != nil {
			db.log.Error("Recovered from panic in DeleteObject: ", r, " ", string(debug.Stack()))
		}
	}()

	atomic.AddInt64(&db.metrics.DeleteOps, 1)

	objectPath := db.getObjectPath(bucket, key)
	metadataPath := db.getObjectMetadataPath(bucket, key)

	objLock := db.getObjectLock(bucket, key)
	objLock.Lock()
	defer objLock.Unlock()

	if _, err := os.Stat(objectPath); os.IsNotExist(err) {
		atomic.AddInt64(&db.metrics.ErrorCount, 1)
		return fmt.Errorf("object not found")
	}

	if err := os.Remove(objectPath); err != nil {
		atomic.AddInt64(&db.metrics.ErrorCount, 1)
		return fmt.Errorf("failed to delete object: %w", err)
	}

	os.Remove(metadataPath)

	dir := filepath.Dir(objectPath)
	for i := 0; i < 2; i++ {
		if entries, err := os.ReadDir(dir); err == nil && len(entries) == 0 {
			os.Remove(dir)
			dir = filepath.Dir(dir)
		} else {
			break
		}
	}

	return nil
}

// Windows-specific RenameObject method
func (db *DB) RenameObject(bucket, oldKey, newKey string) error {
	start := time.Now()
	atomic.AddInt64(&db.metrics.ActiveWrites, 1)
	defer func() {
		atomic.AddInt64(&db.metrics.ActiveWrites, -1)
		atomic.AddInt64(&db.metrics.WriteOps, 1)
		latency := time.Since(start).Nanoseconds()
		atomic.StoreInt64(&db.metrics.AvgWriteLatency, latency)

		if r := recover(); r != nil {
			db.log.Error("Recovered from panic in RenameObject: ", r, " ", string(debug.Stack()))
		}
	}()

	if bucket == "" || oldKey == "" || newKey == "" {
		atomic.AddInt64(&db.metrics.ErrorCount, 1)
		return fmt.Errorf("bucket, oldKey and newKey cannot be empty")
	}

	if oldKey == newKey {
		return nil
	}

	exists, err := db.BucketExists(bucket)
	if err != nil {
		atomic.AddInt64(&db.metrics.ErrorCount, 1)
		return fmt.Errorf("failed to check bucket existence: %w", err)
	}
	if !exists {
		atomic.AddInt64(&db.metrics.ErrorCount, 1)
		return fmt.Errorf("bucket %s does not exist", bucket)
	}

	oldObjectPath := db.getObjectPath(bucket, oldKey)
	oldMetadataPath := db.getObjectMetadataPath(bucket, oldKey)
	newObjectPath := db.getObjectPath(bucket, newKey)
	newMetadataPath := db.getObjectMetadataPath(bucket, newKey)

	var firstLock, secondLock *sync.RWMutex
	var firstKey, secondKey string

	if oldKey < newKey {
		firstLock = db.getObjectLock(bucket, oldKey)
		secondLock = db.getObjectLock(bucket, newKey)
		firstKey, secondKey = oldKey, newKey
	} else {
		firstLock = db.getObjectLock(bucket, newKey)
		secondLock = db.getObjectLock(bucket, oldKey)
		firstKey, secondKey = newKey, oldKey
	}

	firstLock.Lock()
	defer firstLock.Unlock()

	if firstKey != secondKey {
		secondLock.Lock()
		defer secondLock.Unlock()
	}

	if _, err := os.Stat(oldObjectPath); os.IsNotExist(err) {
		atomic.AddInt64(&db.metrics.ErrorCount, 1)
		return fmt.Errorf("source object %s does not exist", oldKey)
	}

	if _, err := os.Stat(newObjectPath); err == nil {
		atomic.AddInt64(&db.metrics.ErrorCount, 1)
		return fmt.Errorf("destination object %s already exists", newKey)
	}

	if err := os.MkdirAll(filepath.Dir(newObjectPath), dirPerm); err != nil {
		atomic.AddInt64(&db.metrics.ErrorCount, 1)
		return fmt.Errorf("failed to create destination directory: %w", err)
	}

	if err := os.Rename(oldObjectPath, newObjectPath); err != nil {
		atomic.AddInt64(&db.metrics.ErrorCount, 1)
		return fmt.Errorf("failed to rename object file: %w", err)
	}

	if _, err := os.Stat(oldMetadataPath); err == nil {
		if err := os.MkdirAll(filepath.Dir(newMetadataPath), dirPerm); err != nil {
			os.Rename(newObjectPath, oldObjectPath)
			atomic.AddInt64(&db.metrics.ErrorCount, 1)
			return fmt.Errorf("failed to create new metadata directory: %w", err)
		}

		metaData, err := db.readFileOptimized(oldMetadataPath)
		if err != nil {
			os.Rename(newObjectPath, oldObjectPath)
			atomic.AddInt64(&db.metrics.ErrorCount, 1)
			return fmt.Errorf("failed to read old metadata: %w", err)
		}

		objectData := &ObjectData{}
		if err := objectData.UnmarshalJSON(metaData); err != nil {
			os.Rename(newObjectPath, oldObjectPath)
			atomic.AddInt64(&db.metrics.ErrorCount, 1)
			return fmt.Errorf("failed to parse metadata: %w", err)
		}

		objectData.Key = newKey
		objectData.LastModified = time.Now()

		newMetaData, err := objectData.MarshalJSON()
		if err != nil {
			os.Rename(newObjectPath, oldObjectPath)
			atomic.AddInt64(&db.metrics.ErrorCount, 1)
			return fmt.Errorf("failed to marshal new metadata: %w", err)
		}

		err = db.withRetry(func() error {
			return db.atomicWriteWithVerify(newMetadataPath, newMetaData, CalculateETag(newMetaData))
		})

		if err != nil {
			os.Rename(newObjectPath, oldObjectPath)
			atomic.AddInt64(&db.metrics.ErrorCount, 1)
			return fmt.Errorf("failed to write new metadata: %w", err)
		}

		os.Remove(oldMetadataPath)
	}

	db.cleanupEmptyDirPath(filepath.Dir(oldObjectPath))

	db.log.Info("Successfully renamed object from ", oldKey, " to ", newKey, " in bucket ", bucket)
	return nil
}

func (s *DB) BucketExists(bucket string) (bool, error) {
	bucketPath := filepath.Join(s.basePath, bucketsDir, bucket)
	_, err := os.Stat(bucketPath)
	if os.IsNotExist(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}
