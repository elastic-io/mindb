//go:build !windows
// +build !windows

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
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sys/unix"
)

const (
	bucketsDir   = "buckets"
	multipartDir = ".db.sys/multipart"
	tmpDir       = ".db.sys/tmp"
	metadataDir  = ".db.sys/buckets"

	metadataExt  = ".json"
	partExt      = ".part"
	uploadConfig = "upload.json"

	maxRetries = 3
	retryDelay = 100 * time.Millisecond

	dirPerm  = 0755
	filePerm = 0644

	md5sum = "md5"
)

const (
	MaxMultipartLifetime = 7 * 24 * time.Hour
	CleanupInterval      = 1 * time.Hour
)

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

func (s *DB) getBucketLock(bucket string) *sync.RWMutex {
	lock, _ := s.bucketLocks.LoadOrStore(bucket, &sync.RWMutex{})
	return lock.(*sync.RWMutex)
}

func (s *DB) getUploadLock(uploadID string) *sync.RWMutex {
	lock, _ := s.uploadLocks.LoadOrStore(uploadID, &sync.RWMutex{})
	return lock.(*sync.RWMutex)
}

func (s *DB) getObjectLock(bucket, key string) *sync.RWMutex {
	objectKey := bucket + "/" + key
	lock, _ := s.objectLocks.LoadOrStore(objectKey, &sync.RWMutex{})
	return lock.(*sync.RWMutex)
}

func (s *DB) getBuffer() []byte {
	if buf := s.bufferPool.Get(); buf != nil {
		buffer := buf.([]byte)

		if cap(buffer) >= s.config.BufferSize && s.config.BufferSize > 0 {
			return buffer[:s.config.BufferSize]
		}
	}

	if s.config.BufferSize <= 0 {
		s.config.BufferSize = 64 * KB
	}
	return make([]byte, s.config.BufferSize)
}

func (s *DB) putBuffer(buf []byte) {
	if buf != nil && cap(buf) >= s.config.BufferSize && s.config.BufferSize > 0 {

		clear(buf)
		// for i := range buf {
		//     buf[i] = 0
		// }
		s.bufferPool.Put(buf[:0])
	}
}

func (s *DB) getHasher() hash.Hash {
	if hasher := s.hasherPool.Get(); hasher != nil {
		h := hasher.(hash.Hash)
		h.Reset()
		return h
	}
	return md5.New()
}

func (s *DB) putHasher(hasher hash.Hash) {
	s.hasherPool.Put(hasher)
}

func (s *DB) Close() error {
	defer func() {
		if r := recover(); r != nil {
			s.log.Errorf("Recovered from panic in Close: %v\n%s", r, debug.Stack())
		}
	}()

	s.cancel()

	if s.cleanupTicker != nil {
		s.cleanupTicker.Stop()
		close(s.cleanupDone)
	}

	return nil
}

func (s *DB) verifyDataIntegrity(path string, expectedChecksum string) error {
	if !s.config.EnableChecksumVerify || expectedChecksum == "" {
		return nil
	}

	expectedChecksum = strings.TrimSpace(strings.Trim(expectedChecksum, `"`))

	if strings.Contains(expectedChecksum, "-") {
		parts := strings.Split(expectedChecksum, "-")
		if len(parts) == 2 {
			if partCount, err := strconv.Atoi(parts[1]); err == nil && partCount > 1 {

				s.log.Debugf("Skipping integrity check for multipart ETag: %s", expectedChecksum)
				return nil
			}
		}
	}

	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("failed to open file for verification: %w", err)
	}
	defer file.Close()

	hasher := s.getHasher()
	defer s.putHasher(hasher)

	buf := s.getBuffer()
	defer s.putBuffer(buf)

	if len(buf) == 0 {
		return fmt.Errorf("buffer is empty, cannot proceed with verification")
	}

	if _, err := io.CopyBuffer(hasher, file, buf); err != nil {
		return fmt.Errorf("failed to calculate checksum: %w", err)
	}

	actualChecksum := hex.EncodeToString(hasher.Sum(nil))

	s.log.Debugf("Checksum verification - Expected: '%s', Actual: '%s'", expectedChecksum, actualChecksum)

	if actualChecksum != expectedChecksum {
		return fmt.Errorf("data corruption detected: expected '%s', got '%s'",
			expectedChecksum, actualChecksum)
	}

	return nil
}

func (s *DB) atomicWriteWithVerify(filePath string, data []byte, expectedChecksum string) error {
	dir := filepath.Dir(filePath)
	if err := os.MkdirAll(dir, dirPerm); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	tmpFile := filepath.Join(s.basePath, tmpDir, fmt.Sprintf("tmp_%d_%d_%s",
		time.Now().UnixNano(), os.Getpid(), filepath.Base(filePath)))

	var file *os.File
	var err error

	if s.config.UseDirectIO {

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

	if err = s.verifyDataIntegrity(tmpFile, expectedChecksum); err != nil {
		return fmt.Errorf("data integrity check failed: %w", err)
	}

	if err = os.Rename(tmpFile, filePath); err != nil {
		return fmt.Errorf("failed to move temp file: %w", err)
	}

	return nil
}

func (s *DB) atomicWrite(filePath string, data []byte) error {
	checksum := CalculateETag(data)
	return s.atomicWriteWithVerify(filePath, data, checksum)
}

func (s *DB) readFileOptimized(path string) ([]byte, error) {
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
	buf := s.getBuffer()
	defer s.putBuffer(buf)

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

func (s *DB) readFileMmap(path string) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	info, err := file.Stat()
	if err != nil {
		return nil, err
	}

	size := int(info.Size())
	if size == 0 {
		return nil, nil
	}

	data, err := unix.Mmap(int(file.Fd()), 0, size, unix.PROT_READ, unix.MAP_SHARED)
	if err != nil {
		return nil, fmt.Errorf("mmap failed: %w", err)
	}
	defer unix.Munmap(data)

	result := make([]byte, size)
	copy(result, data)

	return result, nil
}

func (s *DB) startOptimizedCleanupRoutine() {
	s.cleanupTicker = time.NewTicker(s.config.CleanupInterval)

	go func() {
		defer func() {
			if r := recover(); r != nil {
				s.log.Errorf("Recovered from panic in cleanup routine: %v\n%s", r, debug.Stack())

				time.Sleep(time.Minute)
				s.startOptimizedCleanupRoutine()
			}
		}()

		for {
			select {
			case <-s.cleanupTicker.C:

				var wg sync.WaitGroup

				wg.Add(1)
				go func() {
					defer wg.Done()
					if err := s.cleanupExpiredUploads(); err != nil {
						s.log.Error("Failed to cleanup expired multipart uploads: ", err)
					}
				}()

				wg.Add(1)
				go func() {
					defer wg.Done()
					if err := s.cleanupTempFiles(); err != nil {
						s.log.Error("Failed to cleanup temp files: ", err)
					}
				}()

				wg.Add(1)
				go func() {
					defer wg.Done()
					if err := s.cleanupEmptyDirectories(); err != nil {
						s.log.Error("Failed to cleanup empty directories: ", err)
					}
				}()

				wg.Add(1)
				go func() {
					defer wg.Done()
					s.cleanupLockMaps()
				}()

				wg.Wait()

			case <-s.ctx.Done():
				return
			}
		}
	}()
}

func (s *DB) cleanupExpiredUploads() error {
	defer func() {
		if r := recover(); r != nil {
			s.log.Errorf("Recovered from panic in cleanupExpiredUploads: %v\n%s", r, debug.Stack())
		}
	}()

	multipartPath := filepath.Join(s.basePath, multipartDir)
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

		data, err := s.readFileOptimized(infoFile)
		if err != nil {
			return nil
		}

		uploadInfo := &MultipartUploadInfo{}
		if err := uploadInfo.UnmarshalJSON(data); err != nil {
			return nil
		}

		if now.Sub(uploadInfo.CreatedAt) > MaxMultipartLifetime {
			s.log.Info("Cleaning up expired multipart upload: ", uploadInfo.UploadID)

			func() {
				defer func() {
					if r := recover(); r != nil {
						s.log.Errorf("Recovered from panic while cleaning upload %s: %v\n%s",
							uploadInfo.UploadID, r, debug.Stack())
					}
				}()

				if err := os.RemoveAll(path); err != nil {
					s.log.Warn("Failed to remove expired upload directory: ", err)
				}
			}()
		}

		return nil
	})
}

func (s *DB) cleanupTempFiles() error {
	tmpPath := filepath.Join(s.basePath, tmpDir)
	now := time.Now()

	return filepath.WalkDir(tmpPath, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}

		if info, err := d.Info(); err == nil {
			if now.Sub(info.ModTime()) > s.config.TempFileMaxAge {
				os.Remove(path)
			}
		}

		return nil
	})
}

func (s *DB) cleanupEmptyDirectories() error {
	bucketsPath := filepath.Join(s.basePath, bucketsDir)

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

func (s *DB) cleanupLockMaps() {

	s.bucketLocks.Range(func(key, value interface{}) bool {

		return true
	})
}

func (s *DB) withRetry(operation func() error) error {
	var lastErr error
	for i := 0; i < maxRetries; i++ {
		if err := operation(); err != nil {
			lastErr = err
			s.log.Warnf("Operation failed (attempt %d/%d): %v", i+1, maxRetries, err)
			if i < maxRetries-1 {
				time.Sleep(retryDelay * time.Duration(i+1))
			}
			continue
		}
		return nil
	}
	return fmt.Errorf("operation failed after %d retries: %w", maxRetries, lastErr)
}

func (s *DB) ListBuckets() ([]BucketInfo, error) {
	defer func() {
		if r := recover(); r != nil {
			s.log.Errorf("Recovered from panic in ListBuckets: %v\n%s", r, debug.Stack())
		}
	}()

	atomic.AddInt64(&s.metrics.ListOps, 1)

	var buckets []BucketInfo
	bucketsPath := filepath.Join(s.basePath, bucketsDir)

	entries, err := os.ReadDir(bucketsPath)
	if err != nil {
		if os.IsNotExist(err) {
			return buckets, nil
		}
		atomic.AddInt64(&s.metrics.ErrorCount, 1)
		return nil, fmt.Errorf("failed to read buckets directory: %w", err)
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		bucketName := entry.Name()
		info, err := entry.Info()
		if err != nil {
			s.log.Warn("Failed to get bucket info for ", bucketName, ": ", err)
			continue
		}

		buckets = append(buckets, BucketInfo{
			Name:         bucketName,
			CreationDate: info.ModTime(),
		})
	}

	return buckets, nil
}

func (s *DB) CreateBucket(bucket string) error {
	bucketLock := s.getBucketLock(bucket)
	bucketLock.Lock()
	defer bucketLock.Unlock()

	bucketPath := filepath.Join(s.basePath, bucketsDir, bucket)

	if _, err := os.Stat(bucketPath); err == nil {
		return fmt.Errorf("bucket already exists")
	}

	err := s.withRetry(func() error {
		return os.MkdirAll(bucketPath, dirPerm)
	})
	if err != nil {
		atomic.AddInt64(&s.metrics.ErrorCount, 1)
		return fmt.Errorf("failed to create bucket directory: %w", err)
	}

	metadataPath := filepath.Join(s.basePath, metadataDir, bucket+metadataExt)
	bucketInfo := &BucketInfo{
		Name:         bucket,
		CreationDate: time.Now(),
	}

	data, err := bucketInfo.MarshalJSON()
	if err != nil {
		atomic.AddInt64(&s.metrics.ErrorCount, 1)
		return fmt.Errorf("failed to marshal bucket info: %w", err)
	}

	return s.atomicWrite(metadataPath, data)
}

func (s *DB) DeleteBucket(bucket string) error {
	defer func() {
		if r := recover(); r != nil {
			s.log.Errorf("Recovered from panic in DeleteBucket: %v\n%s", r, debug.Stack())
		}
	}()

	bucketLock := s.getBucketLock(bucket)
	bucketLock.Lock()
	defer bucketLock.Unlock()

	atomic.AddInt64(&s.metrics.DeleteOps, 1)

	bucketPath := filepath.Join(s.basePath, bucketsDir, bucket)

	if _, err := os.Stat(bucketPath); os.IsNotExist(err) {
		atomic.AddInt64(&s.metrics.ErrorCount, 1)
		return fmt.Errorf("bucket not found")
	}

	entries, err := os.ReadDir(bucketPath)
	if err != nil {
		atomic.AddInt64(&s.metrics.ErrorCount, 1)
		return fmt.Errorf("failed to read bucket directory: %w", err)
	}

	if len(entries) > 0 {
		atomic.AddInt64(&s.metrics.ErrorCount, 1)
		return fmt.Errorf("bucket not empty")
	}

	if err := os.RemoveAll(bucketPath); err != nil {
		atomic.AddInt64(&s.metrics.ErrorCount, 1)
		return fmt.Errorf("failed to remove bucket directory: %w", err)
	}

	metadataPath := filepath.Join(s.basePath, metadataDir, bucket+metadataExt)
	os.Remove(metadataPath)

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

func (s *DB) getObjectPath(bucket, key string) string {

	hash := md5.Sum([]byte(key))
	hashStr := hex.EncodeToString(hash[:])

	return filepath.Join(s.basePath, bucketsDir, bucket,
		hashStr[:2], hashStr[2:4], key)
}

func (s *DB) getObjectMetadataPath(bucket, key string) string {
	return s.getObjectPath(bucket, key) + metadataExt
}

func (s *DB) ObjectExists(bucket, key string) (bool, error) {
	start := time.Now()
	atomic.AddInt64(&s.metrics.ActiveReads, 1)
	defer func() {
		atomic.AddInt64(&s.metrics.ActiveReads, -1)
		latency := time.Since(start).Nanoseconds()

		atomic.StoreInt64(&s.metrics.AvgReadLatency, latency)

		if r := recover(); r != nil {
			s.log.Errorf("Recovered from panic in ObjectExists: %v\n%s", r, debug.Stack())
		}
	}()

	exists, err := s.BucketExists(bucket)
	if err != nil {
		return false, fmt.Errorf("failed to check bucket existence: %w", err)
	}
	if !exists {
		return false, nil
	}

	objectPath := s.getObjectPath(bucket, key)

	objLock := s.getObjectLock(bucket, key)
	objLock.RLock()
	defer objLock.RUnlock()

	_, err = os.Stat(objectPath)
	if os.IsNotExist(err) {
		return false, nil
	}
	if err != nil {
		atomic.AddInt64(&s.metrics.ErrorCount, 1)
		return false, fmt.Errorf("failed to check object existence: %w", err)
	}

	return true, nil
}

func (s *DB) ListObjects(bucket, prefix, marker, delimiter string, maxKeys int) ([]ObjectInfo, []string, error) {
	bucketLock := s.getBucketLock(bucket)
	bucketLock.RLock()
	defer bucketLock.RUnlock()

	defer func() {
		if r := recover(); r != nil {
			s.log.Errorf("Recovered from panic in ListObjects: %v\n%s", r, debug.Stack())
		}
	}()

	atomic.AddInt64(&s.metrics.ListOps, 1)

	var objects []ObjectInfo
	var commonPrefixes []string
	prefixMap := make(map[string]bool)

	bucketPath := filepath.Join(s.basePath, bucketsDir, bucket)

	err := filepath.WalkDir(bucketPath, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}

		if strings.HasSuffix(path, metadataExt) {
			return nil
		}

		relPath, err := filepath.Rel(bucketPath, path)
		if err != nil {
			return nil
		}

		parts := strings.Split(filepath.ToSlash(relPath), "/")
		if len(parts) < 3 {
			return nil
		}
		objKey := strings.Join(parts[2:], "/")

		if prefix != "" && !strings.HasPrefix(objKey, prefix) {
			return nil
		}

		if marker != "" && objKey <= marker {
			return nil
		}

		if delimiter != "" {
			delimiterIndex := strings.Index(objKey[len(prefix):], delimiter)
			if delimiterIndex >= 0 {
				commonPrefix := objKey[:len(prefix)+delimiterIndex+1]
				if !prefixMap[commonPrefix] {
					prefixMap[commonPrefix] = true
					commonPrefixes = append(commonPrefixes, commonPrefix)
				}
				return nil
			}
		}

		info, err := d.Info()
		if err != nil {
			return nil
		}

		metadataPath := path + metadataExt
		var etag string
		if metaData, err := s.readFileOptimized(metadataPath); err == nil {
			objData := &ObjectData{}
			if objData.UnmarshalJSON(metaData) == nil {
				etag = objData.ETag
			}
		}

		objects = append(objects, ObjectInfo{
			Key:          objKey,
			Size:         info.Size(),
			LastModified: info.ModTime(),
			ETag:         etag,
		})

		return nil
	})
	if err != nil {
		atomic.AddInt64(&s.metrics.ErrorCount, 1)
		return nil, nil, fmt.Errorf("failed to walk bucket directory: %w", err)
	}

	sort.Slice(objects, func(i, j int) bool {
		return objects[i].Key < objects[j].Key
	})

	if len(objects) > maxKeys {
		objects = objects[:maxKeys]
	}

	sort.Strings(commonPrefixes)

	return objects, commonPrefixes, nil
}

func (s *DB) GetObject(bucket, key string) (*ObjectData, error) {
	start := time.Now()
	atomic.AddInt64(&s.metrics.ActiveReads, 1)
	defer func() {
		atomic.AddInt64(&s.metrics.ActiveReads, -1)
		atomic.AddInt64(&s.metrics.ReadOps, 1)
		latency := time.Since(start).Nanoseconds()
		atomic.StoreInt64(&s.metrics.AvgReadLatency, latency)

		if r := recover(); r != nil {
			s.log.Errorf("Recovered from panic in GetObject: %v\n%s", r, debug.Stack())
		}
	}()

	objectPath := s.getObjectPath(bucket, key)
	metadataPath := s.getObjectMetadataPath(bucket, key)

	objLock := s.getObjectLock(bucket, key)
	objLock.RLock()
	defer objLock.RUnlock()

	if _, err := os.Stat(objectPath); os.IsNotExist(err) {
		return nil, fmt.Errorf("object not found")
	}

	metaData, err := s.readFileOptimized(metadataPath)
	if err != nil {

		var data []byte
		err = s.withRetry(func() error {
			data, err = s.readFileOptimized(objectPath)
			return err
		})
		if err != nil {
			if os.IsNotExist(err) {
				return nil, fmt.Errorf("object not found")
			}
			atomic.AddInt64(&s.metrics.ErrorCount, 1)
			return nil, fmt.Errorf("failed to read object: %w", err)
		}

		info, _ := os.Stat(objectPath)
		objectData := ObjectData{
			Key:          key,
			Data:         data,
			ContentType:  "application/octet-stream",
			LastModified: info.ModTime(),
			ETag:         CalculateETag(data),
		}

		atomic.AddInt64(&s.metrics.ReadBytes, int64(len(data)))
		return &objectData, nil
	}

	objectData := &ObjectData{}
	if err := objectData.UnmarshalJSON(metaData); err != nil {
		atomic.AddInt64(&s.metrics.ErrorCount, 1)
		return nil, fmt.Errorf("failed to parse metadata: %w", err)
	}

	var data []byte
	if s.config.UseMmap && objectData.Size > 0 && objectData.Size < 100*MB {
		data, err = s.readFileMmap(objectPath)
	} else {
		data, err = s.readFileOptimized(objectPath)
	}

	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("object not found")
		}
		atomic.AddInt64(&s.metrics.ErrorCount, 1)
		return nil, fmt.Errorf("failed to read object: %w", err)
	}

	if err = s.verifyDataIntegrity(objectPath, objectData.ETag); err != nil {
		atomic.AddInt64(&s.metrics.ErrorCount, 1)
		return nil, fmt.Errorf("data integrity check failed: %w", err)
	}

	objectData.Data = data
	atomic.AddInt64(&s.metrics.ReadBytes, int64(len(data)))

	return objectData, nil
}

func (s *DB) PutObject(bucket string, object *ObjectData) error {
	start := time.Now()
	atomic.AddInt64(&s.metrics.ActiveWrites, 1)
	defer func() {
		atomic.AddInt64(&s.metrics.ActiveWrites, -1)
		atomic.AddInt64(&s.metrics.WriteOps, 1)
		latency := time.Since(start).Nanoseconds()
		atomic.StoreInt64(&s.metrics.AvgWriteLatency, latency)

		if r := recover(); r != nil {
			s.log.Errorf("Recovered from panic in PutObject: %v\n%s", r, debug.Stack())
		}
	}()

	objectPath := s.getObjectPath(bucket, object.Key)
	metadataPath := s.getObjectMetadataPath(bucket, object.Key)

	objLock := s.getObjectLock(bucket, object.Key)
	objLock.Lock()
	defer objLock.Unlock()

	if err := os.MkdirAll(filepath.Dir(objectPath), dirPerm); err != nil {
		atomic.AddInt64(&s.metrics.ErrorCount, 1)
		return fmt.Errorf("failed to create object directory: %w", err)
	}

	if object.ETag == "" {
		object.ETag = CalculateETag(object.Data)
	}

	var err error
	isMultipart := strings.Contains(object.ETag, "-")
	if isMultipart {

		err = s.withRetry(func() error {
			return s.atomicWriteWithoutVerify(objectPath, object.Data)
		})
	} else {

		err = s.withRetry(func() error {
			return s.atomicWriteWithVerify(objectPath, object.Data, object.ETag)
		})
	}

	if err != nil {
		atomic.AddInt64(&s.metrics.ErrorCount, 1)
		return fmt.Errorf("failed to write object data: %w", err)
	}

	metadata := *object
	metadata.Data = nil
	metadata.Size = int64(len(object.Data))
	metadata.LastModified = time.Now()

	metaData, err := metadata.MarshalJSON()
	if err != nil {
		atomic.AddInt64(&s.metrics.ErrorCount, 1)
		return fmt.Errorf("failed to marshal metadata: %w", err)
	}

	err = s.withRetry(func() error {
		return s.atomicWriteWithVerify(metadataPath, metaData, CalculateETag(metaData))
	})
	if err != nil {

		os.Remove(objectPath)
		atomic.AddInt64(&s.metrics.ErrorCount, 1)
		return fmt.Errorf("failed to write metadata: %w", err)
	}

	atomic.AddInt64(&s.metrics.WriteBytes, int64(len(object.Data)))
	return nil
}

func (s *DB) DeleteObject(bucket, key string) error {
	defer func() {
		if r := recover(); r != nil {
			s.log.Errorf("Recovered from panic in DeleteObject: %v\n%s", r, debug.Stack())
		}
	}()

	atomic.AddInt64(&s.metrics.DeleteOps, 1)

	objectPath := s.getObjectPath(bucket, key)
	metadataPath := s.getObjectMetadataPath(bucket, key)

	objLock := s.getObjectLock(bucket, key)
	objLock.Lock()
	defer objLock.Unlock()

	if _, err := os.Stat(objectPath); os.IsNotExist(err) {
		atomic.AddInt64(&s.metrics.ErrorCount, 1)
		return fmt.Errorf("object not found")
	}

	if err := os.Remove(objectPath); err != nil {
		atomic.AddInt64(&s.metrics.ErrorCount, 1)
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

func (s *DB) getMultipartPath(uploadID string) string {
	return filepath.Join(s.basePath, multipartDir, uploadID)
}

func (s *DB) CreateMultipartUpload(bucket, key, contentType string, metadata map[string]string) (string, error) {
	uploadLock := s.getUploadLock("create_" + bucket + "_" + key)
	uploadLock.Lock()
	defer uploadLock.Unlock()

	s.log.Info("Creating multipart upload for ", key, " in bucket ", bucket)

	exists, err := s.BucketExists(bucket)
	if err != nil {
		atomic.AddInt64(&s.metrics.ErrorCount, 1)
		return "", fmt.Errorf("failed to check bucket existence: %w", err)
	}
	if !exists {
		atomic.AddInt64(&s.metrics.ErrorCount, 1)
		return "", fmt.Errorf("bucket does not exist: %s", bucket)
	}

	uploadID := GenerateUploadID(bucket, key)

	uploadPath := s.getMultipartPath(uploadID)
	if err := os.MkdirAll(uploadPath, dirPerm); err != nil {
		atomic.AddInt64(&s.metrics.ErrorCount, 1)
		return "", fmt.Errorf("failed to create upload directory: %w", err)
	}

	uploadInfo := &MultipartUploadInfo{
		Bucket:      bucket,
		Key:         key,
		UploadID:    uploadID,
		ContentType: contentType,
		Metadata:    metadata,
		CreatedAt:   time.Now(),
	}

	infoPath := filepath.Join(uploadPath, uploadConfig)
	data, err := uploadInfo.MarshalJSON()
	if err != nil {
		atomic.AddInt64(&s.metrics.ErrorCount, 1)
		return "", fmt.Errorf("failed to marshal upload info: %w", err)
	}

	if err := os.WriteFile(infoPath, data, filePerm); err != nil {
		os.RemoveAll(uploadPath)
		atomic.AddInt64(&s.metrics.ErrorCount, 1)
		return "", fmt.Errorf("failed to write upload info: %w", err)
	}

	return uploadID, nil
}

func (s *DB) UploadPart(bucket, key, uploadID string, partNumber int, data []byte) (string, error) {
	if partNumber < 1 || partNumber > 10000 {
		return "", fmt.Errorf("invalid part number: must be between 1 and 10000")
	}

	uploadLock := s.getUploadLock(uploadID)
	uploadLock.Lock()
	defer uploadLock.Unlock()

	s.log.Info("Uploading part ", partNumber, " for ", key, " in bucket ", bucket, " (size: ", len(data), " bytes)")

	uploadPath := s.getMultipartPath(uploadID)

	infoPath := filepath.Join(uploadPath, uploadConfig)
	infoData, err := s.readFileOptimized(infoPath)
	if err != nil {
		atomic.AddInt64(&s.metrics.ErrorCount, 1)
		return "", fmt.Errorf("upload ID not found: %s", uploadID)
	}

	uploadInfo := &MultipartUploadInfo{}
	if err := uploadInfo.UnmarshalJSON(infoData); err != nil {
		atomic.AddInt64(&s.metrics.ErrorCount, 1)
		return "", fmt.Errorf("failed to parse upload info: %w", err)
	}

	if uploadInfo.Bucket != bucket || uploadInfo.Key != key {
		atomic.AddInt64(&s.metrics.ErrorCount, 1)
		return "", fmt.Errorf("bucket or key mismatch")
	}

	hasher := s.getHasher()
	defer s.putHasher(hasher)

	hasher.Write(data)
	etag := hex.EncodeToString(hasher.Sum(nil))

	partPath := filepath.Join(uploadPath, fmt.Sprintf("part.%05d", partNumber))
	err = s.withRetry(func() error {
		return s.atomicWriteWithVerify(partPath, data, etag)
	})
	if err != nil {
		atomic.AddInt64(&s.metrics.ErrorCount, 1)
		return "", fmt.Errorf("failed to write part data: %w", err)
	}

	partInfo := &PartInfo{
		PartNumber:   partNumber,
		ETag:         etag,
		Size:         len(data),
		LastModified: time.Now(),
	}

	partInfoPath := filepath.Join(uploadPath, fmt.Sprintf("part.%05d.json", partNumber))
	partInfoData, err := partInfo.MarshalJSON()
	if err != nil {
		os.Remove(partPath)
		atomic.AddInt64(&s.metrics.ErrorCount, 1)
		return "", fmt.Errorf("failed to marshal part info: %w", err)
	}

	err = s.withRetry(func() error {
		return s.atomicWriteWithVerify(partInfoPath, partInfoData, CalculateETag(partInfoData))
	})
	if err != nil {
		os.Remove(partPath)
		atomic.AddInt64(&s.metrics.ErrorCount, 1)
		return "", fmt.Errorf("failed to write part info: %w", err)
	}

	atomic.AddInt64(&s.metrics.WriteBytes, int64(len(data)))
	return etag, nil
}

func (s *DB) CompleteMultipartUpload(bucket, key, uploadID string, parts []MultipartPart) (string, error) {
	uploadLock := s.getUploadLock(uploadID)
	uploadLock.Lock()
	defer uploadLock.Unlock()

	defer func() {
		if r := recover(); r != nil {
			s.log.Errorf("Recovered from panic in CompleteMultipartUpload: %v\n%s", r, debug.Stack())
		}
	}()

	s.log.Info("Completing multipart upload for ", key, " in bucket ", bucket, " with ", len(parts), " parts")

	if len(parts) == 0 {
		atomic.AddInt64(&s.metrics.ErrorCount, 1)
		return "", fmt.Errorf("no parts specified")
	}

	uploadPath := s.getMultipartPath(uploadID)

	infoPath := filepath.Join(uploadPath, uploadConfig)
	infoData, err := s.readFileOptimized(infoPath)
	if err != nil {
		atomic.AddInt64(&s.metrics.ErrorCount, 1)
		return "", fmt.Errorf("upload ID not found: %s", uploadID)
	}

	uploadInfo := &MultipartUploadInfo{}
	if err := uploadInfo.UnmarshalJSON(infoData); err != nil {
		atomic.AddInt64(&s.metrics.ErrorCount, 1)
		return "", fmt.Errorf("failed to parse upload info: %w", err)
	}

	if uploadInfo.Bucket != bucket || uploadInfo.Key != key {
		atomic.AddInt64(&s.metrics.ErrorCount, 1)
		return "", fmt.Errorf("bucket or key mismatch")
	}

	sortedParts := make([]MultipartPart, len(parts))
	copy(sortedParts, parts)
	sort.Slice(sortedParts, func(i, j int) bool {
		return sortedParts[i].PartNumber < sortedParts[j].PartNumber
	})

	tmpFile := filepath.Join(s.basePath, tmpDir, fmt.Sprintf("merge_%s_%d", uploadID, time.Now().UnixNano()))

	var file *os.File
	if s.config.UseDirectIO {
		file, err = openWithDirectIO(tmpFile, filePerm)
	} else {
		file, err = os.Create(tmpFile)
	}

	if err != nil {
		atomic.AddInt64(&s.metrics.ErrorCount, 1)
		return "", fmt.Errorf("failed to create temp file: %w", err)
	}
	defer func() {
		file.Close()
		os.Remove(tmpFile)
	}()

	var allETags []string
	buf := s.getBuffer()
	defer s.putBuffer(buf)

	if len(buf) == 0 {
		buf = make([]byte, 64*KB)
	}

	for _, part := range sortedParts {
		partPath := filepath.Join(uploadPath, fmt.Sprintf("part.%05d", part.PartNumber))
		partInfoPath := filepath.Join(uploadPath, fmt.Sprintf("part.%05d.json", part.PartNumber))

		partInfoData, err := s.readFileOptimized(partInfoPath)
		if err != nil {
			atomic.AddInt64(&s.metrics.ErrorCount, 1)
			return "", fmt.Errorf("part %d info not found: %w", part.PartNumber, err)
		}

		partInfo := &PartInfo{}
		if err := partInfo.UnmarshalJSON(partInfoData); err != nil {
			atomic.AddInt64(&s.metrics.ErrorCount, 1)
			return "", fmt.Errorf("failed to parse part info: %w", err)
		}

		if partInfo.ETag != part.ETag {
			atomic.AddInt64(&s.metrics.ErrorCount, 1)
			return "", fmt.Errorf("ETag mismatch for part %d: expected %s, got %s",
				part.PartNumber, partInfo.ETag, part.ETag)
		}

		partFile, err := os.Open(partPath)
		if err != nil {
			atomic.AddInt64(&s.metrics.ErrorCount, 1)
			return "", fmt.Errorf("failed to open part %d: %w", part.PartNumber, err)
		}

		_, err = io.CopyBuffer(file, partFile, buf)
		partFile.Close()
		if err != nil {
			atomic.AddInt64(&s.metrics.ErrorCount, 1)
			return "", fmt.Errorf("failed to copy part %d: %w", part.PartNumber, err)
		}

		allETags = append(allETags, part.ETag)
	}

	if err = file.Sync(); err != nil {
		atomic.AddInt64(&s.metrics.ErrorCount, 1)
		return "", fmt.Errorf("failed to sync merged file: %w", err)
	}

	file.Close()

	finalData, err := s.readFileOptimized(tmpFile)
	if err != nil {
		atomic.AddInt64(&s.metrics.ErrorCount, 1)
		return "", fmt.Errorf("failed to read merged data: %w", err)
	}

	finalETag := CalculateMultipartETag(allETags)

	objectData := &ObjectData{
		Key:          key,
		Data:         finalData,
		ContentType:  uploadInfo.ContentType,
		LastModified: time.Now(),
		ETag:         finalETag,
		Metadata:     uploadInfo.Metadata,
		Size:         int64(len(finalData)),
	}

	if err := s.putObjectWithoutVerification(bucket, objectData); err != nil {
		atomic.AddInt64(&s.metrics.ErrorCount, 1)
		return "", fmt.Errorf("failed to store final object: %w", err)
	}

	go func() {
		defer func() {
			if r := recover(); r != nil {
				s.log.Errorf("Recovered from panic while cleaning up upload %s: %v\n%s",
					uploadID, r, debug.Stack())
			}
		}()

		if err := os.RemoveAll(uploadPath); err != nil {
			s.log.Warn("Failed to cleanup multipart upload directory: ", err)
		}
	}()

	s.log.Info("Completed multipart upload for ", key, " in bucket ", bucket, " with final ETag: ", finalETag)

	return finalETag, nil
}

func (s *DB) putObjectWithoutVerification(bucket string, object *ObjectData) error {
	objectPath := s.getObjectPath(bucket, object.Key)
	metadataPath := s.getObjectMetadataPath(bucket, object.Key)

	objLock := s.getObjectLock(bucket, object.Key)
	objLock.Lock()
	defer objLock.Unlock()

	if err := os.MkdirAll(filepath.Dir(objectPath), dirPerm); err != nil {
		return fmt.Errorf("failed to create object directory: %w", err)
	}

	err := s.withRetry(func() error {
		return s.atomicWriteWithoutVerify(objectPath, object.Data)
	})
	if err != nil {
		return fmt.Errorf("failed to write object data: %w", err)
	}

	metadata := *object
	metadata.Data = nil
	metadata.Size = int64(len(object.Data))
	metadata.LastModified = time.Now()

	metaData, err := metadata.MarshalJSON()
	if err != nil {
		return fmt.Errorf("failed to marshal metadata: %w", err)
	}

	err = s.withRetry(func() error {
		return s.atomicWriteWithoutVerify(metadataPath, metaData)
	})
	if err != nil {
		os.Remove(objectPath)
		return fmt.Errorf("failed to write metadata: %w", err)
	}

	return nil
}

func (s *DB) atomicWriteWithoutVerify(filePath string, data []byte) error {
	dir := filepath.Dir(filePath)
	if err := os.MkdirAll(dir, dirPerm); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	tmpFile := filepath.Join(s.basePath, tmpDir, fmt.Sprintf("tmp_%d_%d_%s",
		time.Now().UnixNano(), os.Getpid(), filepath.Base(filePath)))

	var file *os.File
	var err error

	if s.config.UseDirectIO {
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

func (s *DB) AbortMultipartUpload(bucket, key, uploadID string) error {
	uploadLock := s.getUploadLock(uploadID)
	uploadLock.Lock()
	defer uploadLock.Unlock()

	defer func() {
		if r := recover(); r != nil {
			s.log.Errorf("Recovered from panic in AbortMultipartUpload: %v\n%s", r, debug.Stack())
		}
	}()

	s.log.Info("Aborting multipart upload for ", key, " in bucket ", bucket)

	uploadPath := s.getMultipartPath(uploadID)

	infoPath := filepath.Join(uploadPath, uploadConfig)
	infoData, err := s.readFileOptimized(infoPath)
	if err != nil {
		atomic.AddInt64(&s.metrics.ErrorCount, 1)
		return fmt.Errorf("upload ID not found: %s", uploadID)
	}

	uploadInfo := &MultipartUploadInfo{}
	if err := uploadInfo.UnmarshalJSON(infoData); err != nil {
		atomic.AddInt64(&s.metrics.ErrorCount, 1)
		return fmt.Errorf("failed to parse upload info: %w", err)
	}

	if uploadInfo.Bucket != bucket || uploadInfo.Key != key {
		atomic.AddInt64(&s.metrics.ErrorCount, 1)
		return fmt.Errorf("bucket or key mismatch")
	}

	if err := os.RemoveAll(uploadPath); err != nil {
		atomic.AddInt64(&s.metrics.ErrorCount, 1)
		return fmt.Errorf("failed to remove upload directory: %w", err)
	}

	return nil
}

func (s *DB) ListMultipartUploads(bucket string) ([]MultipartUploadInfo, error) {
	bucketLock := s.getBucketLock(bucket)
	bucketLock.RLock()
	defer bucketLock.RUnlock()

	defer func() {
		if r := recover(); r != nil {
			s.log.Errorf("Recovered from panic in ListMultipartUploads: %v\n%s", r, debug.Stack())
		}
	}()

	atomic.AddInt64(&s.metrics.ListOps, 1)
	s.log.Info("Listing multipart uploads for bucket ", bucket)

	var uploads []MultipartUploadInfo
	multipartPath := filepath.Join(s.basePath, multipartDir)

	entries, err := os.ReadDir(multipartPath)
	if err != nil {
		if os.IsNotExist(err) {
			return uploads, nil
		}
		atomic.AddInt64(&s.metrics.ErrorCount, 1)
		return nil, fmt.Errorf("failed to read multipart directory: %w", err)
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		uploadID := entry.Name()
		infoPath := filepath.Join(multipartPath, uploadID, uploadConfig)

		infoData, err := s.readFileOptimized(infoPath)
		if err != nil {
			s.log.Warn("Failed to read upload info for ", uploadID, ": ", err)
			continue
		}

		uploadInfo := &MultipartUploadInfo{}
		if err := uploadInfo.UnmarshalJSON(infoData); err != nil {
			s.log.Warn("Failed to parse upload info for ", uploadID, ": ", err)
			continue
		}

		if uploadInfo.Bucket == bucket {
			uploads = append(uploads, MultipartUploadInfo{
				Bucket:    uploadInfo.Bucket,
				Key:       uploadInfo.Key,
				UploadID:  uploadInfo.UploadID,
				CreatedAt: uploadInfo.CreatedAt,
			})
		}
	}

	return uploads, nil
}

func (s *DB) ListParts(bucket, key, uploadID string) ([]*PartInfo, error) {
	uploadLock := s.getUploadLock(uploadID)
	uploadLock.RLock()
	defer uploadLock.RUnlock()

	defer func() {
		if r := recover(); r != nil {
			s.log.Errorf("Recovered from panic in ListParts: %v\n%s", r, debug.Stack())
		}
	}()

	atomic.AddInt64(&s.metrics.ListOps, 1)
	s.log.Info("Listing parts for upload ", uploadID, " in bucket ", bucket)

	uploadPath := s.getMultipartPath(uploadID)

	infoPath := filepath.Join(uploadPath, uploadConfig)
	infoData, err := s.readFileOptimized(infoPath)
	if err != nil {
		atomic.AddInt64(&s.metrics.ErrorCount, 1)
		return nil, fmt.Errorf("upload ID not found: %s", uploadID)
	}

	uploadInfo := &MultipartUploadInfo{}
	if err := uploadInfo.UnmarshalJSON(infoData); err != nil {
		atomic.AddInt64(&s.metrics.ErrorCount, 1)
		return nil, fmt.Errorf("failed to parse upload info: %w", err)
	}

	if uploadInfo.Bucket != bucket || uploadInfo.Key != key {
		atomic.AddInt64(&s.metrics.ErrorCount, 1)
		return nil, fmt.Errorf("bucket or key mismatch")
	}

	var parts []*PartInfo

	entries, err := os.ReadDir(uploadPath)
	if err != nil {
		atomic.AddInt64(&s.metrics.ErrorCount, 1)
		return nil, fmt.Errorf("failed to read upload directory: %w", err)
	}

	for _, entry := range entries {
		if entry.IsDir() || !strings.HasPrefix(entry.Name(), "part.") || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}

		partInfoPath := filepath.Join(uploadPath, entry.Name())
		partInfoData, err := s.readFileOptimized(partInfoPath)
		if err != nil {
			s.log.Warn("Failed to read part info file ", entry.Name(), ": ", err)
			continue
		}

		partInfo := &PartInfo{}
		if err := partInfo.UnmarshalJSON(partInfoData); err != nil {
			s.log.Warn("Failed to parse part info file ", entry.Name(), ": ", err)
			continue
		}

		parts = append(parts, partInfo)
	}

	sort.Slice(parts, func(i, j int) bool {
		return parts[i].PartNumber < parts[j].PartNumber
	})

	return parts, nil
}

func (s *DB) GetObjectStream(bucket, key string) (io.ReadCloser, *ObjectData, error) {
	defer func() {
		if r := recover(); r != nil {
			s.log.Errorf("Recovered from panic in GetObjectStream: %v\n%s", r, debug.Stack())
		}
	}()

	atomic.AddInt64(&s.metrics.ReadOps, 1)

	objectPath := s.getObjectPath(bucket, key)
	metadataPath := s.getObjectMetadataPath(bucket, key)

	objLock := s.getObjectLock(bucket, key)
	objLock.RLock()
	defer objLock.RUnlock()

	file, err := os.Open(objectPath)
	if err != nil {
		if os.IsNotExist(err) {
			atomic.AddInt64(&s.metrics.ErrorCount, 1)
			return nil, nil, fmt.Errorf("object not found")
		}
		atomic.AddInt64(&s.metrics.ErrorCount, 1)
		return nil, nil, fmt.Errorf("failed to open object: %w", err)
	}

	objectData := &ObjectData{}
	if metaData, err := s.readFileOptimized(metadataPath); err == nil {
		if err := objectData.UnmarshalJSON(metaData); err != nil {
			file.Close()
			atomic.AddInt64(&s.metrics.ErrorCount, 1)
			return nil, nil, fmt.Errorf("failed to parse metadata: %w", err)
		}
	} else {
		info, err := file.Stat()
		if err != nil {
			file.Close()
			atomic.AddInt64(&s.metrics.ErrorCount, 1)
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

func (s *DB) PutObjectStream(bucket, key string, reader io.Reader, size int64, contentType string, metadata map[string]string, progressWriter io.Writer) (string, error) {
	start := time.Now()
	atomic.AddInt64(&s.metrics.ActiveWrites, 1)
	defer func() {
		atomic.AddInt64(&s.metrics.ActiveWrites, -1)
		atomic.AddInt64(&s.metrics.WriteOps, 1)
		latency := time.Since(start).Nanoseconds()
		atomic.StoreInt64(&s.metrics.AvgWriteLatency, latency)

		if r := recover(); r != nil {
			s.log.Errorf("Recovered from panic in PutObjectStream: %v\n%s", r, debug.Stack())
		}
	}()

	objectPath := s.getObjectPath(bucket, key)
	metadataPath := s.getObjectMetadataPath(bucket, key)

	objLock := s.getObjectLock(bucket, key)
	objLock.Lock()
	defer objLock.Unlock()

	if err := os.MkdirAll(filepath.Dir(objectPath), dirPerm); err != nil {
		atomic.AddInt64(&s.metrics.ErrorCount, 1)
		return "", fmt.Errorf("failed to create object directory: %w", err)
	}

	tmpFile := filepath.Join(s.basePath, tmpDir, fmt.Sprintf("stream_%d_%d_%s",
		time.Now().UnixNano(), os.Getpid(), filepath.Base(objectPath)))

	var file *os.File
	var err error

	if s.config.UseDirectIO && size > MB {
		file, err = openWithDirectIO(tmpFile, filePerm)
	} else {
		file, err = os.Create(tmpFile)
	}

	if err != nil {
		atomic.AddInt64(&s.metrics.ErrorCount, 1)
		return "", fmt.Errorf("failed to create temp file: %w", err)
	}

	defer func() {
		file.Close()
		if err != nil {
			os.Remove(tmpFile)
		}
	}()

	buf := s.getBuffer()
	defer s.putBuffer(buf)

	if len(buf) == 0 {
		buf = make([]byte, 64*KB)
	}

	hasher := s.getHasher()
	defer s.putHasher(hasher)

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
		atomic.AddInt64(&s.metrics.ErrorCount, 1)
		return "", fmt.Errorf("failed to write object data: %w", err)
	}

	if err = file.Sync(); err != nil {
		atomic.AddInt64(&s.metrics.ErrorCount, 1)
		return "", fmt.Errorf("failed to sync file: %w", err)
	}

	file.Close()

	etag := hex.EncodeToString(hasher.Sum(nil))

	if err = s.verifyDataIntegrity(tmpFile, etag); err != nil {
		atomic.AddInt64(&s.metrics.ErrorCount, 1)
		return "", fmt.Errorf("data integrity check failed: %w", err)
	}

	if err = os.Rename(tmpFile, objectPath); err != nil {
		atomic.AddInt64(&s.metrics.ErrorCount, 1)
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
		atomic.AddInt64(&s.metrics.ErrorCount, 1)
		return "", fmt.Errorf("failed to marshal metadata: %w", err)
	}

	if err = s.atomicWriteWithVerify(metadataPath, metaData, CalculateETag(metaData)); err != nil {
		os.Remove(objectPath)
		atomic.AddInt64(&s.metrics.ErrorCount, 1)
		return "", fmt.Errorf("failed to write metadata: %w", err)
	}

	atomic.AddInt64(&s.metrics.WriteBytes, written)
	return etag, nil
}

func (s *DB) GetObjectRange(bucket, key string, start, end int64) ([]byte, *ObjectData, error) {
	defer func() {
		if r := recover(); r != nil {
			s.log.Errorf("Recovered from panic in GetObjectRange: %v\n%s", r, debug.Stack())
		}
	}()

	atomic.AddInt64(&s.metrics.ReadOps, 1)

	objectPath := s.getObjectPath(bucket, key)
	metadataPath := s.getObjectMetadataPath(bucket, key)

	objLock := s.getObjectLock(bucket, key)
	objLock.RLock()
	defer objLock.RUnlock()

	file, err := os.Open(objectPath)
	if err != nil {
		if os.IsNotExist(err) {
			atomic.AddInt64(&s.metrics.ErrorCount, 1)
			return nil, nil, fmt.Errorf("object not found")
		}
		atomic.AddInt64(&s.metrics.ErrorCount, 1)
		return nil, nil, fmt.Errorf("failed to open object: %w", err)
	}
	defer file.Close()

	info, err := file.Stat()
	if err != nil {
		atomic.AddInt64(&s.metrics.ErrorCount, 1)
		return nil, nil, fmt.Errorf("failed to get file info: %w", err)
	}

	fileSize := info.Size()

	if start < 0 {
		start = 0
	}
	if end < 0 || end >= fileSize {
		end = fileSize - 1
	}
	if start > end {
		atomic.AddInt64(&s.metrics.ErrorCount, 1)
		return nil, nil, fmt.Errorf("invalid range")
	}

	rangeSize := end - start + 1
	data := make([]byte, rangeSize)

	_, err = file.ReadAt(data, start)
	if err != nil && err != io.EOF {
		atomic.AddInt64(&s.metrics.ErrorCount, 1)
		return nil, nil, fmt.Errorf("failed to read range: %w", err)
	}

	objectData := &ObjectData{}
	if metaData, err := s.readFileOptimized(metadataPath); err == nil {
		if err := objectData.UnmarshalJSON(metaData); err == nil {
			atomic.AddInt64(&s.metrics.ReadBytes, rangeSize)
			return data, objectData, nil
		}
	}

	objectData = &ObjectData{
		Key:          key,
		ContentType:  "application/octet-stream",
		LastModified: info.ModTime(),
		ETag:         "",
		Size:         fileSize,
	}

	atomic.AddInt64(&s.metrics.ReadBytes, rangeSize)
	return data, objectData, nil
}

func (s *DB) GetStats() (*Stats, error) {
	defer func() {
		if r := recover(); r != nil {
			s.log.Errorf("Recovered from panic in Gettats: %v\n%s", r, debug.Stack())
		}
	}()

	stats := &Stats{
		BucketCount: 0,
		ObjectCount: 0,
		TotalSize:   0,
	}

	bucketsPath := filepath.Join(s.basePath, bucketsDir)

	err := filepath.WalkDir(bucketsPath, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return nil
		}

		if d.IsDir() {
			if filepath.Dir(path) == bucketsPath {
				stats.BucketCount++
			}
			return nil
		}

		if strings.HasSuffix(path, metadataExt) {
			return nil
		}

		info, err := d.Info()
		if err != nil {
			return nil
		}

		stats.ObjectCount++
		stats.TotalSize += info.Size()

		return nil
	})

	if err != nil {
		atomic.AddInt64(&s.metrics.ErrorCount, 1)
		return nil, fmt.Errorf("failed to calculate stats: %w", err)
	}

	return stats, nil
}

func (s *DB) GetMetrics() *Metrics {
	return &Metrics{
		ReadOps:         atomic.LoadInt64(&s.metrics.ReadOps),
		WriteOps:        atomic.LoadInt64(&s.metrics.WriteOps),
		DeleteOps:       atomic.LoadInt64(&s.metrics.DeleteOps),
		ListOps:         atomic.LoadInt64(&s.metrics.ListOps),
		ReadBytes:       atomic.LoadInt64(&s.metrics.ReadBytes),
		WriteBytes:      atomic.LoadInt64(&s.metrics.WriteBytes),
		ErrorCount:      atomic.LoadInt64(&s.metrics.ErrorCount),
		AvgReadLatency:  atomic.LoadInt64(&s.metrics.AvgReadLatency),
		AvgWriteLatency: atomic.LoadInt64(&s.metrics.AvgWriteLatency),
		ActiveReads:     atomic.LoadInt64(&s.metrics.ActiveReads),
		ActiveWrites:    atomic.LoadInt64(&s.metrics.ActiveWrites),
	}
}

func (s *DB) HealthCheck() error {
	if _, err := os.Stat(s.basePath); err != nil {
		return fmt.Errorf("base path not accessible: %w", err)
	}

	var stat unix.Statfs_t
	if err := unix.Statfs(s.basePath, &stat); err != nil {
		return fmt.Errorf("failed to get disk stats: %w", err)
	}

	freeSpace := stat.Bavail * uint64(stat.Bsize)
	totalSpace := stat.Blocks * uint64(stat.Bsize)
	usagePercent := float64(totalSpace-freeSpace) / float64(totalSpace) * 100

	if usagePercent > 95 {
		return fmt.Errorf("disk usage too high: %.2f%%", usagePercent)
	}

	metrics := s.GetMetrics()
	totalOps := metrics.ReadOps + metrics.WriteOps + metrics.DeleteOps + metrics.ListOps
	if totalOps > 0 {
		errorRate := float64(metrics.ErrorCount) / float64(totalOps) * 100
		if errorRate > 5 {
			return fmt.Errorf("error rate too high: %.2f%%", errorRate)
		}
	}

	return nil
}

func (s *DB) ResetMetrics() {
	atomic.StoreInt64(&s.metrics.ReadOps, 0)
	atomic.StoreInt64(&s.metrics.WriteOps, 0)
	atomic.StoreInt64(&s.metrics.DeleteOps, 0)
	atomic.StoreInt64(&s.metrics.ListOps, 0)
	atomic.StoreInt64(&s.metrics.ReadBytes, 0)
	atomic.StoreInt64(&s.metrics.WriteBytes, 0)
	atomic.StoreInt64(&s.metrics.ErrorCount, 0)
}

func (s *DB) GetDiskUsage() (total, free, used uint64, err error) {
	var stat unix.Statfs_t
	if err = unix.Statfs(s.basePath, &stat); err != nil {
		return 0, 0, 0, fmt.Errorf("failed to get disk stats: %w", err)
	}

	total = stat.Blocks * uint64(stat.Bsize)
	free = stat.Bavail * uint64(stat.Bsize)
	used = total - free

	return total, free, used, nil
}

func (s *DB) SetConfig(config *Config) {
	if config == nil {
		return
	}

	s.config = config

	s.bufferPool = sync.Pool{
		New: func() interface{} {
			return make([]byte, s.config.BufferSize)
		},
	}
}

func (s *DB) GetConfig() *Config {
	return s.config
}

func (s *DB) Compact() error {
	s.log.Info("Starting compaction")

	if err := s.cleanupEmptyDirectories(); err != nil {
		s.log.Error("Failed to cleanup empty directories during compaction: ", err)
	}

	if err := s.cleanupTempFiles(); err != nil {
		s.log.Error("Failed to cleanup temp files during compaction: ", err)
	}

	if err := s.cleanupExpiredUploads(); err != nil {
		s.log.Error("Failed to cleanup expired uploads during compaction: ", err)
	}

	s.cleanupLockMaps()

	s.log.Info("compaction completed")
	return nil
}

func (s *DB) Backup(backupPath string) error {
	s.log.Info("Starting backup to: ", backupPath)

	if err := os.MkdirAll(backupPath, dirPerm); err != nil {
		return fmt.Errorf("failed to create backup directory: %w", err)
	}

	return s.copyDir(s.basePath, backupPath)
}

func (s *DB) copyDir(src, dst string) error {
	return filepath.WalkDir(src, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		relPath, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}

		dstPath := filepath.Join(dst, relPath)

		if d.IsDir() {
			return os.MkdirAll(dstPath, dirPerm)
		}

		srcFile, err := os.Open(path)
		if err != nil {
			return err
		}
		defer srcFile.Close()

		if err := os.MkdirAll(filepath.Dir(dstPath), dirPerm); err != nil {
			return err
		}

		dstFile, err := os.Create(dstPath)
		if err != nil {
			return err
		}
		defer dstFile.Close()

		buf := s.getBuffer()
		defer s.putBuffer(buf)

		if len(buf) == 0 {
			buf = make([]byte, 64*KB)
		}

		_, err = io.CopyBuffer(dstFile, srcFile, buf)
		return err
	})
}

func (s *DB) Restore(backupPath string) error {
	s.log.Info("Starting restore from: ", backupPath)

	if _, err := os.Stat(backupPath); os.IsNotExist(err) {
		return fmt.Errorf("backup directory does not exist: %s", backupPath)
	}

	s.cancel()
	if s.cleanupTicker != nil {
		s.cleanupTicker.Stop()
	}

	if err := os.RemoveAll(s.basePath); err != nil {
		return fmt.Errorf("failed to remove current  %w", err)
	}

	if err := s.copyDir(backupPath, s.basePath); err != nil {
		return fmt.Errorf("failed to restore from backup: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	s.ctx = ctx
	s.cancel = cancel
	s.startOptimizedCleanupRoutine()

	s.log.Info("restore completed")
	return nil
}

func (s *DB) Validate() error {
	s.log.Info("Starting validation")

	var errors []string
	bucketsPath := filepath.Join(s.basePath, bucketsDir)

	err := filepath.WalkDir(bucketsPath, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			errors = append(errors, fmt.Sprintf("Walk error: %v", err))
			return nil
		}

		if d.IsDir() || strings.HasSuffix(path, metadataExt) {
			return nil
		}

		metadataPath := path + metadataExt
		if _, err := os.Stat(metadataPath); os.IsNotExist(err) {
			errors = append(errors, fmt.Sprintf("Missing metadata for: %s", path))
			return nil
		}

		metaData, err := s.readFileOptimized(metadataPath)
		if err != nil {
			errors = append(errors, fmt.Sprintf("Failed to read metadata for %s: %v", path, err))
			return nil
		}

		objectData := &ObjectData{}
		if err := objectData.UnmarshalJSON(metaData); err != nil {
			errors = append(errors, fmt.Sprintf("Failed to parse metadata for %s: %v", path, err))
			return nil
		}

		if err := s.verifyDataIntegrity(path, objectData.ETag); err != nil {
			errors = append(errors, fmt.Sprintf("Data integrity check failed for %s: %v", path, err))
		}

		return nil
	})

	if err != nil {
		errors = append(errors, fmt.Sprintf("Validation walk error: %v", err))
	}

	if len(errors) > 0 {
		s.log.Error("validation found errors:")
		for _, e := range errors {
			s.log.Error("  ", e)
		}
		return fmt.Errorf("validation failed with %d errors", len(errors))
	}

	s.log.Info("validation completed successfully")
	return nil
}

func (s *DB) RenameObject(bucket, oldKey, newKey string) error {
	start := time.Now()
	atomic.AddInt64(&s.metrics.ActiveWrites, 1)
	defer func() {
		atomic.AddInt64(&s.metrics.ActiveWrites, -1)
		atomic.AddInt64(&s.metrics.WriteOps, 1)
		latency := time.Since(start).Nanoseconds()
		atomic.StoreInt64(&s.metrics.AvgWriteLatency, latency)

		if r := recover(); r != nil {
			s.log.Errorf("Recovered from panic in RenameObject: %v\n%s", r, debug.Stack())
		}
	}()

	if bucket == "" || oldKey == "" || newKey == "" {
		atomic.AddInt64(&s.metrics.ErrorCount, 1)
		return fmt.Errorf("bucket, oldKey and newKey cannot be empty")
	}

	if oldKey == newKey {
		return nil
	}

	exists, err := s.BucketExists(bucket)
	if err != nil {
		atomic.AddInt64(&s.metrics.ErrorCount, 1)
		return fmt.Errorf("failed to check bucket existence: %w", err)
	}
	if !exists {
		atomic.AddInt64(&s.metrics.ErrorCount, 1)
		return fmt.Errorf("bucket %s does not exist", bucket)
	}

	oldObjectPath := s.getObjectPath(bucket, oldKey)
	oldMetadataPath := s.getObjectMetadataPath(bucket, oldKey)
	newObjectPath := s.getObjectPath(bucket, newKey)
	newMetadataPath := s.getObjectMetadataPath(bucket, newKey)

	var firstLock, secondLock *sync.RWMutex
	var firstKey, secondKey string

	if oldKey < newKey {
		firstLock = s.getObjectLock(bucket, oldKey)
		secondLock = s.getObjectLock(bucket, newKey)
		firstKey, secondKey = oldKey, newKey
	} else {
		firstLock = s.getObjectLock(bucket, newKey)
		secondLock = s.getObjectLock(bucket, oldKey)
		firstKey, secondKey = newKey, oldKey
	}

	firstLock.Lock()
	defer firstLock.Unlock()

	if firstKey != secondKey {
		secondLock.Lock()
		defer secondLock.Unlock()
	}

	if _, err := os.Stat(oldObjectPath); os.IsNotExist(err) {
		atomic.AddInt64(&s.metrics.ErrorCount, 1)
		return fmt.Errorf("source object %s does not exist", oldKey)
	}

	if _, err := os.Stat(newObjectPath); err == nil {
		atomic.AddInt64(&s.metrics.ErrorCount, 1)
		return fmt.Errorf("destination object %s already exists", newKey)
	}

	if err := os.MkdirAll(filepath.Dir(newObjectPath), dirPerm); err != nil {
		atomic.AddInt64(&s.metrics.ErrorCount, 1)
		return fmt.Errorf("failed to create destination directory: %w", err)
	}

	if err := os.Rename(oldObjectPath, newObjectPath); err != nil {
		atomic.AddInt64(&s.metrics.ErrorCount, 1)
		return fmt.Errorf("failed to rename object file: %w", err)
	}

	if _, err := os.Stat(oldMetadataPath); err == nil {
		if err := os.MkdirAll(filepath.Dir(newMetadataPath), dirPerm); err != nil {
			os.Rename(newObjectPath, oldObjectPath)
			atomic.AddInt64(&s.metrics.ErrorCount, 1)
			return fmt.Errorf("failed to create new metadata directory: %w", err)
		}

		metaData, err := s.readFileOptimized(oldMetadataPath)
		if err != nil {
			os.Rename(newObjectPath, oldObjectPath)
			atomic.AddInt64(&s.metrics.ErrorCount, 1)
			return fmt.Errorf("failed to read old metadata: %w", err)
		}

		objectData := &ObjectData{}
		if err := objectData.UnmarshalJSON(metaData); err != nil {
			os.Rename(newObjectPath, oldObjectPath)
			atomic.AddInt64(&s.metrics.ErrorCount, 1)
			return fmt.Errorf("failed to parse metadata: %w", err)
		}

		objectData.Key = newKey
		objectData.LastModified = time.Now()

		newMetaData, err := objectData.MarshalJSON()
		if err != nil {
			os.Rename(newObjectPath, oldObjectPath)
			atomic.AddInt64(&s.metrics.ErrorCount, 1)
			return fmt.Errorf("failed to marshal new metadata: %w", err)
		}

		err = s.withRetry(func() error {
			return s.atomicWriteWithVerify(newMetadataPath, newMetaData, CalculateETag(newMetaData))
		})

		if err != nil {
			os.Rename(newObjectPath, oldObjectPath)
			atomic.AddInt64(&s.metrics.ErrorCount, 1)
			return fmt.Errorf("failed to write new metadata: %w", err)
		}

		os.Remove(oldMetadataPath)
	}

	s.cleanupEmptyDirPath(filepath.Dir(oldObjectPath))

	s.log.Infof("Successfully renamed object from %s to %s in bucket %s", oldKey, newKey, bucket)
	return nil
}

func (s *DB) cleanupEmptyDirPath(dirPath string) {
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
