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

	// 文件扩展名
	metadataExt  = ".json"
	partExt      = ".part"
	uploadConfig = "upload.json"

	// 分段上传相关
	maxRetries = 3
	retryDelay = 100 * time.Millisecond

	// 文件权限
	dirPerm  = 0755
	filePerm = 0644

	md5sum = "md5"
)

const (
	// 分段上传相关常量
	MaxMultipartLifetime = 7 * 24 * time.Hour // 7天，与S3一致
	CleanupInterval      = 1 * time.Hour      // 清理过期上传的间隔
)

// 存储配置
type Config struct {
	// 并发控制
	MaxConcurrentUploads   int
	MaxConcurrentDownloads int

	// 缓存配置
	MetadataCacheSize int
	MetadataCacheTTL  time.Duration

	// 性能优化
	UseDirectIO bool
	UseMmap     bool
	BufferSize  int

	// 数据完整性
	EnableChecksumVerify bool
	ChecksumAlgorithm    string

	// 清理配置
	CleanupInterval time.Duration
	TempFileMaxAge  time.Duration
}

// 性能指标
type Metrics struct {
	// 操作计数
	ReadOps   int64
	WriteOps  int64
	DeleteOps int64
	ListOps   int64

	// 字节统计
	ReadBytes  int64
	WriteBytes int64

	// 错误统计
	ErrorCount int64

	// 性能统计
	AvgReadLatency  int64 // 纳秒
	AvgWriteLatency int64 // 纳秒

	// 并发统计
	ActiveReads  int64
	ActiveWrites int64
}

// 实现了基于文件系统的S3兼容存储
type DB struct {
	basePath string

	// 细粒度锁控制
	bucketLocks sync.Map // map[string]*sync.RWMutex - 每个bucket一个锁
	uploadLocks sync.Map // map[string]*sync.RWMutex - 每个upload一个锁
	objectLocks sync.Map // map[string]*sync.RWMutex - 热点对象锁

	// 性能监控
	metrics *Metrics

	// 资源池
	bufferPool sync.Pool
	hasherPool sync.Pool

	// 后台任务控制
	cleanupTicker *time.Ticker
	cleanupDone   chan struct{}
	ctx           context.Context
	cancel        context.CancelFunc

	// 配置参数
	config *Config

	// 日志记录器
	log Logger
}

// 默认配置
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

// New创建一个新的文件系统存储实例
func NewWithLogger(path string, log Logger) (*DB, error) {
	if log == nil {
		return nil, fmt.Errorf("logger is required")
	}
	// 确保基础目录存在
	if err := os.MkdirAll(path, dirPerm); err != nil {
		return nil, fmt.Errorf("failed to create base directory: %w", err)
	}

	// 创建必要的系统目录
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

	// 确保配置有效
	if s.config.BufferSize <= 0 {
		s.config.BufferSize = 64 * KB // 64KB 默认
	}

	// 初始化资源池
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

	// 启动后台清理过程
	s.startOptimizedCleanupRoutine()

	return s, nil
}

func New(path string) (*DB, error) {
	return NewWithLogger(path, &nopLogger{})
}

// 锁管理方法
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

// 资源池管理
func (s *DB) getBuffer() []byte {
	if buf := s.bufferPool.Get(); buf != nil {
		buffer := buf.([]byte)
		// 确保缓冲区有正确的长度和容量
		if cap(buffer) >= s.config.BufferSize && s.config.BufferSize > 0 {
			return buffer[:s.config.BufferSize] // 重置长度为配置的大小
		}
	}
	// 如果池中没有合适的缓冲区，创建新的
	if s.config.BufferSize <= 0 {
		s.config.BufferSize = 64 * KB // 64KB 默认
	}
	return make([]byte, s.config.BufferSize)
}

func (s *DB) putBuffer(buf []byte) {
	if buf != nil && cap(buf) >= s.config.BufferSize && s.config.BufferSize > 0 {
		// 重置缓冲区内容并放回池中
		clear(buf) // Go 1.21+ 可以使用 clear，否则用下面的循环
		// for i := range buf {
		//     buf[i] = 0
		// }
		s.bufferPool.Put(buf[:0]) // 重置长度为0
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

// Close 关闭存储并清理资源
func (s *DB) Close() error {
	defer func() {
		if r := recover(); r != nil {
			s.log.Errorf("Recovered from panic in Close: %v\n%s", r, debug.Stack())
		}
	}()

	// 取消上下文
	s.cancel()

	// 停止清理例程
	if s.cleanupTicker != nil {
		s.cleanupTicker.Stop()
		close(s.cleanupDone)
	}

	return nil
}

// 数据完整性验证
func (s *DB) verifyDataIntegrity(path string, expectedChecksum string) error {
	if !s.config.EnableChecksumVerify || expectedChecksum == "" {
		return nil
	}

	// 清理和标准化校验和字符串
	expectedChecksum = strings.TrimSpace(strings.Trim(expectedChecksum, `"`))

	// 检查是否是多段上传的ETag (格式: hash-partCount)
	if strings.Contains(expectedChecksum, "-") {
		parts := strings.Split(expectedChecksum, "-")
		if len(parts) == 2 {
			if partCount, err := strconv.Atoi(parts[1]); err == nil && partCount > 1 {
				// 这是多段上传的ETag，不能直接验证文件MD5
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

// 带校验的原子写入
func (s *DB) atomicWriteWithVerify(filePath string, data []byte, expectedChecksum string) error {
	dir := filepath.Dir(filePath)
	if err := os.MkdirAll(dir, dirPerm); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	// 生成临时文件名
	tmpFile := filepath.Join(s.basePath, tmpDir, fmt.Sprintf("tmp_%d_%d_%s",
		time.Now().UnixNano(), os.Getpid(), filepath.Base(filePath)))

	// 写入临时文件
	var file *os.File
	var err error

	if s.config.UseDirectIO {
		// 使用直接IO避免页缓存
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

	// 写入数据
	if _, err = file.Write(data); err != nil {
		return fmt.Errorf("failed to write temp file: %w", err)
	}

	// 强制刷盘
	if err = file.Sync(); err != nil {
		return fmt.Errorf("failed to sync temp file: %w", err)
	}

	file.Close()

	// 验证数据完整性
	if err = s.verifyDataIntegrity(tmpFile, expectedChecksum); err != nil {
		return fmt.Errorf("data integrity check failed: %w", err)
	}

	// 原子移动到目标位置
	if err = os.Rename(tmpFile, filePath); err != nil {
		return fmt.Errorf("failed to move temp file: %w", err)
	}

	return nil
}

// 原子写入文件（保持向后兼容）
func (s *DB) atomicWrite(filePath string, data []byte) error {
	checksum := CalculateETag(data)
	return s.atomicWriteWithVerify(filePath, data, checksum)
}

// 优化的文件读取
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

	// 确保缓冲区不为空
	if len(buf) == 0 {
		buf = make([]byte, 64*KB) // 64KB 默认缓冲区
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

// 内存映射文件读取
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

	// 使用mmap
	data, err := unix.Mmap(int(file.Fd()), 0, size, unix.PROT_READ, unix.MAP_SHARED)
	if err != nil {
		return nil, fmt.Errorf("mmap failed: %w", err)
	}
	defer unix.Munmap(data)

	// 复制数据（因为mmap的数据在munmap后无效）
	result := make([]byte, size)
	copy(result, data)

	return result, nil
}

// 优化的清理机制
func (s *DB) startOptimizedCleanupRoutine() {
	s.cleanupTicker = time.NewTicker(s.config.CleanupInterval)

	go func() {
		defer func() {
			if r := recover(); r != nil {
				s.log.Errorf("Recovered from panic in cleanup routine: %v\n%s", r, debug.Stack())
				// 重启清理例程
				time.Sleep(time.Minute)
				s.startOptimizedCleanupRoutine()
			}
		}()

		for {
			select {
			case <-s.cleanupTicker.C:
				// 并发执行清理任务
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

// cleanupExpiredUploads 清理过期的分段上传
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
			return nil // 忽略错误，继续处理
		}

		if !d.IsDir() || path == multipartPath {
			return nil
		}

		// 检查上传信息文件
		infoFile := filepath.Join(path, uploadConfig)
		if _, err := os.Stat(infoFile); os.IsNotExist(err) {
			return nil
		}

		// 读取上传信息
		data, err := s.readFileOptimized(infoFile)
		if err != nil {
			return nil
		}

		uploadInfo := &MultipartUploadInfo{}
		if err := uploadInfo.UnmarshalJSON(data); err != nil {
			return nil
		}

		// 检查是否过期
		if now.Sub(uploadInfo.CreatedAt) > MaxMultipartLifetime {
			s.log.Info("Cleaning up expired multipart upload: ", uploadInfo.UploadID)

			// 安全删除整个上传目录
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

// cleanupTempFiles 清理临时文件
func (s *DB) cleanupTempFiles() error {
	tmpPath := filepath.Join(s.basePath, tmpDir)
	now := time.Now()

	return filepath.WalkDir(tmpPath, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}

		// 删除超过配置时间的临时文件
		if info, err := d.Info(); err == nil {
			if now.Sub(info.ModTime()) > s.config.TempFileMaxAge {
				os.Remove(path)
			}
		}

		return nil
	})
}

// 清理空目录
func (s *DB) cleanupEmptyDirectories() error {
	bucketsPath := filepath.Join(s.basePath, bucketsDir)

	return filepath.WalkDir(bucketsPath, func(path string, d fs.DirEntry, err error) error {
		if err != nil || !d.IsDir() || path == bucketsPath {
			return nil
		}

		// 检查目录是否为空
		entries, err := os.ReadDir(path)
		if err != nil {
			return nil
		}

		if len(entries) == 0 {
			// 确保不是bucket根目录
			if filepath.Dir(path) != bucketsPath {
				os.Remove(path)
			}
		}

		return nil
	})
}

// 清理锁映射中的无用条目
func (s *DB) cleanupLockMaps() {
	// 定期清理锁映射，避免内存泄漏
	// 这里可以实现LRU策略或基于时间的清理

	// 示例：清理超过1小时未使用的锁
	s.bucketLocks.Range(func(key, value interface{}) bool {
		// 这里需要添加时间戳跟踪机制
		// 简化示例，实际实现需要更复杂的逻辑
		return true
	})
}

// withRetry 重试机制包装函数
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

// Bucket 相关操作
// ListBuckets 列出所有存储桶
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

// CreateBucket 创建存储桶
func (s *DB) CreateBucket(bucket string) error {
	bucketLock := s.getBucketLock(bucket)
	bucketLock.Lock()
	defer bucketLock.Unlock()

	bucketPath := filepath.Join(s.basePath, bucketsDir, bucket)

	// 检查是否已存在
	if _, err := os.Stat(bucketPath); err == nil {
		return fmt.Errorf("bucket already exists")
	}

	// 创建存储桶目录
	err := s.withRetry(func() error {
		return os.MkdirAll(bucketPath, dirPerm)
	})
	if err != nil {
		atomic.AddInt64(&s.metrics.ErrorCount, 1)
		return fmt.Errorf("failed to create bucket directory: %w", err)
	}

	// 创建存储桶元数据
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

// DeleteBucket 删除存储桶
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

	// 检查存储桶是否存在
	if _, err := os.Stat(bucketPath); os.IsNotExist(err) {
		atomic.AddInt64(&s.metrics.ErrorCount, 1)
		return fmt.Errorf("bucket not found")
	}

	// 检查存储桶是否为空
	entries, err := os.ReadDir(bucketPath)
	if err != nil {
		atomic.AddInt64(&s.metrics.ErrorCount, 1)
		return fmt.Errorf("failed to read bucket directory: %w", err)
	}

	if len(entries) > 0 {
		atomic.AddInt64(&s.metrics.ErrorCount, 1)
		return fmt.Errorf("bucket not empty")
	}

	// 删除存储桶目录
	if err := os.RemoveAll(bucketPath); err != nil {
		atomic.AddInt64(&s.metrics.ErrorCount, 1)
		return fmt.Errorf("failed to remove bucket directory: %w", err)
	}

	// 删除存储桶元数据
	metadataPath := filepath.Join(s.basePath, metadataDir, bucket+metadataExt)
	os.Remove(metadataPath) // 忽略错误

	return nil
}

// BucketExists 检查存储桶是否存在
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

// Object 相关操作

// getObjectPath 获取对象的文件路径
func (s *DB) getObjectPath(bucket, key string) string {
	// 使用哈希分片来避免单个目录下文件过多
	hash := md5.Sum([]byte(key))
	hashStr := hex.EncodeToString(hash[:])

	// 创建两级目录结构：前2位/后2位/文件名
	return filepath.Join(s.basePath, bucketsDir, bucket,
		hashStr[:2], hashStr[2:4], key)
}

// getObjectMetadataPath 获取对象元数据路径
func (s *DB) getObjectMetadataPath(bucket, key string) string {
	return s.getObjectPath(bucket, key) + metadataExt
}

// ObjectExists 检查对象是否存在
func (s *DB) ObjectExists(bucket, key string) (bool, error) {
	start := time.Now()
	atomic.AddInt64(&s.metrics.ActiveReads, 1)
	defer func() {
		atomic.AddInt64(&s.metrics.ActiveReads, -1)
		latency := time.Since(start).Nanoseconds()
		// 更新平均延迟（可选，因为这是轻量级操作）
		atomic.StoreInt64(&s.metrics.AvgReadLatency, latency)

		if r := recover(); r != nil {
			s.log.Errorf("Recovered from panic in ObjectExists: %v\n%s", r, debug.Stack())
		}
	}()

	// 首先检查桶是否存在
	exists, err := s.BucketExists(bucket)
	if err != nil {
		return false, fmt.Errorf("failed to check bucket existence: %w", err)
	}
	if !exists {
		return false, nil
	}

	objectPath := s.getObjectPath(bucket, key)
	
	// 获取读锁（轻量级操作，使用读锁即可）
	objLock := s.getObjectLock(bucket, key)
	objLock.RLock()
	defer objLock.RUnlock()

	// 检查对象文件是否存在
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

// ListObjects 列出存储桶中的对象
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

		// 跳过元数据文件
		if strings.HasSuffix(path, metadataExt) {
			return nil
		}

		// 计算相对于存储桶的对象键
		relPath, err := filepath.Rel(bucketPath, path)
		if err != nil {
			return nil
		}

		// 从路径中提取原始对象键
		// 路径格式：xx/yy/object_key，需要提取object_key部分
		parts := strings.Split(filepath.ToSlash(relPath), "/")
		if len(parts) < 3 {
			return nil
		}
		objKey := strings.Join(parts[2:], "/")

		// 应用前缀过滤
		if prefix != "" && !strings.HasPrefix(objKey, prefix) {
			return nil
		}

		// 应用标记过滤
		if marker != "" && objKey <= marker {
			return nil
		}

		// 处理分隔符
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

		// 读取对象信息
		info, err := d.Info()
		if err != nil {
			return nil
		}

		// 读取元数据获取ETag
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

	// 排序并限制结果
	sort.Slice(objects, func(i, j int) bool {
		return objects[i].Key < objects[j].Key
	})

	if len(objects) > maxKeys {
		objects = objects[:maxKeys]
	}

	sort.Strings(commonPrefixes)

	return objects, commonPrefixes, nil
}

// GetObject 获取对象（优化版本）
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

	// 获取读锁
	objLock := s.getObjectLock(bucket, key)
	objLock.RLock()
	defer objLock.RUnlock()

	// 先检查对象文件是否存在
	if _, err := os.Stat(objectPath); os.IsNotExist(err) {
		return nil, fmt.Errorf("object not found")
	}

	// 先读取元数据
	metaData, err := s.readFileOptimized(metadataPath)
	if err != nil {
		// 如果元数据不存在，尝试直接读取对象文件
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

		// 如果没有元数据，创建基本信息
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

	// 读取对象数据
	var data []byte
	if s.config.UseMmap && objectData.Size > 0 && objectData.Size < 100*MB { // 小于100MB使用mmap
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

	// 验证数据完整性
	if err = s.verifyDataIntegrity(objectPath, objectData.ETag); err != nil {
		atomic.AddInt64(&s.metrics.ErrorCount, 1)
		return nil, fmt.Errorf("data integrity check failed: %w", err)
	}

	objectData.Data = data
	atomic.AddInt64(&s.metrics.ReadBytes, int64(len(data)))

	return objectData, nil
}

// PutObject 存储对象（优化版本）
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

	// 获取对象锁
	objLock := s.getObjectLock(bucket, object.Key)
	objLock.Lock()
	defer objLock.Unlock()

	// 确保目录存在
	if err := os.MkdirAll(filepath.Dir(objectPath), dirPerm); err != nil {
		atomic.AddInt64(&s.metrics.ErrorCount, 1)
		return fmt.Errorf("failed to create object directory: %w", err)
	}
	// 计算ETag如果没有提供
	if object.ETag == "" {
		object.ETag = CalculateETag(object.Data)
	}

	// 检查是否是多段上传的ETag
	var err error
	isMultipart := strings.Contains(object.ETag, "-")
	if isMultipart {
		// 多段上传，不验证
		err = s.withRetry(func() error {
			return s.atomicWriteWithoutVerify(objectPath, object.Data)
		})
	} else {
		// 单文件上传，可以验证
		err = s.withRetry(func() error {
			return s.atomicWriteWithVerify(objectPath, object.Data, object.ETag)
		})
	}

	if err != nil {
		atomic.AddInt64(&s.metrics.ErrorCount, 1)
		return fmt.Errorf("failed to write object data: %w", err)
	}

	// 写入元数据
	metadata := *object
	metadata.Data = nil // 不在元数据中存储实际数据
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
		// 如果元数据写入失败，清理对象文件
		os.Remove(objectPath)
		atomic.AddInt64(&s.metrics.ErrorCount, 1)
		return fmt.Errorf("failed to write metadata: %w", err)
	}

	atomic.AddInt64(&s.metrics.WriteBytes, int64(len(object.Data)))
	return nil
}

// DeleteObject 删除对象
func (s *DB) DeleteObject(bucket, key string) error {
	defer func() {
		if r := recover(); r != nil {
			s.log.Errorf("Recovered from panic in DeleteObject: %v\n%s", r, debug.Stack())
		}
	}()

	atomic.AddInt64(&s.metrics.DeleteOps, 1)

	objectPath := s.getObjectPath(bucket, key)
	metadataPath := s.getObjectMetadataPath(bucket, key)

	// 获取对象锁
	objLock := s.getObjectLock(bucket, key)
	objLock.Lock()
	defer objLock.Unlock()

	// 检查对象是否存在
	if _, err := os.Stat(objectPath); os.IsNotExist(err) {
		atomic.AddInt64(&s.metrics.ErrorCount, 1)
		return fmt.Errorf("object not found")
	}

	// 删除对象文件
	if err := os.Remove(objectPath); err != nil {
		atomic.AddInt64(&s.metrics.ErrorCount, 1)
		return fmt.Errorf("failed to delete object: %w", err)
	}

	// 删除元数据文件
	os.Remove(metadataPath) // 忽略错误

	// 尝试清理空目录
	dir := filepath.Dir(objectPath)
	for i := 0; i < 2; i++ { // 最多清理两级目录
		if entries, err := os.ReadDir(dir); err == nil && len(entries) == 0 {
			os.Remove(dir)
			dir = filepath.Dir(dir)
		} else {
			break
		}
	}

	return nil
}

// Multipart Upload 相关操作

// getMultipartPath 获取分段上传的基础路径
func (s *DB) getMultipartPath(uploadID string) string {
	return filepath.Join(s.basePath, multipartDir, uploadID)
}

// CreateMultipartUpload 创建分段上传
func (s *DB) CreateMultipartUpload(bucket, key, contentType string, metadata map[string]string) (string, error) {
	uploadLock := s.getUploadLock("create_" + bucket + "_" + key)
	uploadLock.Lock()
	defer uploadLock.Unlock()

	s.log.Info("Creating multipart upload for ", key, " in bucket ", bucket)

	// 检查桶是否存在
	exists, err := s.BucketExists(bucket)
	if err != nil {
		atomic.AddInt64(&s.metrics.ErrorCount, 1)
		return "", fmt.Errorf("failed to check bucket existence: %w", err)
	}
	if !exists {
		atomic.AddInt64(&s.metrics.ErrorCount, 1)
		return "", fmt.Errorf("bucket does not exist: %s", bucket)
	}

	// 生成上传ID
	uploadID := GenerateUploadID(bucket, key)

	// 创建上传目录
	uploadPath := s.getMultipartPath(uploadID)
	if err := os.MkdirAll(uploadPath, dirPerm); err != nil {
		atomic.AddInt64(&s.metrics.ErrorCount, 1)
		return "", fmt.Errorf("failed to create upload directory: %w", err)
	}

	// 创建上传信息
	uploadInfo := &MultipartUploadInfo{
		Bucket:      bucket,
		Key:         key,
		UploadID:    uploadID,
		ContentType: contentType,
		Metadata:    metadata,
		CreatedAt:   time.Now(),
	}

	// 保存上传信息
	infoPath := filepath.Join(uploadPath, uploadConfig)
	data, err := uploadInfo.MarshalJSON()
	if err != nil {
		atomic.AddInt64(&s.metrics.ErrorCount, 1)
		return "", fmt.Errorf("failed to marshal upload info: %w", err)
	}

	if err := os.WriteFile(infoPath, data, filePerm); err != nil {
		os.RemoveAll(uploadPath) // 清理
		atomic.AddInt64(&s.metrics.ErrorCount, 1)
		return "", fmt.Errorf("failed to write upload info: %w", err)
	}

	return uploadID, nil
}

// UploadPart 上传分段（优化版本）
func (s *DB) UploadPart(bucket, key, uploadID string, partNumber int, data []byte) (string, error) {
	if partNumber < 1 || partNumber > 10000 {
		return "", fmt.Errorf("invalid part number: must be between 1 and 10000")
	}

	// 获取上传锁
	uploadLock := s.getUploadLock(uploadID)
	uploadLock.Lock()
	defer uploadLock.Unlock()

	s.log.Info("Uploading part ", partNumber, " for ", key, " in bucket ", bucket, " (size: ", len(data), " bytes)")

	uploadPath := s.getMultipartPath(uploadID)

	// 验证上传是否存在
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

	// 验证桶和键
	if uploadInfo.Bucket != bucket || uploadInfo.Key != key {
		atomic.AddInt64(&s.metrics.ErrorCount, 1)
		return "", fmt.Errorf("bucket or key mismatch")
	}

	// 计算ETag
	hasher := s.getHasher()
	defer s.putHasher(hasher)

	hasher.Write(data)
	etag := hex.EncodeToString(hasher.Sum(nil))

	// 保存分段数据
	partPath := filepath.Join(uploadPath, fmt.Sprintf("part.%05d", partNumber))
	err = s.withRetry(func() error {
		return s.atomicWriteWithVerify(partPath, data, etag)
	})
	if err != nil {
		atomic.AddInt64(&s.metrics.ErrorCount, 1)
		return "", fmt.Errorf("failed to write part data: %w", err)
	}

	// 保存分段信息
	partInfo := &PartInfo{
		PartNumber:   partNumber,
		ETag:         etag,
		Size:         len(data),
		LastModified: time.Now(),
	}

	partInfoPath := filepath.Join(uploadPath, fmt.Sprintf("part.%05d.json", partNumber))
	partInfoData, err := partInfo.MarshalJSON()
	if err != nil {
		os.Remove(partPath) // 清理分段数据
		atomic.AddInt64(&s.metrics.ErrorCount, 1)
		return "", fmt.Errorf("failed to marshal part info: %w", err)
	}

	err = s.withRetry(func() error {
		return s.atomicWriteWithVerify(partInfoPath, partInfoData, CalculateETag(partInfoData))
	})
	if err != nil {
		os.Remove(partPath) // 清理分段数据
		atomic.AddInt64(&s.metrics.ErrorCount, 1)
		return "", fmt.Errorf("failed to write part info: %w", err)
	}

	atomic.AddInt64(&s.metrics.WriteBytes, int64(len(data)))
	return etag, nil
}

// CompleteMultipartUpload 完成分段上传（优化版本）
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

	// 读取上传信息
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

	// 验证桶和键
	if uploadInfo.Bucket != bucket || uploadInfo.Key != key {
		atomic.AddInt64(&s.metrics.ErrorCount, 1)
		return "", fmt.Errorf("bucket or key mismatch")
	}

	// 按分段号排序
	sortedParts := make([]MultipartPart, len(parts))
	copy(sortedParts, parts)
	sort.Slice(sortedParts, func(i, j int) bool {
		return sortedParts[i].PartNumber < sortedParts[j].PartNumber
	})

	// 创建临时文件用于合并
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

	// 确保缓冲区不为空
	if len(buf) == 0 {
		buf = make([]byte, 64*KB) // 64KB 默认缓冲区
	}

	// 验证并合并所有分段
	for _, part := range sortedParts {
		partPath := filepath.Join(uploadPath, fmt.Sprintf("part.%05d", part.PartNumber))
		partInfoPath := filepath.Join(uploadPath, fmt.Sprintf("part.%05d.json", part.PartNumber))

		// 验证分段信息
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

		// 验证ETag
		if partInfo.ETag != part.ETag {
			atomic.AddInt64(&s.metrics.ErrorCount, 1)
			return "", fmt.Errorf("ETag mismatch for part %d: expected %s, got %s",
				part.PartNumber, partInfo.ETag, part.ETag)
		}

		// 读取并写入分段数据
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

	// 强制刷盘
	if err = file.Sync(); err != nil {
		atomic.AddInt64(&s.metrics.ErrorCount, 1)
		return "", fmt.Errorf("failed to sync merged file: %w", err)
	}

	// 关闭临时文件
	file.Close()

	// 读取合并后的数据
	finalData, err := s.readFileOptimized(tmpFile)
	if err != nil {
		atomic.AddInt64(&s.metrics.ErrorCount, 1)
		return "", fmt.Errorf("failed to read merged data: %w", err)
	}

	// 生成最终ETag（多段上传格式）
	finalETag := CalculateMultipartETag(allETags)

	// 存储最终对象 - 使用专门的方法，不进行完整性验证
	objectData := &ObjectData{
		Key:          key,
		Data:         finalData,
		ContentType:  uploadInfo.ContentType,
		LastModified: time.Now(),
		ETag:         finalETag,
		Metadata:     uploadInfo.Metadata,
		Size:         int64(len(finalData)),
	}

	// 直接存储，不验证多段上传的ETag
	if err := s.putObjectWithoutVerification(bucket, objectData); err != nil {
		atomic.AddInt64(&s.metrics.ErrorCount, 1)
		return "", fmt.Errorf("failed to store final object: %w", err)
	}

	// 清理分段上传数据
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

	// 获取对象锁
	objLock := s.getObjectLock(bucket, object.Key)
	objLock.Lock()
	defer objLock.Unlock()

	// 确保目录存在
	if err := os.MkdirAll(filepath.Dir(objectPath), dirPerm); err != nil {
		return fmt.Errorf("failed to create object directory: %w", err)
	}

	// 直接写入对象数据，不验证
	err := s.withRetry(func() error {
		return s.atomicWriteWithoutVerify(objectPath, object.Data)
	})
	if err != nil {
		return fmt.Errorf("failed to write object data: %w", err)
	}

	// 写入元数据
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

// 添加不验证的原子写入方法
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

	// 直接移动，不验证
	if err = os.Rename(tmpFile, filePath); err != nil {
		return fmt.Errorf("failed to move temp file: %w", err)
	}

	return nil
}

// AbortMultipartUpload 中止分段上传
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

	// 验证上传是否存在
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

	// 验证桶和键
	if uploadInfo.Bucket != bucket || uploadInfo.Key != key {
		atomic.AddInt64(&s.metrics.ErrorCount, 1)
		return fmt.Errorf("bucket or key mismatch")
	}

	// 删除整个上传目录
	if err := os.RemoveAll(uploadPath); err != nil {
		atomic.AddInt64(&s.metrics.ErrorCount, 1)
		return fmt.Errorf("failed to remove upload directory: %w", err)
	}

	return nil
}

// ListMultipartUploads 列出所有进行中的分段上传
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

		// 只返回指定桶的上传
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

// ListParts 列出分段上传的所有部分
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

	// 验证上传是否存在
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

	// 验证桶和键
	if uploadInfo.Bucket != bucket || uploadInfo.Key != key {
		atomic.AddInt64(&s.metrics.ErrorCount, 1)
		return nil, fmt.Errorf("bucket or key mismatch")
	}

	var parts []*PartInfo

	// 读取所有分段信息文件
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

	// 按分段号排序
	sort.Slice(parts, func(i, j int) bool {
		return parts[i].PartNumber < parts[j].PartNumber
	})

	return parts, nil
}

// 优化方法

// GetObjectStream 流式获取对象（用于大文件）
func (s *DB) GetObjectStream(bucket, key string) (io.ReadCloser, *ObjectData, error) {
	defer func() {
		if r := recover(); r != nil {
			s.log.Errorf("Recovered from panic in GetObjectStream: %v\n%s", r, debug.Stack())
		}
	}()

	atomic.AddInt64(&s.metrics.ReadOps, 1)

	objectPath := s.getObjectPath(bucket, key)
	metadataPath := s.getObjectMetadataPath(bucket, key)

	// 获取读锁
	objLock := s.getObjectLock(bucket, key)
	objLock.RLock()
	defer objLock.RUnlock()

	// 打开对象文件
	file, err := os.Open(objectPath)
	if err != nil {
		if os.IsNotExist(err) {
			atomic.AddInt64(&s.metrics.ErrorCount, 1)
			return nil, nil, fmt.Errorf("object not found")
		}
		atomic.AddInt64(&s.metrics.ErrorCount, 1)
		return nil, nil, fmt.Errorf("failed to open object: %w", err)
	}

	// 读取元数据
	objectData := &ObjectData{}
	if metaData, err := s.readFileOptimized(metadataPath); err == nil {
		if err := objectData.UnmarshalJSON(metaData); err != nil {
			file.Close()
			atomic.AddInt64(&s.metrics.ErrorCount, 1)
			return nil, nil, fmt.Errorf("failed to parse metadata: %w", err)
		}
	} else {
		// 如果没有元数据，创建基本信息
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
			ETag:         "", // 流式读取时不计算ETag
			Size:         info.Size(),
		}
	}

	return file, objectData, nil
}

// PutObjectStream 流式存储对象（用于大文件）
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

	// 获取对象锁
	objLock := s.getObjectLock(bucket, key)
	objLock.Lock()
	defer objLock.Unlock()

	// 确保目录存在
	if err := os.MkdirAll(filepath.Dir(objectPath), dirPerm); err != nil {
		atomic.AddInt64(&s.metrics.ErrorCount, 1)
		return "", fmt.Errorf("failed to create object directory: %w", err)
	}

	// 创建临时文件
	tmpFile := filepath.Join(s.basePath, tmpDir, fmt.Sprintf("stream_%d_%d_%s",
		time.Now().UnixNano(), os.Getpid(), filepath.Base(objectPath)))

	var file *os.File
	var err error

	if s.config.UseDirectIO && size > MB { // 大于1MB使用直接IO
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

	// 确保缓冲区不为空
	if len(buf) == 0 {
		buf = make([]byte, 64*KB) // 64KB 默认缓冲区
	}

	hasher := s.getHasher()
	defer s.putHasher(hasher)

	// 多路写入：文件 + 哈希计算 + 进度输出（如果提供）
	var multiWriter io.Writer
	if progressWriter != nil {
		multiWriter = io.MultiWriter(file, hasher, progressWriter)
	} else {
		multiWriter = io.MultiWriter(file, hasher)
	}

	var written int64
	if size > 0 {
		// 已知大小，使用CopyN
		written, err = io.CopyBuffer(multiWriter, io.LimitReader(reader, size), buf)
	} else {
		// 未知大小，使用Copy
		written, err = io.CopyBuffer(multiWriter, reader, buf)
	}

	if err != nil {
		atomic.AddInt64(&s.metrics.ErrorCount, 1)
		return "", fmt.Errorf("failed to write object data: %w", err)
	}

	// 强制刷盘
	if err = file.Sync(); err != nil {
		atomic.AddInt64(&s.metrics.ErrorCount, 1)
		return "", fmt.Errorf("failed to sync file: %w", err)
	}

	file.Close()

	// 计算ETag
	etag := hex.EncodeToString(hasher.Sum(nil))

	// 验证数据完整性
	if err = s.verifyDataIntegrity(tmpFile, etag); err != nil {
		atomic.AddInt64(&s.metrics.ErrorCount, 1)
		return "", fmt.Errorf("data integrity check failed: %w", err)
	}

	// 原子移动到最终位置
	if err = os.Rename(tmpFile, objectPath); err != nil {
		atomic.AddInt64(&s.metrics.ErrorCount, 1)
		return "", fmt.Errorf("failed to move temp file: %w", err)
	}

	// 写入元数据
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
		// 如果元数据写入失败，清理对象文件
		os.Remove(objectPath)
		atomic.AddInt64(&s.metrics.ErrorCount, 1)
		return "", fmt.Errorf("failed to write metadata: %w", err)
	}

	atomic.AddInt64(&s.metrics.WriteBytes, written)
	return etag, nil
}

// GetObjectRange 获取对象的指定范围（支持HTTP Range请求）
func (s *DB) GetObjectRange(bucket, key string, start, end int64) ([]byte, *ObjectData, error) {
	defer func() {
		if r := recover(); r != nil {
			s.log.Errorf("Recovered from panic in GetObjectRange: %v\n%s", r, debug.Stack())
		}
	}()

	atomic.AddInt64(&s.metrics.ReadOps, 1)

	objectPath := s.getObjectPath(bucket, key)
	metadataPath := s.getObjectMetadataPath(bucket, key)

	// 获取读锁
	objLock := s.getObjectLock(bucket, key)
	objLock.RLock()
	defer objLock.RUnlock()

	// 打开文件
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

	// 获取文件大小
	info, err := file.Stat()
	if err != nil {
		atomic.AddInt64(&s.metrics.ErrorCount, 1)
		return nil, nil, fmt.Errorf("failed to get file info: %w", err)
	}

	fileSize := info.Size()

	// 调整范围
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

	// 读取指定范围
	rangeSize := end - start + 1
	data := make([]byte, rangeSize)

	_, err = file.ReadAt(data, start)
	if err != nil && err != io.EOF {
		atomic.AddInt64(&s.metrics.ErrorCount, 1)
		return nil, nil, fmt.Errorf("failed to read range: %w", err)
	}

	// 读取元数据
	objectData := &ObjectData{}
	if metaData, err := s.readFileOptimized(metadataPath); err == nil {
		if err := objectData.UnmarshalJSON(metaData); err == nil {
			atomic.AddInt64(&s.metrics.ReadBytes, rangeSize)
			return data, objectData, nil
		}
	}

	// 如果没有元数据，创建基本信息
	objectData = &ObjectData{
		Key:          key,
		ContentType:  "application/octet-stream",
		LastModified: info.ModTime(),
		ETag:         "", // 范围读取时不计算完整ETag
		Size:         fileSize,
	}

	atomic.AddInt64(&s.metrics.ReadBytes, rangeSize)
	return data, objectData, nil
}

// Gettats 获取存储统计信息
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
			return nil // 忽略错误继续
		}

		if d.IsDir() {
			// 检查是否是桶目录（直接在buckets下的目录）
			if filepath.Dir(path) == bucketsPath {
				stats.BucketCount++
			}
			return nil
		}

		// 跳过元数据文件
		if strings.HasSuffix(path, metadataExt) {
			return nil
		}

		// 统计对象
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

// 性能监控和健康检查方法

// GetMetrics 获取性能指标
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

// HealthCheck 健康检查
func (s *DB) HealthCheck() error {
	// 检查基础目录是否可访问
	if _, err := os.Stat(s.basePath); err != nil {
		return fmt.Errorf("base path not accessible: %w", err)
	}

	// 检查磁盘空间
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

	// 检查错误率
	metrics := s.GetMetrics()
	totalOps := metrics.ReadOps + metrics.WriteOps + metrics.DeleteOps + metrics.ListOps
	if totalOps > 0 {
		errorRate := float64(metrics.ErrorCount) / float64(totalOps) * 100
		if errorRate > 5 { // 错误率超过5%
			return fmt.Errorf("error rate too high: %.2f%%", errorRate)
		}
	}

	return nil
}

// ResetMetrics 重置指标
func (s *DB) ResetMetrics() {
	atomic.StoreInt64(&s.metrics.ReadOps, 0)
	atomic.StoreInt64(&s.metrics.WriteOps, 0)
	atomic.StoreInt64(&s.metrics.DeleteOps, 0)
	atomic.StoreInt64(&s.metrics.ListOps, 0)
	atomic.StoreInt64(&s.metrics.ReadBytes, 0)
	atomic.StoreInt64(&s.metrics.WriteBytes, 0)
	atomic.StoreInt64(&s.metrics.ErrorCount, 0)
}

// GetDiskUsage 获取磁盘使用情况
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

// SetConfig 更新配置
func (s *DB) SetConfig(config *Config) {
	if config == nil {
		return
	}

	// 更新配置
	s.config = config

	// 重新初始化资源池
	s.bufferPool = sync.Pool{
		New: func() interface{} {
			return make([]byte, s.config.BufferSize)
		},
	}
}

// GetConfig 获取当前配置
func (s *DB) GetConfig() *Config {
	return s.config
}

// Compact 压缩存储（清理碎片）
func (s *DB) Compact() error {
	s.log.Info("Starting compaction")

	// 清理空目录
	if err := s.cleanupEmptyDirectories(); err != nil {
		s.log.Error("Failed to cleanup empty directories during compaction: ", err)
	}

	// 清理临时文件
	if err := s.cleanupTempFiles(); err != nil {
		s.log.Error("Failed to cleanup temp files during compaction: ", err)
	}

	// 清理过期上传
	if err := s.cleanupExpiredUploads(); err != nil {
		s.log.Error("Failed to cleanup expired uploads during compaction: ", err)
	}

	// 清理锁映射
	s.cleanupLockMaps()

	s.log.Info("compaction completed")
	return nil
}

// Backup 备份存储（简单实现）
func (s *DB) Backup(backupPath string) error {
	s.log.Info("Starting backup to: ", backupPath)

	// 创建备份目录
	if err := os.MkdirAll(backupPath, dirPerm); err != nil {
		return fmt.Errorf("failed to create backup directory: %w", err)
	}

	// 复制整个存储目录
	return s.copyDir(s.basePath, backupPath)
}

// copyDir 递归复制目录
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

		// 复制文件
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

		// 确保缓冲区不为空
		if len(buf) == 0 {
			buf = make([]byte, 64*KB) // 64KB 默认缓冲区
		}

		_, err = io.CopyBuffer(dstFile, srcFile, buf)
		return err
	})
}

// Restore 从备份恢复存储
func (s *DB) Restore(backupPath string) error {
	s.log.Info("Starting restore from: ", backupPath)

	// 检查备份目录是否存在
	if _, err := os.Stat(backupPath); os.IsNotExist(err) {
		return fmt.Errorf("backup directory does not exist: %s", backupPath)
	}

	// 停止清理例程
	s.cancel()
	if s.cleanupTicker != nil {
		s.cleanupTicker.Stop()
	}

	// 清空当前存储
	if err := os.RemoveAll(s.basePath); err != nil {
		return fmt.Errorf("failed to remove current  %w", err)
	}

	// 从备份恢复
	if err := s.copyDir(backupPath, s.basePath); err != nil {
		return fmt.Errorf("failed to restore from backup: %w", err)
	}

	// 重新启动清理例程
	ctx, cancel := context.WithCancel(context.Background())
	s.ctx = ctx
	s.cancel = cancel
	s.startOptimizedCleanupRoutine()

	s.log.Info("restore completed")
	return nil
}

// Validate验证存储完整性
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

		// 验证对象文件
		metadataPath := path + metadataExt
		if _, err := os.Stat(metadataPath); os.IsNotExist(err) {
			errors = append(errors, fmt.Sprintf("Missing metadata for: %s", path))
			return nil
		}

		// 读取元数据
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

		// 验证数据完整性
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

type nopLogger struct {}
func (l *nopLogger) Debug(args ...interface{}) {}
func (l *nopLogger) Info(args ...interface{}) {}
func (l *nopLogger) Warn(args ...interface{}) {}
func (l *nopLogger) Error(args ...interface{}) {}
func (l *nopLogger) Fatal(args ...interface{}) {}
func (l *nopLogger) Debugf(template string, args ...interface{}) {}
func (l *nopLogger) Infof(template string, args ...interface{}) {}
func (l *nopLogger) Warnf(template string, args ...interface{}) {}
func (l *nopLogger) Errorf(template string, args ...interface{}) {}