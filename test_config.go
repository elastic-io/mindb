package mindb

import (
	"os"
	"testing"
	"time"
)

// 测试配置
func getTestConfig() *Config {
	config := DefaultConfig()

	// 针对测试环境的优化配置
	config.EnableChecksumVerify = true
	config.BufferSize = 32 * 1024             // 32KB，适合测试
	config.UseDirectIO = false                // 测试环境通常不需要DirectIO
	config.UseMmap = false                    // 简化测试
	config.CleanupInterval = 10 * time.Second // 更频繁的清理
	config.TempFileMaxAge = 30 * time.Second  // 更短的临时文件保留时间

	return config
}

// 设置测试存储
func setupTestStorageWithConfig(t *testing.T) (*DB, string) {
	tmpDir, err := os.MkdirTemp("", "test_*")
	if err != nil {
		t.Fatal(err)
	}

	s, err := New(tmpDir)
	if err != nil {
		t.Fatal(err)
	}

	storage := s

	// 应用测试配置
	storage.SetConfig(getTestConfig())

	return storage, tmpDir
}
