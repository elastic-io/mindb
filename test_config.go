 //go:build !windows
// +build !windows

package mindb

import (
	"os"
	"testing"
	"time"
)


func getTestConfig() *Config {
	config := DefaultConfig()

	
	config.EnableChecksumVerify = true
	config.BufferSize = 32 * 1024             
	config.UseDirectIO = false                
	config.UseMmap = false                    
	config.CleanupInterval = 10 * time.Second 
	config.TempFileMaxAge = 30 * time.Second  

	return config
}


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

	
	storage.SetConfig(getTestConfig())

	return storage, tmpDir
}
