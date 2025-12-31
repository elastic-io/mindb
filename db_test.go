 //go:build !windows
// +build !windows

package mindb

//go:build !windows
// +build !windows

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupTest(t *testing.T) (*DB, string) {
	return setupTestStorageWithConfig(t)
}

func cleanupTestStorage(t *testing.T, tmpDir string) {
	os.RemoveAll(tmpDir)
}

func generateTestData(size int) []byte {
	data := make([]byte, size)
	rand.Read(data)
	return data
}

func TestBasicBucketOperations(t *testing.T) {
	s, tmpDir := setupTest(t)
	defer cleanupTestStorage(t, tmpDir)
	defer s.Close()

	testBucket := "test-bucket"

	t.Run("CreateBucket", func(t *testing.T) {
		err := s.CreateBucket(testBucket)
		assert.NoError(t, err)

		err = s.CreateBucket(testBucket)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "already exists")
	})

	t.Run("BucketExists", func(t *testing.T) {
		exists, err := s.BucketExists(testBucket)
		assert.NoError(t, err)
		assert.True(t, exists)

		exists, err = s.BucketExists("non-existent-bucket")
		assert.NoError(t, err)
		assert.False(t, exists)
	})

	t.Run("ListBuckets", func(t *testing.T) {
		buckets, err := s.ListBuckets()
		assert.NoError(t, err)
		assert.Len(t, buckets, 1)
		assert.Equal(t, testBucket, buckets[0].Name)
	})

	t.Run("DeleteNonEmptyBucket", func(t *testing.T) {

		testData := generateTestData(1024)
		obj := &ObjectData{
			Key:         "test-object",
			Data:        testData,
			ContentType: "application/octet-stream",
		}
		err := s.PutObject(testBucket, obj)
		require.NoError(t, err)

		err = s.DeleteBucket(testBucket)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "not empty")

		err = s.DeleteObject(testBucket, "test-object")
		assert.NoError(t, err)

		err = s.DeleteBucket(testBucket)
		assert.NoError(t, err)
	})
}

func TestBasicObjectOperations(t *testing.T) {
	s, tmpDir := setupTest(t)
	defer cleanupTestStorage(t, tmpDir)
	defer s.Close()

	testBucket := "test-bucket"
	err := s.CreateBucket(testBucket)
	require.NoError(t, err)

	t.Run("PutAndGetObject", func(t *testing.T) {
		testData := generateTestData(1024)
		testKey := "test-object"

		obj := &ObjectData{
			Key:         testKey,
			Data:        testData,
			ContentType: "application/octet-stream",
			Metadata:    map[string]string{"test": "value"},
		}
		err := s.PutObject(testBucket, obj)
		assert.NoError(t, err)

		retrievedObj, err := s.GetObject(testBucket, testKey)
		assert.NoError(t, err)
		assert.Equal(t, testKey, retrievedObj.Key)
		assert.Equal(t, testData, retrievedObj.Data)
		assert.Equal(t, "application/octet-stream", retrievedObj.ContentType)
		assert.Equal(t, "value", retrievedObj.Metadata["test"])
		assert.NotEmpty(t, retrievedObj.ETag)
	})

	t.Run("GetNonExistentObject", func(t *testing.T) {
		_, err := s.GetObject(testBucket, "non-existent")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "not found")
	})

	t.Run("DeleteObject", func(t *testing.T) {
		testKey := "delete-test"
		testData := generateTestData(512)

		obj := &ObjectData{
			Key:  testKey,
			Data: testData,
		}
		err := s.PutObject(testBucket, obj)
		require.NoError(t, err)

		err = s.DeleteObject(testBucket, testKey)
		assert.NoError(t, err)

		_, err = s.GetObject(testBucket, testKey)
		assert.Error(t, err)
	})
}

func TestListObjects(t *testing.T) {
	s, tmpDir := setupTest(t)
	defer cleanupTestStorage(t, tmpDir)
	defer s.Close()

	testBucket := "test-bucket"
	err := s.CreateBucket(testBucket)
	require.NoError(t, err)

	testObjects := []string{
		"folder1/file1.txt",
		"folder1/file2.txt",
		"folder2/file1.txt",
		"root-file.txt",
	}

	for _, key := range testObjects {
		obj := &ObjectData{
			Key:  key,
			Data: generateTestData(100),
		}
		err := s.PutObject(testBucket, obj)
		require.NoError(t, err)
	}

	t.Run("ListAllObjects", func(t *testing.T) {
		objects, prefixes, err := s.ListObjects(testBucket, "", "", "", 100)
		assert.NoError(t, err)
		assert.Len(t, objects, 4)
		assert.Empty(t, prefixes)
	})

	t.Run("ListWithPrefix", func(t *testing.T) {
		objects, prefixes, err := s.ListObjects(testBucket, "folder1/", "", "", 100)
		assert.NoError(t, err)
		assert.Len(t, objects, 2)
		assert.Empty(t, prefixes)

		for _, obj := range objects {
			assert.True(t, strings.HasPrefix(obj.Key, "folder1/"))
		}
	})

	t.Run("ListWithDelimiter", func(t *testing.T) {
		objects, prefixes, err := s.ListObjects(testBucket, "", "", "/", 100)
		assert.NoError(t, err)
		assert.Len(t, objects, 1)  // root-file.txt
		assert.Len(t, prefixes, 2) // folder1/, folder2/
	})

	t.Run("ListWithMaxKeys", func(t *testing.T) {
		objects, _, err := s.ListObjects(testBucket, "", "", "", 2)
		assert.NoError(t, err)
		assert.Len(t, objects, 2)
	})
}

func TestMultipartUpload(t *testing.T) {
	s, tmpDir := setupTest(t)
	defer cleanupTestStorage(t, tmpDir)
	defer s.Close()

	testBucket := "test-bucket"
	err := s.CreateBucket(testBucket)
	require.NoError(t, err)

	testKey := "multipart-test"
	contentType := "application/octet-stream"

	t.Run("CompleteMultipartUpload", func(t *testing.T) {

		uploadID, err := s.CreateMultipartUpload(testBucket, testKey, contentType, nil)
		assert.NoError(t, err)
		assert.NotEmpty(t, uploadID)

		partSize := 5 * 1024 * 1024 // 5MB
		numParts := 3
		var parts []MultipartPart
		var allData []byte

		for i := 1; i <= numParts; i++ {
			partData := generateTestData(partSize)
			allData = append(allData, partData...)

			etag, err := s.UploadPart(testBucket, testKey, uploadID, i, partData)
			assert.NoError(t, err)
			assert.NotEmpty(t, etag)

			parts = append(parts, MultipartPart{
				PartNumber: i,
				ETag:       etag,
			})
		}

		finalETag, err := s.CompleteMultipartUpload(testBucket, testKey, uploadID, parts)
		assert.NoError(t, err)
		assert.NotEmpty(t, finalETag)
		assert.Contains(t, finalETag, "-")

		obj, err := s.GetObject(testBucket, testKey)
		assert.NoError(t, err)
		assert.Equal(t, allData, obj.Data)
		assert.Equal(t, finalETag, obj.ETag)
	})

	t.Run("AbortMultipartUpload", func(t *testing.T) {
		uploadID, err := s.CreateMultipartUpload(testBucket, "abort-test", contentType, nil)
		assert.NoError(t, err)

		partData := generateTestData(1024)
		_, err = s.UploadPart(testBucket, "abort-test", uploadID, 1, partData)
		assert.NoError(t, err)

		err = s.AbortMultipartUpload(testBucket, "abort-test", uploadID)
		assert.NoError(t, err)

		_, err = s.ListParts(testBucket, "abort-test", uploadID)
		assert.Error(t, err)
	})

	t.Run("ListMultipartUploads", func(t *testing.T) {

		uploadID1, err := s.CreateMultipartUpload(testBucket, "test1", contentType, nil)
		assert.NoError(t, err)

		uploadID2, err := s.CreateMultipartUpload(testBucket, "test2", contentType, nil)
		assert.NoError(t, err)

		uploads, err := s.ListMultipartUploads(testBucket)
		assert.NoError(t, err)
		assert.Len(t, uploads, 2)

		s.AbortMultipartUpload(testBucket, "test1", uploadID1)
		s.AbortMultipartUpload(testBucket, "test2", uploadID2)
	})
}

func TestStreamOperations(t *testing.T) {
	s, tmpDir := setupTest(t)
	defer cleanupTestStorage(t, tmpDir)
	defer s.Close()

	testBucket := "test-bucket"
	err := s.CreateBucket(testBucket)
	require.NoError(t, err)

	t.Run("PutObjectStream", func(t *testing.T) {
		testData := generateTestData(10 * 1024) // 10KB
		reader := bytes.NewReader(testData)

		etag, err := s.PutObjectStream(testBucket, "stream-test", reader,
			int64(len(testData)), "application/octet-stream", nil, nil)
		assert.NoError(t, err)
		assert.NotEmpty(t, etag)

		obj, err := s.GetObject(testBucket, "stream-test")
		assert.NoError(t, err)
		assert.Equal(t, testData, obj.Data)
	})

	t.Run("GetObjectStream", func(t *testing.T) {
		testData := generateTestData(5 * 1024)
		obj := &ObjectData{
			Key:  "stream-read-test",
			Data: testData,
		}
		err := s.PutObject(testBucket, obj)
		require.NoError(t, err)

		reader, metadata, err := s.GetObjectStream(testBucket, "stream-read-test")
		assert.NoError(t, err)
		defer reader.Close()

		readData, err := io.ReadAll(reader)
		assert.NoError(t, err)
		assert.Equal(t, testData, readData)
		assert.Equal(t, "stream-read-test", metadata.Key)
	})

	t.Run("GetObjectRange", func(t *testing.T) {
		testData := generateTestData(1024)
		obj := &ObjectData{
			Key:  "range-test",
			Data: testData,
		}
		err := s.PutObject(testBucket, obj)
		require.NoError(t, err)

		start, end := int64(100), int64(199)
		rangeData, metadata, err := s.GetObjectRange(testBucket, "range-test", start, end)
		assert.NoError(t, err)
		assert.Equal(t, testData[start:end+1], rangeData)
		assert.Equal(t, "range-test", metadata.Key)
	})
}

func TestDataIntegrity(t *testing.T) {
	s, tmpDir := setupTest(t)
	defer cleanupTestStorage(t, tmpDir)
	defer s.Close()

	config := s.GetConfig()
	config.EnableChecksumVerify = true
	s.SetConfig(config)

	testBucket := "test-bucket"
	err := s.CreateBucket(testBucket)
	require.NoError(t, err)

	t.Run("SingleFileIntegrity", func(t *testing.T) {
		testData := generateTestData(1024)
		obj := &ObjectData{
			Key:  "integrity-test",
			Data: testData,
		}

		err := s.PutObject(testBucket, obj)
		assert.NoError(t, err)

		retrievedObj, err := s.GetObject(testBucket, "integrity-test")
		assert.NoError(t, err)
		assert.Equal(t, testData, retrievedObj.Data)
	})

	t.Run("MultipartIntegrity", func(t *testing.T) {
		uploadID, err := s.CreateMultipartUpload(testBucket, "multipart-integrity",
			"application/octet-stream", nil)
		require.NoError(t, err)

		partSize := 1024
		numParts := 3
		var parts []MultipartPart
		var allData []byte

		for i := 1; i <= numParts; i++ {
			partData := generateTestData(partSize)
			allData = append(allData, partData...)

			etag, err := s.UploadPart(testBucket, "multipart-integrity", uploadID, i, partData)
			assert.NoError(t, err)

			parts = append(parts, MultipartPart{
				PartNumber: i,
				ETag:       etag,
			})
		}

		finalETag, err := s.CompleteMultipartUpload(testBucket, "multipart-integrity", uploadID, parts)
		assert.NoError(t, err)

		obj, err := s.GetObject(testBucket, "multipart-integrity")
		assert.NoError(t, err)
		assert.Equal(t, allData, obj.Data)
		assert.Equal(t, finalETag, obj.ETag)
	})
}

func TestConcurrentOperations(t *testing.T) {
	s, tmpDir := setupTest(t)
	defer cleanupTestStorage(t, tmpDir)
	defer s.Close()

	testBucket := "test-bucket"
	err := s.CreateBucket(testBucket)
	require.NoError(t, err)

	t.Run("ConcurrentWrites", func(t *testing.T) {
		numGoroutines := 10
		var wg sync.WaitGroup
		errors := make(chan error, numGoroutines)

		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()

				testData := generateTestData(1024)
				obj := &ObjectData{
					Key:  fmt.Sprintf("concurrent-test-%d", id),
					Data: testData,
				}

				if err := s.PutObject(testBucket, obj); err != nil {
					errors <- err
				}
			}(i)
		}

		wg.Wait()
		close(errors)

		for err := range errors {
			t.Errorf("Concurrent write error: %v", err)
		}
	})

	t.Run("ConcurrentReads", func(t *testing.T) {

		testData := generateTestData(1024)
		obj := &ObjectData{
			Key:  "read-test",
			Data: testData,
		}
		err := s.PutObject(testBucket, obj)
		require.NoError(t, err)

		numGoroutines := 10
		var wg sync.WaitGroup
		errors := make(chan error, numGoroutines)

		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()

				retrievedObj, err := s.GetObject(testBucket, "read-test")
				if err != nil {
					errors <- err
					return
				}

				if !bytes.Equal(testData, retrievedObj.Data) {
					errors <- fmt.Errorf("data mismatch in concurrent read")
				}
			}()
		}

		wg.Wait()
		close(errors)

		for err := range errors {
			t.Errorf("Concurrent read error: %v", err)
		}
	})
}

func TestStorageStats(t *testing.T) {
	s, tmpDir := setupTest(t)
	defer cleanupTestStorage(t, tmpDir)
	defer s.Close()

	testBucket := "test-bucket"
	err := s.CreateBucket(testBucket)
	require.NoError(t, err)

	for i := 0; i < 5; i++ {
		obj := &ObjectData{
			Key:  fmt.Sprintf("test-object-%d", i),
			Data: generateTestData(1024),
		}
		err := s.PutObject(testBucket, obj)
		require.NoError(t, err)
	}

	stats, err := s.GetStats()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), stats.BucketCount)
	assert.Equal(t, int64(5), stats.ObjectCount)
	assert.Greater(t, stats.TotalSize, int64(0))

	metrics := s.GetMetrics()
	assert.Greater(t, metrics.WriteOps, int64(0))
	assert.Greater(t, metrics.WriteBytes, int64(0))
}

func TestHealthCheck(t *testing.T) {
	s, tmpDir := setupTest(t)
	defer cleanupTestStorage(t, tmpDir)
	defer s.Close()

	err := s.HealthCheck()
	assert.NoError(t, err)

	total, free, used, err := s.GetDiskUsage()
	assert.NoError(t, err)
	assert.Greater(t, total, uint64(0))
	assert.Greater(t, free, uint64(0))
	assert.Greater(t, used, uint64(0))
	assert.Equal(t, total, free+used)
}
