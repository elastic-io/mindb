# API Reference

This document provides a comprehensive reference for the mindb storage system API.

## Table of Contents

- [Storage Instance](#storage-instance)
- [Bucket Operations](#bucket-operations)
- [Object Operations](#object-operations)
- [Multipart Upload](#multipart-upload)
- [Streaming Operations](#streaming-operations)
- [Monitoring & Statistics](#monitoring--statistics)
- [Configuration](#configuration)
- [Utility Functions](#utility-functions)
- [Data Types](#data-types)
- [Error Handling](#error-handling)
- [Logger Interface](#logger-interface)

## Storage Instance

### New(path string, logger Logger) (*DB, error)

Creates a new storage instance with the specified base path and logger.

**Parameters:**
- `path` (string): Base directory for storage data
- `logger` (Logger): Logger implementation for system events

**Returns:**
- `*DB`: Storage instance
- `error`: Error if creation fails

**Example:**
```go
logger := &MyLogger{}
storage, err := mindb.New("./data", logger)
if err != nil {
    log.Fatal("Failed to create storage:", err)
}
defer storage.Close()
```

**Errors:**
- Directory creation failure
- Permission issues
- Invalid logger (nil)

### Close() error

Closes the storage instance and cleans up all resources including background routines.

**Returns:**
- `error`: Error if cleanup fails

**Example:**
```go
if err := storage.Close(); err != nil {
    log.Printf("Error closing storage: %v", err)
}
```

## Bucket Operations

### CreateBucket(bucket string) error

Creates a new bucket with the specified name.

**Parameters:**
- `bucket` (string): Bucket name (must be valid identifier)

**Returns:**
- `error`: Error if creation fails

**Example:**
```go
err := storage.CreateBucket("my-bucket")
if err != nil {
    log.Printf("Failed to create bucket: %v", err)
}
```

**Errors:**
- Bucket already exists
- Invalid bucket name
- Permission issues

### DeleteBucket(bucket string) error

Deletes an empty bucket. The bucket must contain no objects.

**Parameters:**
- `bucket` (string): Bucket name to delete

**Returns:**
- `error`: Error if deletion fails

**Example:**
```go
err := storage.DeleteBucket("my-bucket")
if err != nil {
    log.Printf("Failed to delete bucket: %v", err)
}
```

**Errors:**
- Bucket not found
- Bucket not empty
- Permission issues

### BucketExists(bucket string) (bool, error)

Checks if a bucket exists.

**Parameters:**
- `bucket` (string): Bucket name to check

**Returns:**
- `bool`: True if bucket exists, false otherwise
- `error`: Error if check fails

**Example:**
```go
exists, err := storage.BucketExists("my-bucket")
if err != nil {
    log.Printf("Error checking bucket: %v", err)
} else if exists {
    fmt.Println("Bucket exists")
}
```

### ListBuckets() ([]BucketInfo, error)

Lists all buckets in the storage system.

**Returns:**
- `[]BucketInfo`: Slice of bucket information
- `error`: Error if listing fails

**Example:**
```go
buckets, err := storage.ListBuckets()
if err != nil {
    log.Printf("Failed to list buckets: %v", err)
} else {
    for _, bucket := range buckets {
        fmt.Printf("Bucket: %s, Created: %s\n", 
            bucket.Name, bucket.CreationDate.Format(time.RFC3339))
    }
}
```

## Object Operations

### PutObject(bucket string, object *ObjectData) error

Stores an object in the specified bucket.

**Parameters:**
- `bucket` (string): Target bucket name
- `object` (*ObjectData): Object data and metadata

**Returns:**
- `error`: Error if storage fails

**Example:**
```go
objectData := &mindb.ObjectData{
    Key:         "documents/readme.txt",
    Data:        []byte("Hello, World!"),
    ContentType: "text/plain",
    Metadata: map[string]string{
        "author":  "john.doe",
        "version": "1.0",
        "project": "mindb-storage",
    },
}

err := storage.PutObject("my-bucket", objectData)
if err != nil {
    log.Printf("Failed to store object: %v", err)
}
```

**Features:**
- Automatic ETag calculation
- Atomic write operations
- Data integrity verification
- Metadata support

### GetObject(bucket, key string) (*ObjectData, error)

Retrieves an object from the specified bucket.

**Parameters:**
- `bucket` (string): Source bucket name
- `key` (string): Object key

**Returns:**
- `*ObjectData`: Object data and metadata
- `error`: Error if retrieval fails

**Example:**
```go
object, err := storage.GetObject("my-bucket", "documents/readme.txt")
if err != nil {
    log.Printf("Failed to get object: %v", err)
} else {
    fmt.Printf("Content: %s\n", string(object.Data))
    fmt.Printf("Type: %s\n", object.ContentType)
    fmt.Printf("ETag: %s\n", object.ETag)
    fmt.Printf("Size: %d bytes\n", object.Size)
    
    // Access metadata
    if author, ok := object.Metadata["author"]; ok {
        fmt.Printf("Author: %s\n", author)
    }
}
```

### DeleteObject(bucket, key string) error

Deletes an object from the specified bucket.

**Parameters:**
- `bucket` (string): Source bucket name
- `key` (string): Object key to delete

**Returns:**
- `error`: Error if deletion fails

**Example:**
```go
err := storage.DeleteObject("my-bucket", "documents/readme.txt")
if err != nil {
    log.Printf("Failed to delete object: %v", err)
}
```

### ListObjects(bucket, prefix, marker, delimiter string, maxKeys int) ([]ObjectInfo, []string, error)

Lists objects in a bucket with optional filtering and pagination.

**Parameters:**
- `bucket` (string): Bucket name to list
- `prefix` (string): Key prefix filter (empty for all)
- `marker` (string): Start listing after this key (pagination)
- `delimiter` (string): Delimiter for grouping keys (e.g., "/")
- `maxKeys` (int): Maximum number of keys to return

**Returns:**
- `[]ObjectInfo`: Slice of object information
- `[]string`: Common prefixes when using delimiter
- `error`: Error if listing fails

**Example:**
```go
// List all objects
objects, prefixes, err := storage.ListObjects("my-bucket", "", "", "", 1000)
if err != nil {
    log.Printf("Failed to list objects: %v", err)
} else {
    fmt.Printf("Found %d objects\n", len(objects))
    for _, obj := range objects {
        fmt.Printf("- %s (%d bytes, %s)\n", 
            obj.Key, obj.Size, obj.LastModified.Format(time.RFC3339))
    }
}

// List with prefix filter
objects, _, err = storage.ListObjects("my-bucket", "documents/", "", "", 100)

// List with delimiter (directory-like listing)
objects, prefixes, err = storage.ListObjects("my-bucket", "", "", "/", 100)
for _, prefix := range prefixes {
    fmt.Printf("Directory: %s\n", prefix)
}
```

## Multipart Upload

Multipart upload allows you to upload large files in parts, providing better performance and reliability.

### CreateMultipartUpload(bucket, key, contentType string, metadata map[string]string) (string, error)

Initiates a multipart upload session.

**Parameters:**
- `bucket` (string): Target bucket name
- `key` (string): Object key
- `contentType` (string): MIME type of the final object
- `metadata` (map[string]string): Object metadata

**Returns:**
- `string`: Upload ID for subsequent operations
- `error`: Error if initiation fails

**Example:**
```go
metadata := map[string]string{
    "author": "john.doe",
    "type":   "backup",
}

uploadID, err := storage.CreateMultipartUpload(
    "my-bucket", 
    "large-file.zip", 
    "application/zip", 
    metadata,
)
if err != nil {
    log.Printf("Failed to create multipart upload: %v", err)
} else {
    fmt.Printf("Upload ID: %s\n", uploadID)
}
```

### UploadPart(bucket, key, uploadID string, partNumber int, data []byte) (string, error)

Uploads a part of the multipart upload.

**Parameters:**
- `bucket` (string): Target bucket name
- `key` (string): Object key
- `uploadID` (string): Upload ID from CreateMultipartUpload
- `partNumber` (int): Part number (1-10000)
- `data` ([]byte): Part data

**Returns:**
- `string`: Part ETag for completion
- `error`: Error if upload fails

**Example:**
```go
// Upload parts
var parts []mindb.MultipartPart
partSize := 5 * 1024 * 1024 // 5MB parts

for i := 0; i < totalParts; i++ {
    partData := getPartData(i, partSize) // Your function to get part data
    
    etag, err := storage.UploadPart("my-bucket", "large-file.zip", uploadID, i+1, partData)
    if err != nil {
        log.Printf("Failed to upload part %d: %v", i+1, err)
        // Abort upload on error
        storage.AbortMultipartUpload("my-bucket", "large-file.zip", uploadID)
        return
    }
    
    parts = append(parts, mindb.MultipartPart{
        PartNumber: i + 1,
        ETag:       etag,
    })
    
    fmt.Printf("Uploaded part %d/%d\n", i+1, totalParts)
}
```

### CompleteMultipartUpload(bucket, key, uploadID string, parts []MultipartPart) (string, error)

Completes the multipart upload by combining all parts.

**Parameters:**
- `bucket` (string): Target bucket name
- `key` (string): Object key
- `uploadID` (string): Upload ID
- `parts` ([]MultipartPart): Slice of uploaded parts with ETags

**Returns:**
- `string`: Final object ETag
- `error`: Error if completion fails

**Example:**
```go
finalETag, err := storage.CompleteMultipartUpload("my-bucket", "large-file.zip", uploadID, parts)
if err != nil {
    log.Printf("Failed to complete multipart upload: %v", err)
} else {
    fmt.Printf("Upload completed successfully! ETag: %s\n", finalETag)
}
```

### AbortMultipartUpload(bucket, key, uploadID string) error

Aborts a multipart upload and cleans up all uploaded parts.

**Parameters:**
- `bucket` (string): Target bucket name
- `key` (string): Object key
- `uploadID` (string): Upload ID to abort

**Returns:**
- `error`: Error if abort fails

**Example:**
```go
err := storage.AbortMultipartUpload("my-bucket", "large-file.zip", uploadID)
if err != nil {
    log.Printf("Failed to abort multipart upload: %v", err)
}
```

### ListMultipartUploads(bucket string) ([]MultipartUploadInfo, error)

Lists all active multipart uploads for a bucket.

**Parameters:**
- `bucket` (string): Bucket name

**Returns:**
- `[]MultipartUploadInfo`: Slice of active upload information
- `error`: Error if listing fails

**Example:**
```go
uploads, err := storage.ListMultipartUploads("my-bucket")
if err != nil {
    log.Printf("Failed to list uploads: %v", err)
} else {
    for _, upload := range uploads {
        fmt.Printf("Upload: %s (ID: %s, Started: %s)\n", 
            upload.Key, upload.UploadID, upload.CreatedAt.Format(time.RFC3339))
    }
}
```

### ListParts(bucket, key, uploadID string) ([]*PartInfo, error)

Lists all uploaded parts for a multipart upload.

**Parameters:**
- `bucket` (string): Bucket name
- `key` (string): Object key
- `uploadID` (string): Upload ID

**Returns:**
- `[]*PartInfo`: Slice of part information
- `error`: Error if listing fails

**Example:**
```go
parts, err := storage.ListParts("my-bucket", "large-file.zip", uploadID)
if err != nil {
    log.Printf("Failed to list parts: %v", err)
} else {
    for _, part := range parts {
        fmt.Printf("Part %d: %s (%d bytes)\n", 
            part.PartNumber, part.ETag, part.Size)
    }
}
```

## Streaming Operations

For handling large files efficiently without loading everything into memory.

### GetObjectStream(bucket, key string) (io.ReadCloser, *ObjectData, error)

Gets an object as a stream for memory-efficient reading.

**Parameters:**
- `bucket` (string): Source bucket name
- `key` (string): Object key

**Returns:**
- `io.ReadCloser`: Stream reader (must be closed)
- `*ObjectData`: Object metadata (Data field will be nil)
- `error`: Error if operation fails

**Example:**
```go
reader, metadata, err := storage.GetObjectStream("my-bucket", "large-file.zip")
if err != nil {
    log.Printf("Failed to get object stream: %v", err)
    return
}
defer reader.Close()

fmt.Printf("Streaming file: %s (%d bytes)\n", metadata.Key, metadata.Size)

// Copy to destination
file, err := os.Create("downloaded-file.zip")
if err != nil {
    log.Printf("Failed to create file: %v", err)
    return
}
defer file.Close()

written, err := io.Copy(file, reader)
if err != nil {
    log.Printf("Failed to copy data: %v", err)
} else {
    fmt.Printf("Downloaded %d bytes\n", written)
}
```

### PutObjectStream(bucket, key string, reader io.Reader, size int64, contentType string, metadata map[string]string) (string, error)

Stores an object from a stream for memory-efficient writing.

**Parameters:**
- `bucket` (string): Target bucket name
- `key` (string): Object key
- `reader` (io.Reader): Data stream
- `size` (int64): Expected size in bytes (-1 if unknown)
- `contentType` (string): MIME type
- `metadata` (map[string]string): Object metadata

**Returns:**
- `string`: Object ETag
- `error`: Error if storage fails

**Example:**
```go
file, err := os.Open("large-file.zip")
if err != nil {
    log.Printf("Failed to open file: %v", err)
    return
}
defer file.Close()

info, err := file.Stat()
if err != nil {
    log.Printf("Failed to get file info: %v", err)
    return
}

metadata := map[string]string{
    "source": "backup-system",
    "type":   "archive",
}

etag, err := storage.PutObjectStream(
    "my-bucket",
    ```go
    "large-file.zip",
    file,
    info.Size(),
    "application/zip",
    metadata,
)
if err != nil {
    log.Printf("Failed to upload stream: %v", err)
} else {
    fmt.Printf("Upload completed! ETag: %s\n", etag)
}
```

### GetObjectRange(bucket, key string, start, end int64) ([]byte, *ObjectData, error)

Gets a specific byte range from an object (useful for HTTP Range requests).

**Parameters:**
- `bucket` (string): Source bucket name
- `key` (string): Object key
- `start` (int64): Start byte offset (0-based)
- `end` (int64): End byte offset (inclusive, -1 for end of file)

**Returns:**
- `[]byte`: Range data
- `*ObjectData`: Object metadata
- `error`: Error if operation fails

**Example:**
```go
// Get first 1024 bytes
data, metadata, err := storage.GetObjectRange("my-bucket", "large-file.txt", 0, 1023)
if err != nil {
    log.Printf("Failed to get range: %v", err)
} else {
    fmt.Printf("Read %d bytes from %s\n", len(data), metadata.Key)
    fmt.Printf("Content preview: %s\n", string(data[:100]))
}

// Get last 1024 bytes
fileSize := metadata.Size
data, _, err = storage.GetObjectRange("my-bucket", "large-file.txt", fileSize-1024, -1)
```

## Monitoring & Statistics

### GetStats() (*Stats, error)

Gets comprehensive storage statistics.

**Returns:**
- `*Stats`: Storage statistics
- `error`: Error if retrieval fails

**Example:**
```go
stats, err := storage.GetStats()
if err != nil {
    log.Printf("Failed to get stats: %v", err)
} else {
    fmt.Printf("Storage Statistics:\n")
    fmt.Printf("  Buckets: %d\n", stats.BucketCount)
    fmt.Printf("  Objects: %d\n", stats.ObjectCount)
    fmt.Printf("  Total Size: %.2f MB\n", float64(stats.TotalSize)/(1024*1024))
}
```

### GetMetrics() *Metrics

Gets real-time performance metrics.

**Returns:**
- `*Metrics`: Performance metrics

**Example:**
```go
metrics := storage.GetMetrics()
fmt.Printf("Performance Metrics:\n")
fmt.Printf("  Read Operations: %d\n", metrics.ReadOps)
fmt.Printf("  Write Operations: %d\n", metrics.WriteOps)
fmt.Printf("  Delete Operations: %d\n", metrics.DeleteOps)
fmt.Printf("  List Operations: %d\n", metrics.ListOps)
fmt.Printf("  Read Bytes: %.2f MB\n", float64(metrics.ReadBytes)/(1024*1024))
fmt.Printf("  Write Bytes: %.2f MB\n", float64(metrics.WriteBytes)/(1024*1024))
fmt.Printf("  Error Count: %d\n", metrics.ErrorCount)
fmt.Printf("  Avg Read Latency: %v\n", time.Duration(metrics.AvgReadLatency))
fmt.Printf("  Avg Write Latency: %v\n", time.Duration(metrics.AvgWriteLatency))
fmt.Printf("  Active Reads: %d\n", metrics.ActiveReads)
fmt.Printf("  Active Writes: %d\n", metrics.ActiveWrites)
```

### HealthCheck() error

Performs a comprehensive health check of the storage system.

**Returns:**
- `error`: Error if health check fails, nil if healthy

**Example:**
```go
if err := storage.HealthCheck(); err != nil {
    log.Printf("Health check failed: %v", err)
    // Take corrective action
} else {
    fmt.Println("Storage system is healthy")
}
```

**Health Check Includes:**
- Base path accessibility
- Disk space availability (warns if >95% full)
- Error rate analysis (warns if >5% error rate)
- System resource availability

### GetDiskUsage() (total, free, used uint64, err error)

Gets disk usage information for the storage path.

**Returns:**
- `total` (uint64): Total disk space in bytes
- `free` (uint64): Free disk space in bytes
- `used` (uint64): Used disk space in bytes
- `err` (error): Error if retrieval fails

**Example:**
```go
total, free, used, err := storage.GetDiskUsage()
if err != nil {
    log.Printf("Failed to get disk usage: %v", err)
} else {
    fmt.Printf("Disk Usage:\n")
    fmt.Printf("  Total: %.2f GB\n", float64(total)/(1024*1024*1024))
    fmt.Printf("  Used: %.2f GB (%.1f%%)\n", 
        float64(used)/(1024*1024*1024), 
        float64(used)/float64(total)*100)
    fmt.Printf("  Free: %.2f GB\n", float64(free)/(1024*1024*1024))
}
```

### ResetMetrics()

Resets all performance metrics to zero.

**Example:**
```go
storage.ResetMetrics()
fmt.Println("Metrics reset")
```

## Configuration

### SetConfig(config *Config)

Updates the storage configuration.

**Parameters:**
- `config` (*Config): New configuration settings

**Example:**
```go
config := mindb.DefaultConfig()
config.MaxConcurrentUploads = 16
config.MaxConcurrentDownloads = 32
config.BufferSize = 1024 * 1024 // 1MB
config.UseMmap = true
config.EnableChecksumVerify = true

storage.SetConfig(config)
```

### GetConfig() *Config

Gets the current storage configuration.

**Returns:**
- `*Config`: Current configuration

**Example:**
```go
config := storage.GetConfig()
fmt.Printf("Current Configuration:\n")
fmt.Printf("  Max Concurrent Uploads: %d\n", config.MaxConcurrentUploads)
fmt.Printf("  Max Concurrent Downloads: %d\n", config.MaxConcurrentDownloads)
fmt.Printf("  Buffer Size: %d bytes\n", config.BufferSize)
fmt.Printf("  Use Memory Mapping: %t\n", config.UseMmap)
fmt.Printf("  Enable Checksum Verify: %t\n", config.EnableChecksumVerify)
```

### DefaultConfig() *Config

Gets the default configuration settings.

**Returns:**
- `*Config`: Default configuration

**Example:**
```go
config := mindb.DefaultConfig()
// Modify as needed
config.BufferSize = 512 * 1024 // 512KB
storage.SetConfig(config)
```

## Utility Functions

### CalculateETag(data []byte) string

Calculates the ETag (MD5 hash) for the given data.

**Parameters:**
- `data` ([]byte): Data to hash

**Returns:**
- `string`: Hex-encoded MD5 hash

**Example:**
```go
data := []byte("Hello, World!")
etag := mindb.CalculateETag(data)
fmt.Printf("ETag: %s\n", etag) // Output: ETag: 65a8e27d8879283831b664bd8b7f0ad4
```

### CalculateMultipartETag(etags []string) string

Calculates the ETag for a multipart upload from individual part ETags.

**Parameters:**
- `etags` ([]string): Slice of part ETags

**Returns:**
- `string`: Combined ETag with part count

**Example:**
```go
partETags := []string{
    "5d41402abc4b2a76b9719d911017c592",
    "7d865e959b2466918c9863afca942d0f",
    "98f6bcd4621d373cade4e832627b4f6",
}

multipartETag := mindb.CalculateMultipartETag(partETags)
fmt.Printf("Multipart ETag: %s\n", multipartETag)
// Output: Multipart ETag: 9bb58f26192e4ba00f01e2e7b136bbd8-3
```

### GenerateUploadID(bucket, key string) string

Generates a unique upload ID for multipart uploads.

**Parameters:**
- `bucket` (string): Bucket name
- `key` (string): Object key

**Returns:**
- `string`: Unique upload ID

**Example:**
```go
uploadID := mindb.GenerateUploadID("my-bucket", "large-file.zip")
fmt.Printf("Upload ID: %s\n", uploadID)
```

## Maintenance Operations

### Compact() error

Performs storage compaction to clean up fragmented space and optimize performance.

**Returns:**
- `error`: Error if compaction fails

**Example:**
```go
fmt.Println("Starting compaction...")
if err := storage.Compact(); err != nil {
    log.Printf("Compaction failed: %v", err)
} else {
    fmt.Println("Compaction completed successfully")
}
```

**Compaction includes:**
- Cleanup of empty directories
- Removal of temporary files
- Cleanup of expired multipart uploads
- Memory optimization

### Backup(backupPath string) error

Creates a backup of the entire storage system.

**Parameters:**
- `backupPath` (string): Destination path for backup

**Returns:**
- `error`: Error if backup fails

**Example:**
```go
backupPath := "./backup-" + time.Now().Format("2006-01-02-15-04-05")
fmt.Printf("Creating backup at: %s\n", backupPath)

if err := storage.Backup(backupPath); err != nil {
    log.Printf("Backup failed: %v", err)
} else {
    fmt.Println("Backup completed successfully")
}
```

### Restore(backupPath string) error

Restores the storage system from a backup.

**Parameters:**
- `backupPath` (string): Source path of backup

**Returns:**
- `error`: Error if restore fails

**Example:**
```go
backupPath := "./backup-2024-01-01-12-00-00"
fmt.Printf("Restoring from backup: %s\n", backupPath)

if err := storage.Restore(backupPath); err != nil {
    log.Printf("Restore failed: %v", err)
} else {
    fmt.Println("Restore completed successfully")
}
```

### Validate() error

Validates the integrity of the entire storage system.

**Returns:**
- `error`: Error if validation fails

**Example:**
```go
fmt.Println("Starting validation...")
if err := storage.Validate(); err != nil {
    log.Printf("Validation failed: %v", err)
} else {
    fmt.Println("Validation completed successfully")
}
```

**Validation includes:**
- File integrity checks
- Metadata consistency
- ETag verification
- Directory structure validation

## Data Types

### ObjectData
```go
type ObjectData struct {
    Key          string            `json:"key"`          // Object key/name
    Data         []byte            `json:"-"`            // Object data (not serialized)
    ContentType  string            `json:"contentType"`  // MIME type
    LastModified time.Time         `json:"lastModified"` // Last modification time
    ETag         string            `json:"etag"`         // Entity tag (MD5 hash)
    Metadata     map[string]string `json:"metadata"`     // Custom metadata
    Size         int64             `json:"size"`         // Object size in bytes
}
```

### BucketInfo
```go
type BucketInfo struct {
    Name         string    `json:"name"`         // Bucket name
    CreationDate time.Time `json:"creationDate"` // Creation timestamp
}
```

### ObjectInfo
```go
type ObjectInfo struct {
    Key          string    `json:"key"`          // Object key
    Size         int64     `json:"size"`         // Size in bytes
    LastModified time.Time `json:"lastModified"` // Last modification time
    ETag         string    `json:"etag"`         // Entity tag
}
```

### MultipartPart
```go
type MultipartPart struct {
    PartNumber int    `json:"partNumber"` // Part number (1-10000)
    ETag       string `json:"etag"`       // Part ETag
}
```

### MultipartUploadInfo
```go
type MultipartUploadInfo struct {
    Bucket      string            `json:"bucket"`      // Bucket name
    Key         string            `json:"key"`         // Object key
    UploadID    string            `json:"uploadId"`    // Upload ID
    ContentType string            `json:"contentType"` // MIME type
    Metadata    map[string]string `json:"metadata"`    // Object metadata
    CreatedAt   time.Time         `json:"createdAt"`   // Upload creation time
}
```

### PartInfo
```go
type PartInfo struct {
    PartNumber   int       `json:"partNumber"`   // Part number
    ETag         string    `json:"etag"`         // Part ETag
    Size         int       `json:"size"`         // Part size in bytes
    LastModified time.Time `json:"lastModified"` // Upload time
}
```

### Config
```go
type Config struct {
    // Concurrency settings
    MaxConcurrentUploads   int           // Max concurrent upload operations
    MaxConcurrentDownloads int           // Max concurrent download operations
    
    // Cache settings
    MetadataCacheSize      int           // Metadata cache size
    MetadataCacheTTL       time.Duration // Metadata cache TTL
    
    // Performance settings
    UseDirectIO            bool          // Use direct I/O (bypass OS cache)
    UseMmap                bool          // Use memory mapping for reads
    BufferSize             int           // I/O buffer size in bytes
    
    // Data integrity
    EnableChecksumVerify   bool          // Enable checksum verification
    ChecksumAlgorithm      string        // Checksum algorithm (md5)
    
    // Cleanup settings
    CleanupInterval        time.Duration // Background cleanup interval
    TempFileMaxAge         time.Duration // Max age for temp files
}
```

### Stats
```go
type Stats struct {
    BucketCount int   `json:"bucketCount"` // Total number of buckets
    ObjectCount int   `json:"objectCount"` // Total number of objects
    TotalSize   int64 `json:"totalSize"`   // Total storage size in bytes
}
```

### Metrics
```go
type Metrics struct {
    // Operation counters
    ReadOps   int64 `json:"readOps"`   // Total read operations
    WriteOps  int64 `json:"writeOps"`  // Total write operations
    DeleteOps int64 `json:"deleteOps"` // Total delete operations
    ListOps   int64 `json:"listOps"`   // Total list operations
    
    // Byte counters
    ReadBytes  int64 `json:"readBytes"`  // Total bytes read
    WriteBytes int64 `json:"writeBytes"` // Total bytes written
    
    // Error tracking
    ErrorCount int64 `json:"errorCount"` // Total error count
    
    // Performance metrics
    AvgReadLatency  int64 `json:"avgReadLatency"`  // Average read latency (nanoseconds)
    AvgWriteLatency int64 `json:"avgWriteLatency"` // Average write latency (nanoseconds)
    
    // Concurrency metrics
    ActiveReads  int64 `json:"activeReads"`  // Current active read operations
    ActiveWrites int64 `json:"activeWrites"` // Current active write operations
}
```

## Error
## Error Handling

The mindb storage system provides detailed error information to help with debugging and error handling.

### Common Error Types

#### Storage Errors
- **Bucket not found**: When trying to access a non-existent bucket
- **Bucket already exists**: When creating a bucket that already exists
- **Bucket not empty**: When trying to delete a non-empty bucket
- **Object not found**: When trying to access a non-existent object

#### I/O Errors
- **Permission denied**: Insufficient file system permissions
- **Disk full**: Not enough disk space for operation
- **File corruption**: Data integrity check failures

#### Validation Errors
- **Invalid bucket name**: Bucket name doesn't meet requirements
- **Invalid part number**: Part number outside valid range (1-10000)
- **ETag mismatch**: Data corruption detected during verification

### Error Handling Examples

```go
// Handle specific error types
err := storage.CreateBucket("my-bucket")
if err != nil {
    if strings.Contains(err.Error(), "already exists") {
        fmt.Println("Bucket already exists, continuing...")
    } else {
        log.Fatalf("Failed to create bucket: %v", err)
    }
}

// Handle object not found
object, err := storage.GetObject("my-bucket", "missing-file.txt")
if err != nil {
    if strings.Contains(err.Error(), "not found") {
        fmt.Println("Object doesn't exist")
    } else {
        log.Printf("Error retrieving object: %v", err)
    }
}

// Handle multipart upload errors with cleanup
uploadID, err := storage.CreateMultipartUpload("bucket", "key", "type", nil)
if err != nil {
    log.Printf("Failed to create upload: %v", err)
    return
}

// Always clean up on error
defer func() {
    if err != nil {
        if abortErr := storage.AbortMultipartUpload("bucket", "key", uploadID); abortErr != nil {
            log.Printf("Failed to abort upload: %v", abortErr)
        }
    }
}()
```

## Logger Interface

The storage system requires a logger implementation for system events and debugging.

### Logger Interface Definition

```go
type Logger interface {
    Info(args ...interface{})
    Infof(format string, args ...interface{})
    Warn(args ...interface{})
    Warnf(format string, args ...interface{})
    Error(args ...interface{})
    Errorf(format string, args ...interface{})
    Debug(args ...interface{})
    Debugf(format string, args ...interface{})
}
```

### Simple Logger Implementation

```go
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
```

### Structured Logger Implementation (with logrus)

```go
import "github.com/sirupsen/logrus"

type StructuredLogger struct {
    logger *logrus.Logger
}

func NewStructuredLogger() *StructuredLogger {
    logger := logrus.New()
    logger.SetFormatter(&logrus.JSONFormatter{})
    return &StructuredLogger{logger: logger}
}

func (l *StructuredLogger) Info(args ...interface{}) {
    l.logger.Info(args...)
}

func (l *StructuredLogger) Infof(format string, args ...interface{}) {
    l.logger.Infof(format, args...)
}

// ... implement other methods similarly
```

## Best Practices

### 1. Resource Management

Always close resources properly:

```go
// Close storage instance
defer storage.Close()

// Close streams
reader, metadata, err := storage.GetObjectStream("bucket", "key")
if err == nil {
    defer reader.Close()
    // Use reader...
}
```

### 2. Error Handling

Handle errors appropriately and clean up resources:

```go
uploadID, err := storage.CreateMultipartUpload("bucket", "key", "type", nil)
if err != nil {
    return err
}

defer func() {
    if err != nil {
        storage.AbortMultipartUpload("bucket", "key", uploadID)
    }
}()
```

### 3. Large File Handling

Use streaming operations for large files:

```go
// For large uploads
etag, err := storage.PutObjectStream("bucket", "key", reader, size, "type", metadata)

// For large downloads
reader, metadata, err := storage.GetObjectStream("bucket", "key")
```

### 4. Multipart Upload Guidelines

- Use multipart upload for files larger than 100MB
- Use part sizes between 5MB and 5GB
- Keep part sizes consistent (except the last part)
- Always handle errors and clean up incomplete uploads

```go
const partSize = 5 * 1024 * 1024 // 5MB

if fileSize > 100*1024*1024 { // 100MB
    // Use multipart upload
    uploadID, err := storage.CreateMultipartUpload(bucket, key, contentType, metadata)
    // ... handle multipart upload
} else {
    // Use regular upload
    err := storage.PutObject(bucket, objectData)
}
```

### 5. Performance Optimization

Configure the storage system for your use case:

```go
config := mindb.DefaultConfig()

// For high-throughput scenarios
config.MaxConcurrentUploads = runtime.NumCPU() * 4
config.MaxConcurrentDownloads = runtime.NumCPU() * 8
config.BufferSize = 1024 * 1024 // 1MB

// For memory-constrained environments
config.UseMmap = false
config.BufferSize = 64 * 1024 // 64KB

// For maximum data integrity
config.EnableChecksumVerify = true

storage.SetConfig(config)
```

### 6. Monitoring and Maintenance

Regularly monitor system health and perform maintenance:

```go
// Health monitoring
go func() {
    ticker := time.NewTicker(5 * time.Minute)
    defer ticker.Stop()
    
    for range ticker.C {
        if err := storage.HealthCheck(); err != nil {
            log.Printf("Health check failed: %v", err)
            // Send alert or take corrective action
        }
        
        metrics := storage.GetMetrics()
        if metrics.ErrorCount > 0 {
            log.Printf("Detected %d errors", metrics.ErrorCount)
        }
    }
}()

// Periodic maintenance
go func() {
    ticker := time.NewTicker(24 * time.Hour)
    defer ticker.Stop()
    
    for range ticker.C {
        if err := storage.Compact(); err != nil {
            log.Printf("Compaction failed: %v", err)
        }
    }
}()
```

### 7. Backup Strategy

Implement regular backups:

```go
func performBackup(storage *mindb.DB) error {
    backupPath := fmt.Sprintf("./backup-%s", time.Now().Format("2006-01-02-15-04-05"))
    
    log.Printf("Starting backup to: %s", backupPath)
    if err := storage.Backup(backupPath); err != nil {
        return fmt.Errorf("backup failed: %w", err)
    }
    
    log.Printf("Backup completed successfully")
    return nil
}

// Schedule daily backups
go func() {
    ticker := time.NewTicker(24 * time.Hour)
    defer ticker.Stop()
    
    for range ticker.C {
        if err := performBackup(storage); err != nil {
            log.Printf("Scheduled backup failed: %v", err)
        }
    }
}()
```

## Constants and Limits

### Size Limits
- Maximum object size: Limited by available disk space
- Maximum part size (multipart): 5GB
- Minimum part size (multipart): 5MB (except last part)
- Maximum parts per upload: 10,000

### Naming Conventions
- Bucket names: Must be valid directory names
- Object keys: Can contain any UTF-8 characters except null
- Upload IDs: Generated automatically, guaranteed unique

### Performance Characteristics
- Concurrent operations: Configurable (default: CPU cores × 4 for uploads)
- Buffer sizes: Configurable (default: 64KB)
- Cleanup interval: Configurable (default: 30 minutes)
- Multipart upload lifetime: 7 days (matches S3)

## Version Compatibility

This API reference is for MinDB storage system v1.0+. 

### Breaking Changes
- None in current version

### Deprecated Features
- None in current version

### Future Enhancements
- Encryption at rest
- Compression support
- Replication features
- Advanced caching mechanisms

For the latest updates and changes, see the [CHANGELOG.md](../CHANGELOG.md) file.