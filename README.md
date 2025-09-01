# MinDB - Embedded Object Storage Database for Go

[![Go Version](https://img.shields.io/badge/go-%3E%3D1.21-blue.svg)](https://golang.org/)
[![License](https://img.shields.io/badge/license-Apache%202.0-green.svg)](LICENSE)
[![Build Status](https://img.shields.io/badge/build-passing-brightgreen.svg)]()
[![Coverage](https://img.shields.io/badge/coverage-85%25-yellow.svg)]()
[![Go Report Card](https://goreportcard.com/badge/github.com/elastic-io/mindb)](https://goreportcard.com/report/github.com/elastic-io/mindb)

MinDB is a high-performance embedded object storage database for Go applications with S3-compatible API. It provides a lightweight, zero-dependency solution for applications that need reliable object storage without external dependencies.

## Why MinDB?

- **🚀 Embedded**: No external services required - embed directly in your Go application
- **📦 Zero Dependencies**: Pure Go implementation with minimal external dependencies
- **🔄 S3 Compatible**: Drop-in replacement for S3 operations in development and testing
- **⚡ High Performance**: Optimized for concurrent operations with intelligent caching
- **🛡️ Data Integrity**: Built-in checksums and atomic operations ensure data safety
- **📊 Observable**: Built-in metrics and health monitoring
- **🔧 Configurable**: Flexible configuration for different use cases

## Use Cases

- **Development & Testing**: S3-compatible local storage for development environments
- **Edge Computing**: Embedded storage for edge applications and IoT devices
- **Microservices**: Local object storage for containerized applications
- **Backup Systems**: Reliable local storage with S3-compatible interface
- **Content Management**: File storage for web applications and CMS systems
- **Data Processing**: Temporary storage for data processing pipelines

## Quick Start

### Installation

```bash
go get github.com/elastic-io/mindb
```

### Basic Usage

```go
package main

import (
    "fmt"
    "log"
    
    "github.com/elastic-io/mindb"
)

func main() {
    // Create embedded storage instance
    storage, err := mindb.New("./data")
    if err != nil {
        log.Fatal(err)
    }
    defer storage.Close()
    
    // Create a bucket
    err = storage.CreateBucket("my-app-data")
    if err != nil {
        log.Fatal(err)
    }
    
    // Store an object
    obj := &mindb.ObjectData{
        Key:         "config/app.json",
        Data:        []byte(`{"version": "1.0", "debug": true}`),
        ContentType: "application/json",
        Metadata:    map[string]string{"app": "myapp"},
    }
    
    err = storage.PutObject("my-app-data", obj)
    if err != nil {
        log.Fatal(err)
    }
    
    // Retrieve the object
    retrievedObj, err := storage.GetObject("my-app-data", "config/app.json")
    if err != nil {
        log.Fatal(err)
    }
    
    fmt.Printf("Retrieved config: %s\n", string(retrievedObj.Data))
    fmt.Printf("Metadata: %v\n", retrievedObj.Metadata)
}
```

### Web Application Example

```go
package main

import (
    "io"
    "net/http"
    
    "github.com/elastic-io/mindb"
)

func main() {
    // Initialize embedded storage
    storage, err := mindb.New("./uploads")
    if err != nil {
        panic(err)
    }
    defer storage.Close()
    
    storage.CreateBucket("user-uploads")
    
    // File upload handler
    http.HandleFunc("/upload", func(w http.ResponseWriter, r *http.Request) {
        file, header, err := r.FormFile("file")
        if err != nil {
            http.Error(w, err.Error(), http.StatusBadRequest)
            return
        }
        defer file.Close()
        
        data, err := io.ReadAll(file)
        if err != nil {
            http.Error(w, err.Error(), http.StatusInternalServerError)
            return
        }
        
        obj := &mindb.ObjectData{
            Key:         header.Filename,
            Data:        data,
            ContentType: header.Header.Get("Content-Type"),
        }
        
        err = storage.PutObject("user-uploads", obj)
        if err != nil {
            http.Error(w, err.Error(), http.StatusInternalServerError)
            return
        }
        
        w.WriteHeader(http.StatusCreated)
        w.Write([]byte("File uploaded successfully"))
    })
    
    // File download handler
    http.HandleFunc("/download/", func(w http.ResponseWriter, r *http.Request) {
        filename := r.URL.Path[10:] // Remove "/download/"
        
        obj, err := storage.GetObject("user-uploads", filename)
        if err != nil {
            http.Error(w, "File not found", http.StatusNotFound)
            return
        }
        
        w.Header().Set("Content-Type", obj.ContentType)
        w.Header().Set("Content-Disposition", "attachment; filename="+filename)
        w.Write(obj.Data)
    })
    
    http.ListenAndServe(":8080", nil)
}
```

## Features

### Core Database Features
- **Embedded Architecture**: Runs directly in your Go process
- **ACID Compliance**: Atomic operations with consistency guarantees
- **Concurrent Access**: Thread-safe operations with fine-grained locking
- **Data Integrity**: MD5 checksums and verification
- **Efficient Storage**: Hash-based sharding and optimized file organization

### S3-Compatible API
- **Bucket Operations**: Create, delete, list buckets
- **Object Operations**: Put, get, delete, list objects with metadata
- **Multipart Uploads**: Large file uploads with resumable transfers
- **Range Requests**: Partial content delivery
- **Streaming Operations**: Memory-efficient handling of large files

### Performance & Reliability
- **High Throughput**: Optimized for concurrent operations
- **Memory Efficient**: Buffer pooling and streaming operations
- **Background Cleanup**: Automatic maintenance and garbage collection
- **Health Monitoring**: Built-in metrics and health checks
- **Configurable**: Tunable for different workloads

## API Reference

### Database Initialization

```go
// Basic initialization
storage, err := mindb.New("/path/to/data", logger)

// With custom configuration
config := &mindb.Config{
    MaxConcurrentUploads:   16,
    MaxConcurrentDownloads: 32,
    BufferSize:            64 * 1024,
    EnableChecksumVerify:  true,
    CleanupInterval:       30 * time.Minute,
}

storage, err := mindb.New("/path/to/data", logger)
storage.SetConfig(config)
```

### Bucket Operations

```go
// Create bucket
err := storage.CreateBucket("my-bucket")

// List buckets
buckets, err := storage.ListBuckets()

// Check bucket existence
exists, err := storage.BucketExists("my-bucket")

// Delete bucket (must be empty)
err := storage.DeleteBucket("my-bucket")
```

### Object Operations

```go
// Store object
obj := &mindb.ObjectData{
    Key:         "documents/readme.txt",
    Data:        []byte("Hello World"),
    ContentType: "text/plain",
    Metadata:    map[string]string{"author": "user1"},
}
err := storage.PutObject("my-bucket", obj)

// Retrieve object
obj, err := storage.GetObject("my-bucket", "documents/readme.txt")

// Delete object
err := storage.DeleteObject("my-bucket", "documents/readme.txt")

// List objects with prefix
objects, prefixes, err := storage.ListObjects("my-bucket", "documents/", "", "", 100)
```

### Large File Handling

```go
// Streaming upload for large files
reader := bytes.NewReader(largeData)
etag, err := storage.PutObjectStream("bucket", "large-file.zip", 
    reader, int64(len(largeData)), "application/zip", nil)

// Streaming download
stream, metadata, err := storage.GetObjectStream("bucket", "large-file.zip")
defer stream.Close()

// Multipart upload for very large files
uploadID, err := storage.CreateMultipartUpload("bucket", "huge-file.dat", 
    "application/octet-stream", nil)

// Upload parts (5MB minimum per part)
var parts []mindb.MultipartPart
for i, chunk := range fileChunks {
    etag, err := storage.UploadPart("bucket", "huge-file.dat", uploadID, i+1, chunk)
    parts = append(parts, mindb.MultipartPart{PartNumber: i+1, ETag: etag})
}

// Complete upload
finalETag, err := storage.CompleteMultipartUpload("bucket", "huge-file.dat", uploadID, parts)
```

### Monitoring & Health

```go
// Get storage statistics
stats, err := storage.GetStats()
fmt.Printf("Buckets: %d, Objects: %d, Size: %d bytes\n", 
    stats.BucketCount, stats.ObjectCount, stats.TotalSize)

// Performance metrics
metrics := storage.GetMetrics()
fmt.Printf("Operations: R:%d W:%d D:%d, Errors: %d\n",
    metrics.ReadOps, metrics.WriteOps, metrics.DeleteOps, metrics.ErrorCount)

// Health check
if err := storage.HealthCheck(); err != nil {
    log.Printf("Storage health issue: %v", err)
}

// Advanced monitoring
monitor := mindb.NewPerformanceMonitor(storage)
monitor.AddAlertCallback(func(alert mindb.Alert) {
    log.Printf("ALERT: %s - %s", alert.Type, alert.Message)
})
monitor.Start(1 * time.Minute)
```

## Configuration

### Storage Configuration

```go
config := &mindb.Config{
    // Concurrency limits
    MaxConcurrentUploads:   runtime.NumCPU() * 4,
    MaxConcurrentDownloads: runtime.NumCPU() * 8,
    
    // Performance tuning
    BufferSize:           64 * 1024,  // 64KB buffer
    UseDirectIO:          false,      // Direct I/O bypass
    UseMmap:              true,       // Memory mapping
    
    // Data integrity
    EnableChecksumVerify: true,       // MD5 verification
    ChecksumAlgorithm:    "md5",      // Checksum algorithm
    
    // Maintenance
    CleanupInterval:      30 * time.Minute,
    TempFileMaxAge:       2 * time.Hour,
}
```

### Directory Structure

The embedded database creates the following structure:

```
data-directory/
├── buckets/             # Object data storage
│   └── bucket-name/
│       └── ab/cd/       # Hash-based sharding (ab/cd/object-key)
├── .db.sys/             # System directory
│   ├── buckets/         # Bucket metadata
│   ├── multipart/       # Multipart upload state
│   └── tmp/             # Temporary files
```

## Performance

### Benchmarks

Performance on Apple M2 Pro (ARM64, 16GB RAM, NVMe SSD):

**Single-threaded Operations**
| Operation | Object Size | Throughput | Latency | Ops/sec |
|-----------|-------------|------------|---------|---------|
| Put | 1KB | 0.13 MB/s | 8.0ms | 124 |
| Put | 64KB | 8.08 MB/s | 8.1ms | 123 |
| Put | 1MB | 85.32 MB/s | 12.3ms | 81 |
| Put | 10MB | 253.06 MB/s | 41.4ms | 24 |
| Put | 100MB | 303.52 MB/s | 345.5ms | 3 |
| Get | 1KB | 5.44 MB/s | 188μs | 5,312 |
| Get | 64KB | 229.69 MB/s | 285μs | 3,504 |
| Get | 1MB | 490.47 MB/s | 2.1ms | 502 |
| Get | 10MB | 627.13 MB/s | 16.7ms | 60 |
| Get | 100MB | 615.08 MB/s | 170.5ms | 6 |

**Concurrent Operations (1MB objects)**
| Concurrency | Put Throughput | Get Throughput |
|-------------|----------------|----------------|
| 1 | 88.81 MB/s | 443.13 MB/s |
| 2 | 158.58 MB/s | 762.40 MB/s |
| 4 | 185.07 MB/s | 1,270.31 MB/s |
| 8 | 167.52 MB/s | 2,470.38 MB/s |
| 16 | 139.81 MB/s | 2,841.09 MB/s |
| 32 | 133.75 MB/s | 3,248.64 MB/s |

**Streaming Operations**
| Operation | Object Size | Throughput | Latency |
|-----------|-------------|------------|---------|
| Stream Put | 1MB | 85.48 MB/s | 12.3ms |
| Stream Put | 10MB | 250.89 MB/s | 41.8ms |
| Stream Put | 100MB | 307.55 MB/s | 341ms |
| Stream Get | 1MB | 6,338.74 MB/s | 165μs |
| Stream Get | 10MB | 17,023.44 MB/s | 616μs |
| Stream Get | 100MB | 13,891.45 MB/s | 7.5ms |

**Multipart Upload**
| Operation | File Size | Throughput | Latency |
|-----------|-----------|------------|---------|
| Multipart Upload | 50MB file (5MB parts) | 162.24 MB/s | 323ms |

### Performance Characteristics

#### Excellent Read Performance
- **Streaming reads** show exceptional performance (up to 17GB/s for 10MB files)
- **Concurrent reads** scale very well with increased concurrency
- **Small object reads** achieve over 5,000 ops/sec

#### Optimized Write Performance
- **Large file writes** achieve 300+ MB/s throughput
- **Concurrent writes** show good scaling up to 4-8 threads
- **Write performance** optimized for larger objects

#### Memory Efficiency
- **Streaming operations** maintain constant memory usage
- **Buffer pooling** reduces allocation overhead
- **Memory usage** scales predictably with concurrent operations

### Platform-Specific Notes

#### Apple Silicon (M2/M3) Performance
- **Exceptional read performance** due to unified memory architecture
- **Good write performance** with NVMe storage
- **Excellent concurrent scaling** for read operations

#### Intel x86_64 Performance
- **Balanced read/write performance**
- **Good scaling** across different workloads
- **Consistent performance** across object sizes

### Memory Usage

- **Base overhead**: ~10MB for the storage engine
- **Per object metadata**: ~200 bytes
- **Buffer pools**: Configurable (default 64KB × CPU cores)
- **Streaming operations**: Constant memory usage regardless of file size
- **Concurrent operations**: Linear memory scaling with active operations

### Optimization Tips

1. **For Read-Heavy Workloads**:
   ```go
   config.MaxConcurrentDownloads = runtime.NumCPU() * 16  // High concurrency
   config.UseMmap = true                                   // Enable memory mapping
   config.BufferSize = 256 * 1024                         // Larger buffers
   ```

2. **For Write-Heavy Workloads**:
   ```go
   config.MaxConcurrentUploads = runtime.NumCPU() * 4     // Moderate concurrency
   config.UseDirectIO = true                               // Bypass OS cache
   config.EnableChecksumVerify = false                     // Disable for speed
   ```

3. **For Large Files**:
   ```go
   // Use streaming operations
   etag, err := storage.PutObjectStream(bucket, key, reader, size, contentType, metadata)
   
   // Use multipart for files > 100MB
   if size > 100*1024*1024 {
       uploadID, err := storage.CreateMultipartUpload(bucket, key, contentType, metadata)
       // Upload in 5-10MB parts
   }
   ```

4. **For Small Files**:
   ```go
   config.BufferSize = 32 * 1024                          // Smaller buffers
   config.MaxConcurrentUploads = runtime.NumCPU() * 8     // Higher concurrency
   ```

5. **Memory Optimization**:
   ```go
   config.UseMmap = false                                  // Disable mmap for memory-constrained environments
   config.BufferSize = 16 * 1024                          // Smaller buffers
   config.MetadataCacheSize = 1000                         // Smaller cache
   ```

### Benchmark Environment

The benchmarks were run on:
- **CPU**: Apple M2 Pro (12-core)
- **Memory**: 16GB unified memory
- **Storage**: NVMe SSD
- **OS**: macOS (darwin/arm64)
- **Go**: 1.21+

For different hardware configurations, performance may vary. Run the included benchmark suite to measure performance on your specific environment:

```bash
make benchmark
```

## Comparison with Alternatives

| Feature | MinDB | SQLite + BLOBs | BadgerDB | File System |
|---------|------|----------------|----------|-------------|
| S3 API | ✅ | ❌ | ❌ | ❌ |
| Embedded | ✅ | ✅ | ✅ | ✅ |
| Large Files | ✅ | ⚠️ | ❌ | ✅ |
| Metadata | ✅ | ✅ | ✅ | ❌ |
| Transactions | ✅ | ✅ | ✅ | ❌ |
| Streaming | ✅ | ❌ | ❌ | ✅ |
| Multipart | ✅ | ❌ | ❌ | ❌ |

## Examples

See the [examples](examples/) directory for complete examples:

- [Basic Usage](examples/basic/main.go) - Simple object storage operations
- [Web Server](examples/webserver/webserver.go) - File upload/download server
- [Performance Test](examples/performance/main.go) - Performance testing and monitoring

## Contributing

We welcome contributions! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.