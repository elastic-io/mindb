# Basic Usage Example

This example demonstrates the basic operations of the MinDB storage system.

## What it does

1. Creates a storage instance
2. Creates a bucket
3. Stores an object with metadata
4. Retrieves the object
5. Lists objects in the bucket
6. Shows storage statistics
7. Performs a health check
8. Cleans up resources

## Running the example

```bash
cd examples/basic
go run main.go
```

## Expected output

```yaml
Creating bucket: my-bucket
Putting object: hello.txt
Getting object: hello.txt
Retrieved data: Hello, World!
Content type: text/plain
ETag: 65a8e27d8879283831b664bd8b7f0ad4
Metadata: map[author:example type:greeting]

Listing objects:
- hello.txt (size: 13, modified: 2024-01-01 12:00:00)

Storage statistics:
Buckets: 1
Objects: 1
Total size: 13 bytes

Performing health check:
Health check passed

Cleaning up...
Example completed successfully!
```