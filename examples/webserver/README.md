# Web Server Example

This example demonstrates how to build a REST API server using the MinDB storage system.

## Features

- RESTful API for bucket and object operations
- Health check endpoint
- Storage statistics
- Request logging
- Metadata support via HTTP headers

## Running the server

```bash
cd examples/webserver
go run main.go [storage-path] [port]
```

Default values:
- storage-path: `./webserver-data`
- port: `8080`

## API Endpoints

### Health Check
```bash
GET /health
```

### Storage Statistics
```bash
GET /stats
```

### Bucket Operations
```bash
# List buckets
GET /buckets

# Create bucket
POST /buckets/{bucket-name}

# Delete bucket
DELETE /buckets/{bucket-name}
```

### Object Operations
```bash
# List objects in bucket
GET /objects/{bucket-name}?prefix=&marker=&delimiter=&max-keys=

# Get object
GET /objects/{bucket-name}/{object-key}

# Put object (with metadata)
PUT /objects/{bucket-name}/{object-key}
Content-Type: text/plain
X-Meta-Author: john
X-Meta-Version: 1.0

# Delete object
DELETE /objects/{bucket-name}/{object-key}
```

## Example Usage

```bash
# Start the server
go run main.go

# Create a bucket
curl -X POST http://localhost:8080/buckets/my-bucket

# Upload a file with metadata
curl -X PUT http://localhost:8080/objects/my-bucket/hello.txt \
  -H "Content-Type: text/plain" \
  -H "X-Meta-Author: john" \
  -H "X-Meta-Version: 1.0" \
  -d "Hello, World!"

# Download the file
curl http://localhost:8080/objects/my-bucket/hello.txt

# List objects
curl http://localhost:8080/objects/my-bucket

# Get statistics
curl http://localhost:8080/stats

# Health check
curl http://localhost:8080/health
```

## Response Examples

### Health Check Response
```json
{
  "status": "healthy",
  "metrics": {
    "readOps": 150,
    "writeOps": 75,
    "errorCount": 0
  }
}
```

### List Buckets Response
```json
[
  {
    "name": "my-bucket",
    "creationDate": "2024-01-01T12:00:00Z"
  }
]
```

### List Objects Response
```json
{
  "objects": [
    {
      "key": "hello.txt",
      "size": 13,
      "lastModified": "2024-01-01T12:00:00Z",
      "etag": "65a8e27d8879283831b664bd8b7f0ad4"
    }
  ],
  "commonPrefixes": []
}
```

### Statistics Response
```json
{
  "storage": {
    "bucketCount": 1,
    "objectCount": 5,
    "totalSize": 1024
  },
  "metrics": {
    "readOps": 150,
    "writeOps": 75,
    "deleteOps": 10,
    "listOps": 25,
    "readBytes": 51200,
    "writeBytes": 25600,
    "errorCount": 0
  },
  "disk": {
    "total": 1000000000,
    "free": 800000000,
    "used": 200000000
  }
}
```

## Error Responses

The server returns appropriate HTTP status codes:

- `200 OK` - Success
- `201 Created` - Resource created
- `400 Bad Request` - Invalid request
- `404 Not Found` - Resource not found
- `409 Conflict` - Resource already exists or bucket not empty
- `500 Internal Server Error` - Server error
- `503 Service Unavailable` - Health check failed

## Metadata Headers

Custom metadata can be set using `X-Meta-` headers:

```bash
curl -X PUT http://localhost:8080/objects/my-bucket/document.pdf \
  -H "Content-Type: application/pdf" \
  -H "X-Meta-Author: john.doe" \
  -H "X-Meta-Department: engineering" \
  -H "X-Meta-Version: 1.2" \
  --data-binary @document.pdf
```

The metadata will be returned in response headers when retrieving the object.

## Query Parameters

### List Objects Parameters

- `prefix` - Only return objects whose keys begin with this prefix
- `marker` - Start listing after this key
- `delimiter` - Character used to group keys
- `max-keys` - Maximum number of keys to return (default: 1000)

Example:
```bash
curl "http://localhost:8080/objects/my-bucket?prefix=docs/&max-keys=50"
```

## Server Logs

The server provides detailed logging for all operations:

```
[INFO] Starting server on port 8080 with storage path: ./webserver-data
[INFO] API endpoints:
[INFO]   GET    /health           - Health check
[INFO]   GET    /stats            - Storage statistics
[INFO]   GET    /buckets          - List buckets
[INFO]   POST   /buckets/{name}   - Create bucket
[INFO]   DELETE /buckets/{name}   - Delete bucket
[INFO]   GET    /objects/{bucket} - List objects
[INFO]   GET    /objects/{bucket}/{key} - Get object
[INFO]   PUT    /objects/{bucket}/{key} - Put object
[INFO]   DELETE /objects/{bucket}/{key} - Delete object
[INFO] POST /buckets/my-bucket 2.5ms
[INFO] PUT /objects/my-bucket/hello.txt 15.2ms
[INFO] GET /objects/my-bucket/hello.txt 1.8ms
```

## Testing with Different Tools

### Using curl
```bash
# Test all endpoints
curl -X POST http://localhost:8080/buckets/test-bucket
curl -X PUT http://localhost:8080/objects/test-bucket/file.txt -d "test data"
curl http://localhost:8080/objects/test-bucket/file.txt
curl http://localhost:8080/objects/test-bucket
curl http://localhost:8080/stats
curl -X DELETE http://localhost:8080/objects/test-bucket/file.txt
curl -X DELETE http://localhost:8080/buckets/test-bucket
```

### Using HTTPie
```bash
# Install: pip install httpie
http POST localhost:8080/buckets/test-bucket
http PUT localhost:8080/objects/test-bucket/file.txt Content-Type:text/plain < file.txt
http GET localhost:8080/objects/test-bucket/file.txt
http GET localhost:8080/stats
```

### Using Postman
1. Import the API endpoints
2. Set base URL to `http://localhost:8080`
3. Test each endpoint with appropriate HTTP methods
4. Add custom headers for metadata

## Production Considerations

For production use, consider adding:

- Authentication and authorization
- HTTPS/TLS support
- Rate limiting
- Request validation
- CORS headers
- Compression
- Caching headers
- Monitoring and alerting

Example with basic authentication:
```go
func basicAuth(handler http.HandlerFunc) http.HandlerFunc {
    return func(w http.ResponseWriter, r *http.Request) {
        user, pass, ok := r.BasicAuth()
        if !ok || user != "admin" || pass != "password" {
            w.Header().Set("WWW-Authenticate", `Basic realm="Restricted"`)
            http.Error(w, "Unauthorized", http.StatusUnauthorized)
            return
        }
        handler(w, r)
    }
}
```