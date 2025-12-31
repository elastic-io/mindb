package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/elastic-io/mindb"
)

// SimpleLogger implements the mindb.Logger interface
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

func (l *SimpleLogger) Fatal(args ...interface{}) {
	log.Fatal("[FATAL]", fmt.Sprint(args...))
}

type Server struct {
	storage *mindb.DB
	logger  *SimpleLogger
}

func NewServer(storagePath string) (*Server, error) {
	logger := &SimpleLogger{}
	storage, err := mindb.NewWithLogger(storagePath, logger)
	if err != nil {
		return nil, err
	}

	return &Server{
		storage: storage,
		logger:  logger,
	}, nil
}

func (s *Server) Close() error {
	return s.storage.Close()
}

// Health check endpoint
func (s *Server) healthHandler(w http.ResponseWriter, r *http.Request) {
	if err := s.storage.HealthCheck(); err != nil {
		http.Error(w, fmt.Sprintf("Health check failed: %v", err), http.StatusServiceUnavailable)
		return
	}

	metrics := s.storage.GetMetrics()
	response := map[string]interface{}{
		"status":  "healthy",
		"metrics": metrics,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// List buckets
func (s *Server) listBucketsHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	buckets, err := s.storage.ListBuckets()
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to list buckets: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(buckets)
}

// Create bucket
func (s *Server) createBucketHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	bucket := strings.TrimPrefix(r.URL.Path, "/buckets/")
	if bucket == "" {
		http.Error(w, "Bucket name required", http.StatusBadRequest)
		return
	}

	if err := s.storage.CreateBucket(bucket); err != nil {
		if strings.Contains(err.Error(), "already exists") {
			http.Error(w, "Bucket already exists", http.StatusConflict)
		} else {
			http.Error(w, fmt.Sprintf("Failed to create bucket: %v", err), http.StatusInternalServerError)
		}
		return
	}

	w.WriteHeader(http.StatusCreated)
	fmt.Fprintf(w, "Bucket %s created successfully", bucket)
}

// Delete bucket
func (s *Server) deleteBucketHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	bucket := strings.TrimPrefix(r.URL.Path, "/buckets/")
	if bucket == "" {
		http.Error(w, "Bucket name required", http.StatusBadRequest)
		return
	}

	if err := s.storage.DeleteBucket(bucket); err != nil {
		if strings.Contains(err.Error(), "not found") {
			http.Error(w, "Bucket not found", http.StatusNotFound)
		} else if strings.Contains(err.Error(), "not empty") {
			http.Error(w, "Bucket not empty", http.StatusConflict)
		} else {
			http.Error(w, fmt.Sprintf("Failed to delete bucket: %v", err), http.StatusInternalServerError)
		}
		return
	}

	fmt.Fprintf(w, "Bucket %s deleted successfully", bucket)
}

// List objects
func (s *Server) listObjectsHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	bucket := strings.TrimPrefix(r.URL.Path, "/objects/")
	if bucket == "" {
		http.Error(w, "Bucket name required", http.StatusBadRequest)
		return
	}

	prefix := r.URL.Query().Get("prefix")
	marker := r.URL.Query().Get("marker")
	delimiter := r.URL.Query().Get("delimiter")
	maxKeysStr := r.URL.Query().Get("max-keys")

	maxKeys := 1000
	if maxKeysStr != "" {
		if mk, err := strconv.Atoi(maxKeysStr); err == nil && mk > 0 {
			maxKeys = mk
		}
	}

	objects, commonPrefixes, err := s.storage.ListObjects(bucket, prefix, marker, delimiter, maxKeys)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to list objects: %v", err), http.StatusInternalServerError)
		return
	}

	response := map[string]interface{}{
		"objects":        objects,
		"commonPrefixes": commonPrefixes,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// Put object
func (s *Server) putObjectHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPut {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	parts := strings.SplitN(strings.TrimPrefix(r.URL.Path, "/objects/"), "/", 2)
	if len(parts) != 2 {
		http.Error(w, "Invalid path format. Use /objects/{bucket}/{key}", http.StatusBadRequest)
		return
	}

	bucket, key := parts[0], parts[1]

	// Read request body
	data, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to read request body: %v", err), http.StatusBadRequest)
		return
	}

	// Get content type
	contentType := r.Header.Get("Content-Type")
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	// Extract metadata from headers
	metadata := make(map[string]string)
	for name, values := range r.Header {
		if strings.HasPrefix(strings.ToLower(name), "x-meta-") {
			metaKey := strings.TrimPrefix(strings.ToLower(name), "x-meta-")
			if len(values) > 0 {
				metadata[metaKey] = values[0]
			}
		}
	}

	objectData := &mindb.ObjectData{
		Key:         key,
		Data:        data,
		ContentType: contentType,
		Metadata:    metadata,
	}

	if err := s.storage.PutObject(bucket, objectData); err != nil {
		http.Error(w, fmt.Sprintf("Failed to put object: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("ETag", objectData.ETag)
	w.WriteHeader(http.StatusCreated)
	fmt.Fprintf(w, "Object %s uploaded successfully", key)
}

// Get object
func (s *Server) getObjectHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	parts := strings.SplitN(strings.TrimPrefix(r.URL.Path, "/objects/"), "/", 2)
	if len(parts) != 2 {
		http.Error(w, "Invalid path format. Use /objects/{bucket}/{key}", http.StatusBadRequest)
		return
	}

	bucket, key := parts[0], parts[1]

	objectData, err := s.storage.GetObject(bucket, key)
	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			http.Error(w, "Object not found", http.StatusNotFound)
		} else {
			http.Error(w, fmt.Sprintf("Failed to get object: %v", err), http.StatusInternalServerError)
		}
		return
	}

	// Set response headers
	w.Header().Set("Content-Type", objectData.ContentType)
	w.Header().Set("ETag", objectData.ETag)
	w.Header().Set("Last-Modified", objectData.LastModified.Format(http.TimeFormat))
	w.Header().Set("Content-Length", strconv.FormatInt(int64(len(objectData.Data)), 10))

	// Set metadata headers
	for key, value := range objectData.Metadata {
		w.Header().Set("X-Meta-"+key, value)
	}

	// Write object data
	w.Write(objectData.Data)
}

// Delete object
func (s *Server) deleteObjectHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	parts := strings.SplitN(strings.TrimPrefix(r.URL.Path, "/objects/"), "/", 2)
	if len(parts) != 2 {
		http.Error(w, "Invalid path format. Use /objects/{bucket}/{key}", http.StatusBadRequest)
		return
	}

	bucket, key := parts[0], parts[1]

	if err := s.storage.DeleteObject(bucket, key); err != nil {
		if strings.Contains(err.Error(), "not found") {
			http.Error(w, "Object not found", http.StatusNotFound)
		} else {
			http.Error(w, fmt.Sprintf("Failed to delete object: %v", err), http.StatusInternalServerError)
		}
		return
	}

	fmt.Fprintf(w, "Object %s deleted successfully", key)
}

// Statistics endpoint
func (s *Server) statsHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	stats, err := s.storage.GetStats()
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to get stats: %v", err), http.StatusInternalServerError)
		return
	}

	metrics := s.storage.GetMetrics()
	total, free, used, err := s.storage.GetDiskUsage()
	if err != nil {
		s.logger.Warnf("Failed to get disk usage: %v", err)
	}

	response := map[string]interface{}{
		"storage": stats,
		"metrics": metrics,
		"disk": map[string]uint64{
			"total": total,
			"free":  free,
			"used":  used,
		},
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// Setup routes
func (s *Server) setupRoutes() *http.ServeMux {
	mux := http.NewServeMux()

	// Health check
	mux.HandleFunc("/health", s.healthHandler)

	// Statistics
	mux.HandleFunc("/stats", s.statsHandler)

	// Bucket operations
	mux.HandleFunc("/buckets", s.listBucketsHandler)
	mux.HandleFunc("/buckets/", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			s.createBucketHandler(w, r)
		case http.MethodDelete:
			s.deleteBucketHandler(w, r)
		default:
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		}
	})

	// Object operations
	mux.HandleFunc("/objects/", func(w http.ResponseWriter, r *http.Request) {
		parts := strings.SplitN(strings.TrimPrefix(r.URL.Path, "/objects/"), "/", 2)
		if len(parts) == 1 {
			// List objects in bucket
			s.listObjectsHandler(w, r)
		} else {
			// Object operations
			switch r.Method {
			case http.MethodGet:
				s.getObjectHandler(w, r)
			case http.MethodPut:
				s.putObjectHandler(w, r)
			case http.MethodDelete:
				s.deleteObjectHandler(w, r)
			default:
				http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			}
		}
	})

	return mux
}

func main() {
	// Parse command line arguments
	storagePath := "./webserver-data"
	port := "8080"

	if len(os.Args) > 1 {
		storagePath = os.Args[1]
	}
	if len(os.Args) > 2 {
		port = os.Args[2]
	}

	// Create server
	server, err := NewServer(storagePath)
	if err != nil {
		log.Fatal("Failed to create server:", err)
	}
	defer server.Close()

	// Setup routes
	mux := server.setupRoutes()

	// Add logging middleware
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		mux.ServeHTTP(w, r)
		server.logger.Infof("%s %s %v", r.Method, r.URL.Path, time.Since(start))
	})

	// Start server
	server.logger.Infof("Starting server on port %s with storage path: %s", port, storagePath)
	server.logger.Infof("API endpoints:")
	server.logger.Infof("  GET    /health           - Health check")
	server.logger.Infof("  GET    /stats            - Storage statistics")
	server.logger.Infof("  GET    /buckets          - List buckets")
	server.logger.Infof("  POST   /buckets/{name}   - Create bucket")
	server.logger.Infof("  DELETE /buckets/{name}   - Delete bucket")
	server.logger.Infof("  GET    /objects/{bucket} - List objects")
	server.logger.Infof("  GET    /objects/{bucket}/{key} - Get object")
	server.logger.Infof("  PUT    /objects/{bucket}/{key} - Put object")
	server.logger.Infof("  DELETE /objects/{bucket}/{key} - Delete object")

	if err := http.ListenAndServe(":"+port, handler); err != nil {
		log.Fatal("Server failed:", err)
	}
}
