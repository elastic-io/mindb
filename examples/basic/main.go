package main

import (
	"fmt"
	"log"

	"github.com/elastic-io/mindb"
)

func main() {
	// Create storage instance
	storage, err := mindb.New("./data")
	if err != nil {
		log.Fatal("Failed to create storage:", err)
	}
	defer storage.Close()

	// Create a bucket
	bucketName := "my-bucket"
	fmt.Printf("Creating bucket: %s\n", bucketName)
	if err := storage.CreateBucket(bucketName); err != nil {
		log.Fatal("Failed to create bucket:", err)
	}

	// Put an object
	objectKey := "hello.txt"
	objectData := &mindb.ObjectData{
		Key:         objectKey,
		Data:        []byte("Hello, World!"),
		ContentType: "text/plain",
		Metadata: map[string]string{
			"author": "example",
			"type":   "greeting",
		},
	}

	fmt.Printf("Putting object: %s\n", objectKey)
	if err := storage.PutObject(bucketName, objectData); err != nil {
		log.Fatal("Failed to put object:", err)
	}

	// Get the object
	fmt.Printf("Getting object: %s\n", objectKey)
	retrievedObject, err := storage.GetObject(bucketName, objectKey)
	if err != nil {
		log.Fatal("Failed to get object:", err)
	}

	fmt.Printf("Retrieved data: %s\n", string(retrievedObject.Data))
	fmt.Printf("Content type: %s\n", retrievedObject.ContentType)
	fmt.Printf("ETag: %s\n", retrievedObject.ETag)
	fmt.Printf("Metadata: %+v\n", retrievedObject.Metadata)

	// List objects
	fmt.Println("\nListing objects:")
	objects, _, err := storage.ListObjects(bucketName, "", "", "", 100)
	if err != nil {
		log.Fatal("Failed to list objects:", err)
	}

	for _, obj := range objects {
		fmt.Printf("- %s (size: %d, modified: %s)\n",
			obj.Key, obj.Size, obj.LastModified.Format("2006-01-02 15:04:05"))
	}

	// Get storage stats
	fmt.Println("\nStorage statistics:")
	stats, err := storage.GetStats()
	if err != nil {
		log.Fatal("Failed to get stats:", err)
	}

	fmt.Printf("Buckets: %d\n", stats.BucketCount)
	fmt.Printf("Objects: %d\n", stats.ObjectCount)
	fmt.Printf("Total size: %d bytes\n", stats.TotalSize)

	// Health check
	fmt.Println("\nPerforming health check:")
	if err := storage.HealthCheck(); err != nil {
		fmt.Printf("Health check failed: %v\n", err)
	} else {
		fmt.Println("Health check passed")
	}

	// Clean up
	fmt.Printf("\nCleaning up...\n")
	if err := storage.DeleteObject(bucketName, objectKey); err != nil {
		log.Printf("Failed to delete object: %v", err)
	}

	if err := storage.DeleteBucket(bucketName); err != nil {
		log.Printf("Failed to delete bucket: %v", err)
	}

	fmt.Println("Example completed successfully!")
}
