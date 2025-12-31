package mindb

import (
	"time"
)

const (
	Byte = 1

	KB = 1 << (10 * iota) // 1 << (10 * 1) = 1024
	MB                    // 1 << (10 * 2) = 1,048,576
	GB                    // 1 << (10 * 3) = 1,073,741,824
	TB                    // 1 << (10 * 4) = 1,099,511,627,776
	PB                    // 1 << (10 * 5) = 1,125,899,906,842,624
)

const (
	AK     = "ak"
	SK     = "sk"
	Token  = "token"
	Region = "region"
)

//
//go:generate easyjson -all types.go
type MultipartPart struct {
	PartNumber int
	ETag       string
}

//
//go:generate easyjson -all types.go
type MultipartUploadInfo struct {
	Bucket      string
	Key         string
	UploadID    string
	ContentType string
	Metadata    map[string]string
	CreatedAt   time.Time
}

//go:generate easyjson -all types.go
type PartInfo struct {
	PartNumber   int
	ETag         string
	Size         int
	LastModified time.Time
}

//
//go:generate easyjson -all types.go
type BucketInfo struct {
	Name         string
	CreationDate time.Time
}

//
//go:generate easyjson -all types.go
type ObjectInfo struct {
	Key          string
	Size         int64
	LastModified time.Time
	ETag         string
}

//
//go:generate easyjson -all types.go
type ObjectData struct {
	Key          string
	Data         []byte
	ContentType  string
	LastModified time.Time
	ETag         string
	Metadata     map[string]string
	Size         int64
}

//go:generate easyjson -all types.go
type Stats struct {
	BucketCount int64
	ObjectCount int64
	TotalSize   int64
}
