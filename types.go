package mindb

import (
	"time"
)

// 定义数据大小单位常量
const (
	// 基本单位 - 字节
	Byte = 1

	// 使用 iota 从 0 开始，每次增加 1
	// 左移 10 位相当于乘以 2^10 = 1024
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

// MultipartPart 表示分段上传的一个部分
//
//go:generate easyjson -all types.go
type MultipartPart struct {
	PartNumber int
	ETag       string
}

// multipartUploadInfo 存储分段上传的元数据
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

// BucketInfo 表示存储桶的元数据
//
//go:generate easyjson -all types.go
type BucketInfo struct {
	Name         string
	CreationDate time.Time
}

// ObjectInfo 表示 types 对象的元数据
//
//go:generate easyjson -all types.go
type ObjectInfo struct {
	Key          string
	Size         int64
	LastModified time.Time
	ETag         string
}

// ObjectData 表示 types 对象的完整数据
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
