package mindb

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"strings"
	"time"
)

// calculateETag 计算数据的MD5哈希作为ETag
func CalculateETag(data []byte) string {
	hash := md5.Sum(data)
	return fmt.Sprintf("\"%s\"", hex.EncodeToString(hash[:]))
}

// calculateMultipartETag 计算分段上传的最终ETag
// S3兼容的格式: "{md5-of-all-etags}-{number-of-parts}"
func CalculateMultipartETag(partETags []string) string {
	if len(partETags) == 0 {
		return ""
	}

	// 移除每个ETag的引号
	cleanETags := make([]string, len(partETags))
	for i, etag := range partETags {
		cleanETags[i] = strings.Trim(etag, "\"")
	}

	if len(partETags) == 1 {
		// 单个分段，直接返回该分段的ETag
		return cleanETags[0]
	}

	// 多个分段：计算所有分段ETag的MD5
	hasher := md5.New()
	for _, etag := range cleanETags {
		// 将十六进制字符串转换为字节
		if bytes, err := hex.DecodeString(etag); err == nil {
			hasher.Write(bytes)
		} else {
			// 如果解码失败，直接写入字符串（不应该发生）
			hasher.Write([]byte(etag))
		}
	}

	hash := hex.EncodeToString(hasher.Sum(nil))
	return fmt.Sprintf("%s-%d", hash, len(partETags))
}

// generateUploadID 生成唯一的上传ID
func GenerateUploadID(bucket, key string) string {
	now := time.Now().UnixNano()
	hash := md5.Sum([]byte(fmt.Sprintf("%s-%s-%d", bucket, key, now)))
	return hex.EncodeToString(hash[:])
}
