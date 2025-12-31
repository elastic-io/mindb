package mindb

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"strings"
	"time"
)


func CalculateETag(data []byte) string {
	hash := md5.Sum(data)
	return fmt.Sprintf("\"%s\"", hex.EncodeToString(hash[:]))
}



func CalculateMultipartETag(partETags []string) string {
	if len(partETags) == 0 {
		return ""
	}

	
	cleanETags := make([]string, len(partETags))
	for i, etag := range partETags {
		cleanETags[i] = strings.Trim(etag, "\"")
	}

	if len(partETags) == 1 {
		
		return cleanETags[0]
	}

	
	hasher := md5.New()
	for _, etag := range cleanETags {
		
		if bytes, err := hex.DecodeString(etag); err == nil {
			hasher.Write(bytes)
		} else {
			
			hasher.Write([]byte(etag))
		}
	}

	hash := hex.EncodeToString(hasher.Sum(nil))
	return fmt.Sprintf("%s-%d", hash, len(partETags))
}


func GenerateUploadID(bucket, key string) string {
	now := time.Now().UnixNano()
	hash := md5.Sum([]byte(fmt.Sprintf("%s-%s-%d", bucket, key, now)))
	return hex.EncodeToString(hash[:])
}
