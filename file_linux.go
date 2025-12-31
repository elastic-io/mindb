//go:build linux

package mindb

import (
	"os"
	"syscall"
)

func openWithDirectIO(filename string, perm os.FileMode) (*os.File, error) {
	return os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|syscall.O_DIRECT, perm)
}
