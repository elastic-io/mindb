//go:build darwin

package mindb

import (
	"os"
	"syscall"
)

func openWithDirectIO(filename string, perm os.FileMode) (*os.File, error) {
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY, perm)
	if err == nil {
		syscall.Syscall(syscall.SYS_FCNTL, file.Fd(), syscall.F_NOCACHE, 1)
	}
	return file, err
}
