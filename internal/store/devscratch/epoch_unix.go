//go:build darwin || linux

package devscratch

import (
	"errors"
	"fmt"
	"os"
	"syscall"

	"golang.org/x/sys/unix"
)

type platformLock interface {
	rewrite([]byte) error
	release() error
}

type fileLock struct {
	file *os.File
}

func acquirePlatformLock(path string) (platformLock, error) {
	fd, err := unix.Open(path, unix.O_RDWR|unix.O_CREAT|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open dev scratch epoch authority: %w", err)
	}
	file := os.NewFile(uintptr(fd), path)
	if file == nil {
		_ = unix.Close(fd)
		return nil, errors.New("open dev scratch epoch authority file")
	}
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("stat dev scratch epoch authority: %w", err)
	}
	if !info.Mode().IsRegular() || !isSingleLink(info) {
		_ = file.Close()
		return nil, errors.New("dev scratch epoch authority must be one unaliased regular file")
	}
	if err := requireSupportedFilesystem(path); err != nil {
		_ = file.Close()
		return nil, err
	}
	if err := unix.Flock(fd, unix.LOCK_EX|unix.LOCK_NB); err != nil {
		_ = file.Close()
		if errors.Is(err, unix.EWOULDBLOCK) || errors.Is(err, unix.EAGAIN) {
			return nil, errors.New("another swarm serve --dev runtime owns this canonical project scratch epoch")
		}
		return nil, fmt.Errorf("acquire dev scratch epoch authority: %w", err)
	}
	current, err := os.Stat(path)
	if err != nil || !os.SameFile(info, current) {
		lock := &fileLock{file: file}
		return nil, errors.Join(errors.New("dev scratch epoch authority changed during acquisition"), lock.release())
	}
	return &fileLock{file: file}, nil
}

func (l *fileLock) rewrite(content []byte) error {
	if l == nil || l.file == nil {
		return errors.New("dev scratch epoch authority is not retained")
	}
	if err := l.file.Truncate(0); err != nil {
		return err
	}
	if _, err := l.file.Seek(0, 0); err != nil {
		return err
	}
	if _, err := l.file.Write(content); err != nil {
		return err
	}
	return l.file.Sync()
}

func (l *fileLock) release() error {
	if l == nil || l.file == nil {
		return nil
	}
	file := l.file
	l.file = nil
	return errors.Join(unix.Flock(int(file.Fd()), unix.LOCK_UN), file.Close())
}

func isSingleLink(info os.FileInfo) bool {
	if info == nil || !info.Mode().IsRegular() {
		return false
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	return ok && stat.Nlink == 1
}
