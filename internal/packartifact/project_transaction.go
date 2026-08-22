package packartifact

import (
	"fmt"
	"os"
	"path/filepath"
)

type projectPackTransaction struct {
	lock      *os.File
	stateRoot string
}

func acquireProjectPackTransaction(projectRoot string, exclusive bool) (*projectPackTransaction, error) {
	lockPath := filepath.Join(projectRoot, "package.yaml")
	lockInfo, err := os.Lstat(lockPath)
	if err != nil {
		return nil, fmt.Errorf("inspect project pack transaction anchor: %w", err)
	}
	if lockInfo.Mode()&os.ModeSymlink != 0 || !lockInfo.Mode().IsRegular() {
		return nil, fmt.Errorf("project pack transaction anchor package.yaml must be a regular file")
	}
	lock, err := os.Open(lockPath)
	if err != nil {
		return nil, fmt.Errorf("open project pack transaction anchor: %w", err)
	}
	if err := lockProjectPackFile(lock, exclusive); err != nil {
		_ = lock.Close()
		return nil, fmt.Errorf("lock project pack transaction: %w", err)
	}
	pathInfo, pathErr := os.Lstat(lockPath)
	fileInfo, fileErr := lock.Stat()
	if pathErr != nil || fileErr != nil || pathInfo.Mode()&os.ModeSymlink != 0 || !os.SameFile(pathInfo, fileInfo) {
		_ = unlockProjectPackFile(lock)
		_ = lock.Close()
		return nil, fmt.Errorf("project pack transaction anchor changed during acquisition")
	}
	return &projectPackTransaction{lock: lock, stateRoot: projectRoot}, nil
}

func (t *projectPackTransaction) close() error {
	if t == nil || t.lock == nil {
		return nil
	}
	unlockErr := unlockProjectPackFile(t.lock)
	closeErr := t.lock.Close()
	t.lock = nil
	if unlockErr != nil {
		return fmt.Errorf("unlock project pack transaction: %w", unlockErr)
	}
	if closeErr != nil {
		return fmt.Errorf("close project pack transaction lock: %w", closeErr)
	}
	return nil
}
