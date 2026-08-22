package packartifact

import (
	"fmt"
	"os"
	"path/filepath"
)

const projectPackLockRelativePath = ".swarm/project-packs.lock"

type projectPackTransaction struct {
	lock      *os.File
	stateRoot string
}

func acquireProjectPackTransaction(projectRoot string) (*projectPackTransaction, error) {
	stateRoot := filepath.Join(projectRoot, ".swarm")
	info, err := os.Lstat(stateRoot)
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, fmt.Errorf("inspect project pack transaction directory: %w", err)
		}
		if err := os.Mkdir(stateRoot, 0o700); err != nil && !os.IsExist(err) {
			return nil, fmt.Errorf("create project pack transaction directory: %w", err)
		}
		info, err = os.Lstat(stateRoot)
		if err != nil {
			return nil, fmt.Errorf("inspect project pack transaction directory: %w", err)
		}
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return nil, fmt.Errorf("project pack transaction path %q must be a real directory", ".swarm")
	}

	lockPath := filepath.Join(projectRoot, filepath.FromSlash(projectPackLockRelativePath))
	if lockInfo, statErr := os.Lstat(lockPath); statErr == nil {
		if lockInfo.Mode()&os.ModeSymlink != 0 || !lockInfo.Mode().IsRegular() {
			return nil, fmt.Errorf("project pack transaction lock %q must be a regular file", projectPackLockRelativePath)
		}
	} else if !os.IsNotExist(statErr) {
		return nil, fmt.Errorf("inspect project pack transaction lock: %w", statErr)
	}
	lock, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open project pack transaction lock: %w", err)
	}
	if err := lockProjectPackFile(lock); err != nil {
		_ = lock.Close()
		return nil, fmt.Errorf("lock project pack transaction: %w", err)
	}
	pathInfo, pathErr := os.Lstat(lockPath)
	fileInfo, fileErr := lock.Stat()
	if pathErr != nil || fileErr != nil || pathInfo.Mode()&os.ModeSymlink != 0 || !os.SameFile(pathInfo, fileInfo) {
		_ = unlockProjectPackFile(lock)
		_ = lock.Close()
		return nil, fmt.Errorf("project pack transaction lock changed during acquisition")
	}
	return &projectPackTransaction{lock: lock, stateRoot: stateRoot}, nil
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
