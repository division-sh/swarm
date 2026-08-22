package packartifact

import (
	"fmt"
	"os"
	"path/filepath"
)

type projectPackTransaction struct {
	lock      *os.File
	root      *admittedArtifactRoot
	stateRoot string
}

func acquireProjectPackTransaction(projectRoot string, exclusive bool) (*projectPackTransaction, error) {
	root, err := openAdmittedArtifactRoot(projectRoot)
	if err != nil {
		return nil, fmt.Errorf("open project pack transaction root: %w", err)
	}
	lock, err := root.openRegularFile("package.yaml")
	if err != nil {
		_ = root.close()
		return nil, fmt.Errorf("open project pack transaction anchor: %w", err)
	}
	if err := lockProjectPackFile(lock, exclusive); err != nil {
		_ = lock.Close()
		_ = root.close()
		return nil, fmt.Errorf("lock project pack transaction: %w", err)
	}
	return &projectPackTransaction{lock: lock, root: root, stateRoot: filepath.Clean(projectRoot)}, nil
}

func (t *projectPackTransaction) close() error {
	if t == nil || t.lock == nil {
		return nil
	}
	unlockErr := unlockProjectPackFile(t.lock)
	closeErr := t.lock.Close()
	rootCloseErr := t.root.close()
	t.lock = nil
	t.root = nil
	if unlockErr != nil {
		return fmt.Errorf("unlock project pack transaction: %w", unlockErr)
	}
	if closeErr != nil {
		return fmt.Errorf("close project pack transaction lock: %w", closeErr)
	}
	if rootCloseErr != nil {
		return fmt.Errorf("close project pack transaction root: %w", rootCloseErr)
	}
	return nil
}
