package packartifact

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
)

type projectPackTransaction struct {
	pathLock   *os.File
	root       *admittedArtifactRoot
	writerRoot *os.Root
	stateRoot  string
}

func acquireProjectPackTransaction(projectRoot string, exclusive bool) (*projectPackTransaction, error) {
	pathLock, err := openProjectPackPathLock(projectRoot)
	if err != nil {
		return nil, fmt.Errorf("open project pack path transaction anchor: %w", err)
	}
	if err := lockProjectPackFile(pathLock, exclusive); err != nil {
		_ = pathLock.Close()
		return nil, fmt.Errorf("lock project pack path transaction: %w", err)
	}
	root, err := openAdmittedArtifactRoot(projectRoot)
	if err != nil {
		_ = unlockProjectPackFile(pathLock)
		_ = pathLock.Close()
		return nil, fmt.Errorf("open project pack transaction root: %w", err)
	}
	writerRoot, err := os.OpenRoot(projectRoot)
	if err != nil {
		_ = root.close()
		_ = unlockProjectPackFile(pathLock)
		_ = pathLock.Close()
		return nil, fmt.Errorf("open project pack writer root: %w", err)
	}
	readerInfo, readerErr := root.info()
	writerInfo, writerErr := writerRoot.Stat(".")
	if readerErr != nil || writerErr != nil || !os.SameFile(readerInfo, writerInfo) {
		_ = writerRoot.Close()
		_ = root.close()
		_ = unlockProjectPackFile(pathLock)
		_ = pathLock.Close()
		if readerErr != nil {
			return nil, fmt.Errorf("inspect project pack reader root: %w", readerErr)
		}
		if writerErr != nil {
			return nil, fmt.Errorf("inspect project pack writer root: %w", writerErr)
		}
		return nil, fmt.Errorf("project pack reader and writer roots do not identify the same directory")
	}
	return &projectPackTransaction{
		pathLock: pathLock, root: root, writerRoot: writerRoot,
		stateRoot: filepath.Clean(projectRoot),
	}, nil
}

func (t *projectPackTransaction) close() error {
	if t == nil {
		return nil
	}
	var firstErr error
	record := func(message string, err error) {
		if err != nil && firstErr == nil {
			firstErr = fmt.Errorf("%s: %w", message, err)
		}
	}
	if t.writerRoot != nil {
		record("close project pack writer root", t.writerRoot.Close())
		t.writerRoot = nil
	}
	if t.root != nil {
		record("close project pack transaction root", t.root.close())
		t.root = nil
	}
	if t.pathLock != nil {
		record("unlock project pack path transaction", unlockProjectPackFile(t.pathLock))
		record("close project pack path transaction lock", t.pathLock.Close())
		t.pathLock = nil
	}
	return firstErr
}

func openProjectPackPathLock(projectRoot string) (*os.File, error) {
	absolute, err := filepath.Abs(filepath.Clean(projectRoot))
	if err != nil {
		return nil, err
	}
	if runtime.GOOS == "darwin" || runtime.GOOS == "windows" {
		absolute = strings.ToLower(absolute)
	}
	key := sha256.Sum256([]byte(absolute))
	cacheRoot, err := os.UserCacheDir()
	if err != nil {
		return nil, fmt.Errorf("resolve user cache directory: %w", err)
	}
	lockRoot := filepath.Join(cacheRoot, "swarm", "project-pack-transactions")
	if err := os.MkdirAll(lockRoot, 0o700); err != nil {
		return nil, fmt.Errorf("create project pack transaction directory: %w", err)
	}
	info, err := os.Lstat(lockRoot)
	if err != nil {
		return nil, fmt.Errorf("inspect project pack transaction directory: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return nil, fmt.Errorf("project pack transaction path must be a real directory")
	}
	lockDirectory, err := os.OpenRoot(lockRoot)
	if err != nil {
		return nil, fmt.Errorf("open project pack transaction directory: %w", err)
	}
	defer lockDirectory.Close()
	lockName := hex.EncodeToString(key[:]) + ".lock"
	file, err := lockDirectory.OpenFile(lockName, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	fileInfo, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	pathInfo, pathErr := lockDirectory.Lstat(lockName)
	if pathErr != nil || pathInfo.Mode()&os.ModeSymlink != 0 || !fileInfo.Mode().IsRegular() || !os.SameFile(fileInfo, pathInfo) {
		_ = file.Close()
		if pathErr != nil {
			return nil, pathErr
		}
		return nil, fmt.Errorf("project pack path transaction anchor must be a regular file")
	}
	return file, nil
}
