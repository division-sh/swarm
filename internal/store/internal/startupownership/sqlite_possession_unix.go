//go:build darwin || linux

package startupownership

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"

	runtimestartupownership "github.com/division-sh/swarm/internal/runtime/startupownership"
	"golang.org/x/sys/unix"
)

type sqliteFilePossession struct {
	mu           sync.Mutex
	database     *os.File
	databaseInfo os.FileInfo
	lock         *os.File
	lockInfo     os.FileInfo
	lockPath     string
	path         string
	released     bool
}

func acquireSQLiteFilePossession(selectedPath string) (sqlitePossession, error) {
	selectedPath = strings.TrimSpace(selectedPath)
	abs, err := filepath.Abs(filepath.Clean(selectedPath))
	if err != nil {
		return nil, fmt.Errorf("resolve SQLite selected-store path: %w", err)
	}
	resolved, err := filepath.EvalSymlinks(abs)
	if err != nil {
		return nil, fmt.Errorf("resolve SQLite selected-store identity: %w", err)
	}
	resolved = filepath.Clean(resolved)
	if filepath.Clean(abs) != resolved && !systemCanonicalPathAlias(abs, resolved) {
		return nil, &runtimestartupownership.AcquisitionError{
			Failure: runtimestartupownership.AcquisitionPriorOwnerAmbiguous,
			Detail:  "SQLite selected-store aliases are not ownership authority; select its canonical path",
		}
	}
	if err := requireSupportedLocalFilesystem(resolved); err != nil {
		return nil, err
	}
	database, err := os.Open(resolved)
	if err != nil {
		return nil, fmt.Errorf("open SQLite selected-store identity: %w", err)
	}
	databaseInfo, err := database.Stat()
	if err != nil {
		_ = database.Close()
		return nil, fmt.Errorf("stat SQLite selected-store identity: %w", err)
	}
	if !isSingleLinkRegularFile(databaseInfo) {
		_ = database.Close()
		return nil, &runtimestartupownership.AcquisitionError{
			Failure: runtimestartupownership.AcquisitionPriorOwnerAmbiguous,
			Detail:  "SQLite selected-store hard-link aliases cannot prove one canonical ownership coordinate",
		}
	}
	lockPath := resolved + ".swarm-owner.lock"
	lock, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		_ = database.Close()
		return nil, fmt.Errorf("open SQLite selected-store possession file: %w", err)
	}
	lockInfo, err := lock.Stat()
	if err != nil {
		_ = lock.Close()
		_ = database.Close()
		return nil, fmt.Errorf("stat SQLite selected-store possession file: %w", err)
	}
	if !isSingleLinkRegularFile(lockInfo) {
		_ = lock.Close()
		_ = database.Close()
		return nil, &runtimestartupownership.AcquisitionError{
			Failure: runtimestartupownership.AcquisitionPriorOwnerAmbiguous,
			Detail:  "SQLite selected-store possession identity is aliased",
		}
	}
	if err := unix.Flock(int(lock.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		_ = lock.Close()
		_ = database.Close()
		if errors.Is(err, unix.EWOULDBLOCK) || errors.Is(err, unix.EAGAIN) {
			return nil, &runtimestartupownership.AcquisitionError{
				Failure: runtimestartupownership.AcquisitionTakeoverRequired,
				Detail:  "selected store is held by another process",
			}
		}
		return nil, fmt.Errorf("acquire SQLite selected-store possession: %w", err)
	}
	possession := &sqliteFilePossession{
		database: database, databaseInfo: databaseInfo,
		lock: lock, lockInfo: lockInfo, lockPath: lockPath, path: resolved,
	}
	if err := possession.ProveCurrent(context.Background()); err != nil {
		return nil, errors.Join(err, possession.Release())
	}
	return possession, nil
}

func isSingleLinkRegularFile(info os.FileInfo) bool {
	if info == nil || !info.Mode().IsRegular() {
		return false
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	return ok && stat.Nlink == 1
}

func (p *sqliteFilePossession) ProveCurrent(ctx context.Context) error {
	if p == nil {
		return errors.New("SQLite selected-store possession is missing")
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.released || p.database == nil || p.lock == nil {
		return errors.New("SQLite selected-store possession is released")
	}
	current, err := os.Stat(p.path)
	if err != nil {
		return fmt.Errorf("prove SQLite selected-store identity: %w", err)
	}
	if !os.SameFile(p.databaseInfo, current) {
		return &runtimestartupownership.PossessionError{
			Cause: runtimestartupownership.TerminalOwnershipUnprovable,
			Err:   errors.New("SQLite selected-store file identity changed"),
		}
	}
	currentLock, err := os.Lstat(p.lockPath)
	if err != nil {
		return &runtimestartupownership.PossessionError{
			Cause: runtimestartupownership.TerminalOwnershipUnprovable,
			Err:   fmt.Errorf("prove SQLite selected-store possession identity: %w", err),
		}
	}
	if currentLock.Mode()&os.ModeSymlink != 0 || !isSingleLinkRegularFile(currentLock) || !os.SameFile(p.lockInfo, currentLock) {
		return &runtimestartupownership.PossessionError{
			Cause: runtimestartupownership.TerminalOwnershipUnprovable,
			Err:   errors.New("SQLite selected-store possession identity changed"),
		}
	}
	return nil
}

func (p *sqliteFilePossession) Release() error {
	if p == nil {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.released {
		return nil
	}
	p.released = true
	var unlockErr, lockCloseErr, databaseCloseErr error
	if p.lock != nil {
		unlockErr = unix.Flock(int(p.lock.Fd()), unix.LOCK_UN)
		lockCloseErr = p.lock.Close()
	}
	if p.database != nil {
		databaseCloseErr = p.database.Close()
	}
	p.lock = nil
	p.lockInfo = nil
	p.database = nil
	return errors.Join(unlockErr, lockCloseErr, databaseCloseErr)
}
