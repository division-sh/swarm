//go:build linux || darwin

package sessionprovider

import (
	"context"
	"errors"
	"os"
	"path/filepath"

	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/google/uuid"
	"golang.org/x/sys/unix"
)

func (s *InvocationState) directory(ctx context.Context) (string, error) {
	if s == nil || ctx == nil {
		return "", errSessionProtection
	}
	process, ok := worklifetime.ProcessFromContext(ctx)
	if !ok || process != s.process {
		return "", errSessionProtection
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if ctx.Err() != nil || s.retired {
		return "", errors.Join(errSessionProtection, context.Cause(ctx))
	}
	if s.base != nil {
		return s.path, s.validateBase()
	}
	if s.parent != nil {
		return "", errSessionProtection
	}
	parentPath := filepath.Clean(os.TempDir())
	fd, err := unix.Open(parentPath, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return "", err
	}
	s.parent = os.NewFile(uintptr(fd), parentPath)
	s.retireFiles = retireNativeInvocationFiles
	s.name = "swarm-native-" + uuid.NewString()
	s.path = filepath.Join(parentPath, s.name)
	if err := unix.Mkdirat(fd, s.name, 0o700); err != nil {
		s.name, s.path = "", ""
		return "", errors.Join(err, s.closeDescriptors())
	}
	s.base, err = openPrivateAt(fd, s.name, unix.O_RDONLY, true)
	if err != nil {
		// Keep the exact created name and parent for retirement, even when a
		// partial construction cannot acquire its own directory descriptor.
		return "", err
	}
	if ctx.Err() != nil {
		return "", context.Cause(ctx)
	}
	return s.path, nil
}

func (s *InvocationState) validateBase() error {
	if s.parent == nil || s.base == nil || s.name == "" || privateDescriptor(s.base, true) != nil {
		return errSessionProtection
	}
	current, err := openPrivateAt(int(s.parent.Fd()), s.name, unix.O_RDONLY, true)
	if err != nil {
		return err
	}
	owned, ownedErr := s.base.Stat()
	observed, observedErr := current.Stat()
	closeErr := current.Close()
	if ownedErr != nil || observedErr != nil || !os.SameFile(owned, observed) {
		return errors.Join(errSessionProtection, ownedErr, observedErr, closeErr)
	}
	parent, err := os.Open(filepath.Dir(s.path))
	if err != nil {
		return errors.Join(err, closeErr)
	}
	ownedParent, ownedErr := s.parent.Stat()
	observedParent, observedErr := parent.Stat()
	parentCloseErr := parent.Close()
	if ownedErr != nil || observedErr != nil || !os.SameFile(ownedParent, observedParent) {
		return errors.Join(errSessionProtection, ownedErr, observedErr, closeErr, parentCloseErr)
	}
	return errors.Join(closeErr, parentCloseErr)
}

func retireNativeInvocationFiles(s *InvocationState) error {
	if s.parent == nil {
		return nil
	}
	if err := s.validateBase(); err != nil {
		return err
	}
	if err := removeInvocationConnections(s.base); err != nil {
		return err
	}
	if err := s.validateBase(); err != nil {
		return err
	}
	if err := unix.Unlinkat(int(s.parent.Fd()), s.name, unix.AT_REMOVEDIR); err != nil {
		return err
	}
	s.name, s.path = "", ""
	return s.closeDescriptors()
}

func (s *InvocationState) closeDescriptors() error {
	var result error
	for _, file := range []**os.File{&s.base, &s.parent} {
		if *file != nil {
			result = errors.Join(result, (*file).Close())
			*file = nil
		}
	}
	return result
}

func removeInvocationConnections(directory *os.File) error {
	if _, err := directory.Seek(0, 0); err != nil {
		return err
	}
	entries, err := directory.ReadDir(-1)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if !entry.IsDir() || uuid.Validate(entry.Name()) != nil {
			return errSessionProtection
		}
		child, err := openPrivateAt(int(directory.Fd()), entry.Name(), unix.O_RDONLY, true)
		if err != nil {
			return err
		}
		err = removeInvocationConnection(child)
		if err == nil {
			err = unlinkInvocationEntry(directory, child, entry.Name(), unix.AT_REMOVEDIR)
		}
		if err = errors.Join(err, child.Close()); err != nil {
			return err
		}
	}
	return nil
}

func removeInvocationConnection(directory *os.File) (result error) {
	lock, err := openPrivateAt(int(directory.Fd()), "possession.lock", unix.O_RDWR, false)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if lock != nil {
		defer func() { result = errors.Join(result, lock.Close()) }()
		if err := unix.Flock(int(lock.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
			return errors.Join(errSessionPossession, err)
		}
	}
	entries, err := directory.ReadDir(-1)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		switch entry.Name() {
		case "possession.lock", "session.json", "provider.db", "provider.db-journal", "provider.db-wal", "provider.db-shm":
		default:
			return errSessionProtection
		}
		file, err := openPrivateAt(int(directory.Fd()), entry.Name(), unix.O_RDONLY|unix.O_NONBLOCK, false)
		if err != nil {
			return err
		}
		err = unlinkInvocationEntry(directory, file, entry.Name(), 0)
		if err = errors.Join(err, file.Close()); err != nil {
			return err
		}
	}
	return nil
}

func unlinkInvocationEntry(parent, file *os.File, name string, flags int) error {
	var owned, observed unix.Stat_t
	if err := unix.Fstat(int(file.Fd()), &owned); err != nil {
		return err
	}
	if err := unix.Fstatat(int(parent.Fd()), name, &observed, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		return err
	}
	if owned.Dev != observed.Dev || owned.Ino != observed.Ino {
		return errSessionProtection
	}
	return unix.Unlinkat(int(parent.Fd()), name, flags)
}
