//go:build linux || darwin

package whatsapp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/google/uuid"
	"golang.org/x/sys/unix"
)

var errSessionPossession = errors.New("WhatsApp session directory is not exclusively possessed")
var errSessionProtection = errors.New("WhatsApp session directory requires exact private ownership and v1 state")

type sessionDirectory struct {
	mu           sync.Mutex
	base         *os.File
	directory    *os.File
	lock         *os.File
	connectionID string
	path         string
	released     bool
}

type sessionStateHeader struct {
	Version      int    `json:"version"`
	ConnectionID string `json:"connection_id"`
}

func privateDescriptor(file *os.File, directory bool) error {
	var stat unix.Stat_t
	if err := unix.Fstat(int(file.Fd()), &stat); err != nil {
		return err
	}
	want := uint32(unix.S_IFREG)
	if directory {
		want = unix.S_IFDIR
	}
	if stat.Mode&unix.S_IFMT != want || stat.Mode&0o077 != 0 || stat.Uid != uint32(os.Geteuid()) || !directory && stat.Nlink != 1 {
		return errSessionProtection
	}
	return nil
}

func openPrivateAt(parent int, name string, flags int, directory bool) (*os.File, error) {
	mode := uint32(0o600)
	if directory {
		flags |= unix.O_DIRECTORY
		mode = 0o700
	}
	fd, err := unix.Openat(parent, name, flags|unix.O_CLOEXEC|unix.O_NOFOLLOW, mode)
	if err != nil {
		return nil, err
	}
	file := os.NewFile(uintptr(fd), name)
	if err := privateDescriptor(file, directory); err != nil {
		return nil, errors.Join(err, file.Close())
	}
	return file, nil
}

// The caller supplies the existing private runtime-state base, not a source,
// run, or process-occurrence directory. Opening never connects or pairs.
func openSessionDirectory(basePath, connectionID string) (*sessionDirectory, error) {
	if !filepath.IsAbs(basePath) || uuid.Validate(connectionID) != nil {
		return nil, errSessionProtection
	}
	base, err := openPrivateAt(unix.AT_FDCWD, basePath, unix.O_RDONLY, true)
	if err != nil {
		return nil, err
	}
	createErr := unix.Mkdirat(int(base.Fd()), connectionID, 0o700)
	if createErr != nil && !errors.Is(createErr, unix.EEXIST) {
		return nil, errors.Join(createErr, base.Close())
	}
	directory, err := openPrivateAt(int(base.Fd()), connectionID, unix.O_RDONLY, true)
	if err != nil {
		return nil, errors.Join(err, base.Close())
	}
	lock, err := openPrivateAt(int(directory.Fd()), "possession.lock", unix.O_CREAT|unix.O_RDWR, false)
	if err != nil {
		return nil, errors.Join(err, directory.Close(), base.Close())
	}
	if err := unix.Flock(int(lock.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		return nil, errors.Join(errSessionPossession, err, lock.Close(), directory.Close(), base.Close())
	}
	d := &sessionDirectory{base: base, directory: directory, lock: lock,
		connectionID: connectionID, path: filepath.Join(basePath, connectionID)}
	if err := d.admitHeader(createErr == nil); err != nil {
		return nil, errors.Join(err, d.release())
	}
	return d, nil
}

func (d *sessionDirectory) admitHeader(create bool) error {
	header := sessionStateHeader{Version: 1, ConnectionID: d.connectionID}
	if !create {
		file, err := openPrivateAt(int(d.directory.Fd()), "session.json", unix.O_RDONLY, false)
		if err != nil {
			return err
		}
		defer file.Close()
		raw, err := io.ReadAll(io.LimitReader(file, 4097))
		if err != nil || len(raw) > 4096 {
			return errors.Join(errSessionProtection, err)
		}
		if _, err := canonicaljson.Decode(raw); err != nil {
			return err
		}
		decoder := json.NewDecoder(bytes.NewReader(raw))
		decoder.DisallowUnknownFields()
		var stored sessionStateHeader
		if err := decoder.Decode(&stored); err != nil {
			return err
		}
		var trailing any
		if decoder.Decode(&trailing) != io.EOF || stored != header {
			return errSessionProtection
		}
		return nil
	}
	file, err := openPrivateAt(int(d.directory.Fd()), "session.json", unix.O_RDWR|unix.O_CREAT|unix.O_EXCL, false)
	if err != nil {
		return err
	}
	raw, err := json.Marshal(header)
	if err == nil {
		_, err = file.Write(raw)
	}
	if err == nil {
		err = file.Sync()
	}
	if err == nil {
		err = d.directory.Sync()
	}
	return errors.Join(err, file.Close())
}

type sessionRetirementJoin interface {
	join(context.Context) error
}

// Real installation supplies the complete client/store/callback join. The
// isolated component uses controlled leases; it cannot clear the SDK defect.
func (d *sessionDirectory) retire(ctx context.Context, owner sessionRetirementJoin) error {
	if owner == nil {
		return errSessionPossession
	}
	if err := owner.join(ctx); err != nil {
		return err
	}
	return d.release()
}

func (d *sessionDirectory) release() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.released {
		return nil
	}
	if err := errors.Join(d.lock.Close(), d.directory.Close(), d.base.Close()); err != nil {
		return fmt.Errorf("release WhatsApp session possession: %w", err)
	}
	d.released = true
	return nil
}
