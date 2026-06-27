//go:build aix || darwin || dragonfly || freebsd || hurd || illumos || linux || netbsd || openbsd || solaris

package credentials

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/sys/unix"
)

func TestFileStore_SetDeleteFailClosedWhenCredentialFileLocked(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "credentials.json")
	store, err := NewFileStore(path)
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	lock, err := os.OpenFile(path+".lock", os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatalf("open lock: %v", err)
	}
	defer lock.Close()
	if err := unix.Flock(int(lock.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		t.Fatalf("lock credential file: %v", err)
	}
	defer func() {
		_ = unix.Flock(int(lock.Fd()), unix.LOCK_UN)
	}()

	if err := store.Set(ctx, "sendgrid_api_key", "secret-1"); !errors.Is(err, ErrStoreLocked) {
		t.Fatalf("Set err = %v, want ErrStoreLocked", err)
	}
	if err := store.Delete(ctx, "sendgrid_api_key"); !errors.Is(err, ErrStoreLocked) {
		t.Fatalf("Delete err = %v, want ErrStoreLocked", err)
	}
}
