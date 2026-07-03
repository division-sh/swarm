//go:build aix || darwin || dragonfly || freebsd || hurd || illumos || linux || netbsd || openbsd || solaris

package managedcredentials

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/sys/unix"
)

func TestFileStorePutDeleteFailClosedWhenManagedCredentialFileLocked(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "managed_credentials.json")
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
		t.Fatalf("lock managed credential file: %v", err)
	}
	defer func() {
		_ = unix.Flock(int(lock.Fd()), unix.LOCK_UN)
	}()

	record := Record{
		Key:         "github",
		GrantType:   GrantClientCredentials,
		TokenURL:    "https://example.invalid/token",
		ClientID:    "client-id",
		AccessToken: "token",
		Status:      StatusConnected,
	}
	if err := store.Put(ctx, record); !errors.Is(err, ErrStoreLocked) {
		t.Fatalf("Put err = %v, want ErrStoreLocked", err)
	}
	if err := store.Delete(ctx, "github"); !errors.Is(err, ErrStoreLocked) {
		t.Fatalf("Delete err = %v, want ErrStoreLocked", err)
	}
}
