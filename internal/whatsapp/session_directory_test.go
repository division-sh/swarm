//go:build linux || darwin

package whatsapp

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
)

func TestWhatsAppSessionDirectoryJoinFailureRetainsPossessionAndPairing(t *testing.T) {
	base, connection := t.TempDir(), uuid.NewString()
	if err := os.Chmod(base, 0o700); err != nil {
		t.Fatal(err)
	}
	directory, err := openSessionDirectory(base, connection)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = directory.release() }()
	path := filepath.Join(directory.path, "session.db")
	if err := os.WriteFile(path, []byte("controlled retained pairing state"), 0o600); err != nil {
		t.Fatal(err)
	}
	fence := newSDKStoreFence()
	_, release, err := fence.acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := directory.retire(ctx, fence); !errors.Is(err, context.Canceled) {
		t.Fatal("unjoined state released possession", err)
	}
	if other, err := openSessionDirectory(base, connection); err == nil {
		_ = other.release()
		t.Fatal("successor acquired state while predecessor was unjoined")
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatal("failed join deleted pairing", err)
	}
	release()
	if err := directory.retire(context.Background(), fence); err != nil {
		t.Fatal(err)
	}
	reopened, err := openSessionDirectory(base, connection)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reopened.release() }()
	got, err := os.ReadFile(path)
	if err != nil || string(got) != "controlled retained pairing state" {
		t.Fatal("ordinary retirement/reopen deleted or replaced pairing", err)
	}
}

func TestWhatsAppSessionDirectoryRefusesUnsafeAndCopiedState(t *testing.T) {
	for _, cell := range []string{"directory_symlink", "directory_mode", "lock_symlink", "header_mode", "missing_header", "foreign_connection", "foreign_version", "unknown_header", "trailing_header", "duplicate_header"} {
		t.Run(cell, func(t *testing.T) {
			base, connection := t.TempDir(), uuid.NewString()
			if err := os.Chmod(base, 0o700); err != nil {
				t.Fatal(err)
			}
			directory, err := openSessionDirectory(base, connection)
			if err != nil {
				t.Fatal(err)
			}
			path := directory.path
			if err := directory.release(); err != nil {
				t.Fatal(err)
			}
			header := filepath.Join(path, "session.json")
			switch cell {
			case "directory_symlink":
				moved := path + "-moved"
				if err := os.Rename(path, moved); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(moved, path); err != nil {
					t.Fatal(err)
				}
			case "directory_mode":
				if err := os.Chmod(path, 0o755); err != nil {
					t.Fatal(err)
				}
			case "lock_symlink":
				lock := filepath.Join(path, "possession.lock")
				if err := os.Remove(lock); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(header, lock); err != nil {
					t.Fatal(err)
				}
			case "header_mode":
				if err := os.Chmod(header, 0o644); err != nil {
					t.Fatal(err)
				}
			case "missing_header":
				if err := os.Remove(header); err != nil {
					t.Fatal(err)
				}
			default:
				body := `{"version":1,"connection_id":"` + connection + `"}`
				switch cell {
				case "foreign_connection":
					body = `{"version":1,"connection_id":"` + uuid.NewString() + `"}`
				case "foreign_version":
					body = `{"version":2,"connection_id":"` + connection + `"}`
				case "unknown_header":
					body = `{"version":1,"connection_id":"` + connection + `","alias":true}`
				case "trailing_header":
					body += `{}`
				case "duplicate_header":
					body = `{"version":2,"version":1,"connection_id":"` + connection + `"}`
				}
				if err := os.WriteFile(header, []byte(body), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			if reopened, err := openSessionDirectory(base, connection); err == nil {
				_ = reopened.release()
				t.Fatal("unsafe/copied state was accepted or repaired")
			}
		})
	}
}
