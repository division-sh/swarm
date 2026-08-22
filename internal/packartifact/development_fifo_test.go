//go:build linux || darwin

package packartifact

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func TestDevelopmentOverrideRejectsFIFOEnvelopeWithoutBlocking(t *testing.T) {
	embedded, err := LoadEmbeddedPlatformPackInventory(testPlatformVersion)
	if err != nil {
		t.Fatal(err)
	}
	dirs := materializeDevelopmentInventory(t, embedded, nil)
	envelopePath := filepath.Join(dirs[0], EnvelopeFileName)
	if err := os.Remove(envelopePath); err != nil {
		t.Fatal(err)
	}
	if err := unix.Mkfifo(envelopePath, 0o600); err != nil {
		t.Fatal(err)
	}

	result := make(chan error, 1)
	go func() {
		_, err := readRegularDevelopmentPackFile(dirs[0], EnvelopeFileName)
		result <- err
	}()
	select {
	case err := <-result:
		if err == nil || !strings.Contains(err.Error(), "must be a regular non-symlink file") {
			t.Fatalf("FIFO envelope error = %v, want same-handle regular-file rejection", err)
		}
	case <-time.After(2 * time.Second):
		writer, _ := os.OpenFile(envelopePath, os.O_WRONLY|unix.O_NONBLOCK, 0)
		if writer != nil {
			_ = writer.Close()
		}
		t.Fatal("same-handle development artifact reader blocked on a FIFO replacement")
	}
}
