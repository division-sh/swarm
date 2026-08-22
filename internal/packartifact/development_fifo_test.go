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
		_, err := LoadDevelopmentPlatformPackInventory(testPlatformVersion, dirs, embedded)
		result <- err
	}()
	select {
	case err := <-result:
		if err == nil || !strings.Contains(err.Error(), "contains unsupported entry \"pack.yaml\"") {
			t.Fatalf("FIFO envelope error = %v, want pre-read unsupported-entry rejection", err)
		}
	case <-time.After(2 * time.Second):
		writer, _ := os.OpenFile(envelopePath, os.O_WRONLY|unix.O_NONBLOCK, 0)
		if writer != nil {
			_ = writer.Close()
		}
		t.Fatal("development pack admission blocked while reading a FIFO envelope")
	}
}
