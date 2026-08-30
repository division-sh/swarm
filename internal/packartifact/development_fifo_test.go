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

func TestProjectPackReaderRejectsFIFOReplacementWithoutBlocking(t *testing.T) {
	base, err := LoadPlatformPackInventoryFS(
		testInventoryFS(t, "provider.demo", TypeTrigger, "provider-triggers/demo", ProvenancePlatform, []byte("provider: demo\n")),
		InventoryManifestFileName,
		testPlatformVersion,
		SelectionEmbedded,
	)
	if err != nil {
		t.Fatal(err)
	}
	project := t.TempDir()
	if err := os.WriteFile(filepath.Join(project, "schema.yaml"), []byte("name: demo\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := ImportEmbeddedPack(project, "provider.demo", base); err != nil {
		t.Fatal(err)
	}
	bodyPath := filepath.Join(project, ProjectPackDirectory, "provider.demo", TriggerManifestFileName)
	if err := os.Remove(bodyPath); err != nil {
		t.Fatal(err)
	}
	if err := unix.Mkfifo(bodyPath, 0o600); err != nil {
		t.Fatal(err)
	}

	result := make(chan error, 1)
	go func() {
		_, err := LoadProjectPackSet(project)
		result <- err
	}()
	select {
	case err := <-result:
		if err == nil || !strings.Contains(err.Error(), "must be a regular non-symlink file") {
			t.Fatalf("FIFO project artifact error = %v, want rooted same-handle regular-file rejection", err)
		}
	case <-time.After(2 * time.Second):
		writer, _ := os.OpenFile(bodyPath, os.O_WRONLY|unix.O_NONBLOCK, 0)
		if writer != nil {
			_ = writer.Close()
		}
		t.Fatal("same-handle project artifact reader blocked on a FIFO replacement")
	}
}
