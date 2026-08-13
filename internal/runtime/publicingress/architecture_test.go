package publicingress

import (
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestPublicIngressArchitectureRatchets(t *testing.T) {
	repo := filepath.Clean(filepath.Join("..", "..", ".."))
	registrationSource, err := os.ReadFile(filepath.Join(repo, "internal", "runtime", "publicingress", "registration.go"))
	if err != nil {
		t.Fatalf("read registration owner: %v", err)
	}
	if strings.Contains(strings.ToLower(string(registrationSource)), "telegram") {
		t.Fatal("provider registration owner contains a provider-specific branch")
	}

	for _, root := range []string{
		filepath.Join(repo, "internal", "store", "migrations"),
		filepath.Join(repo, "internal", "store", "internal"),
	} {
		err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sql") {
				return nil
			}
			raw, readErr := os.ReadFile(path)
			if readErr != nil {
				return readErr
			}
			lower := strings.ToLower(string(raw))
			for _, forbidden := range []string{
				"create table public_ingress",
				"create table provider_registration",
				"create table registration_intent",
				"create table registration_evidence",
			} {
				if strings.Contains(lower, forbidden) {
					t.Errorf("%s persists process-local observation via %q", path, forbidden)
				}
			}
			return nil
		})
		if err != nil && !os.IsNotExist(err) {
			t.Fatalf("scan persistence tree %s: %v", root, err)
		}
	}
}
