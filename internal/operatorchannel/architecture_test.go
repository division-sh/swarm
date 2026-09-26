package operatorchannel

import (
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/checkoutsource"
)

func TestOperatorChannelV1IsFullyRetired(t *testing.T) {
	repo := filepath.Clean(filepath.Join("..", ".."))
	retired := "swarm.hitl-channel/" + "v1"
	err := checkoutsource.WalkDir(repo, repo, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		ext := strings.ToLower(filepath.Ext(path))
		if ext != ".go" && ext != ".yaml" && ext != ".yml" && ext != ".json" {
			return nil
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if strings.Contains(string(raw), retired) {
			t.Errorf("retired operator channel v1 remains in %s", path)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestOperatorChannelIdentityZoneHasNoProviderNativeInterpreter(t *testing.T) {
	repo := filepath.Clean(filepath.Join("..", ".."))
	violations, err := providerNativeIdentityViolations(repo)
	if err != nil {
		t.Fatal(err)
	}
	for _, violation := range violations {
		t.Error(violation)
	}
}

func TestOperatorChannelIdentityCensusExcludesNestedCheckout(t *testing.T) {
	repo := t.TempDir()
	var locals []string
	for _, zone := range []string{"internal/operatorchannel", "internal/store/internal/backend/operatorchannel"} {
		root := filepath.Join(repo, zone)
		local := filepath.Join(root, "current", "hostile.go")
		foreign := filepath.Join(root, "foreign")
		for _, path := range []string{local, filepath.Join(foreign, "hostile.go")} {
			if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path, []byte("package hostile\n// telegram\n"), 0o644); err != nil {
				t.Fatal(err)
			}
		}
		if err := os.WriteFile(filepath.Join(foreign, ".git"), []byte("gitdir: elsewhere\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		locals = append(locals, local)
	}
	violations, err := providerNativeIdentityViolations(repo)
	if err != nil || len(violations) != len(locals) {
		t.Fatalf("provider-native violations = %v, %v; want %d current-local files", violations, err, len(locals))
	}
	for _, path := range locals {
		if err := os.Remove(path); err != nil {
			t.Fatal(err)
		}
	}
	violations, err = providerNativeIdentityViolations(repo)
	if err != nil || len(violations) != 0 {
		t.Fatalf("foreign-only provider-native source was rejected: %v, %v", violations, err)
	}
}

func providerNativeIdentityViolations(repo string) ([]string, error) {
	var violations []string
	zones := []string{
		filepath.Join(repo, "internal", "operatorchannel"),
		filepath.Join(repo, "internal", "store", "internal", "backend", "operatorchannel"),
	}
	for _, zone := range zones {
		err := checkoutsource.WalkDir(repo, zone, func(path string, entry fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if entry.IsDir() || filepath.Ext(path) != ".go" || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			raw, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			text := strings.ToLower(string(raw))
			for _, forbidden := range []string{"telegram", "sender_chat", "callback_query", "message.from", "supergroup"} {
				if strings.Contains(text, forbidden) {
					violations = append(violations, path+" contains provider-native interpreter "+forbidden)
				}
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
	}
	return violations, nil
}
