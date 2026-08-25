package operatorchannel

import (
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestOperatorChannelV1IsFullyRetired(t *testing.T) {
	repo := filepath.Clean(filepath.Join("..", ".."))
	retired := "swarm.hitl-channel/" + "v1"
	err := filepath.WalkDir(repo, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			if entry.Name() == ".git" {
				return filepath.SkipDir
			}
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
	zones := []string{
		filepath.Join(repo, "internal", "operatorchannel"),
		filepath.Join(repo, "internal", "store", "internal", "backend", "operatorchannel"),
	}
	for _, zone := range zones {
		err := filepath.WalkDir(zone, func(path string, entry fs.DirEntry, err error) error {
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
					t.Errorf("provider-neutral identity owner %s contains provider-native interpreter %q", path, forbidden)
				}
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
}
