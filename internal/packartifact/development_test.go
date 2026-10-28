package packartifact

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestDevelopmentOverrideRejectsNonRegularEnvelopeBeforeRead(t *testing.T) {
	embedded, err := LoadEmbeddedPlatformPackInventory(testPlatformVersion)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name    string
		replace func(t *testing.T, memberPath string)
	}{
		{name: "symlink", replace: func(t *testing.T, memberPath string) {
			target := filepath.Join(t.TempDir(), "target.yaml")
			if err := os.WriteFile(target, []byte("version: 1.0.0\nplatform_version: '*'\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(target, memberPath); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "directory", replace: func(t *testing.T, memberPath string) {
			if err := os.Mkdir(memberPath, 0o700); err != nil {
				t.Fatal(err)
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			var dirs []string
			for _, entry := range embedded.Entries() {
				directory := filepath.Join(root, entry.ID())
				for name, body := range entry.Provider().Members() {
					member := filepath.Join(directory, name)
					if err := os.MkdirAll(filepath.Dir(member), 0o700); err != nil {
						t.Fatal(err)
					}
					if err := os.WriteFile(member, body, 0o600); err != nil {
						t.Fatal(err)
					}
				}
				dirs = append(dirs, directory)
			}
			captured, err := LoadDevelopmentPlatformPackInventory(testPlatformVersion, dirs, embedded)
			if err != nil {
				t.Fatalf("capture complete development inventory before fault: %v", err)
			}
			for _, entry := range captured.Entries() {
				provider := entry.Provider()
				if provider.Key() != filepath.Base(entry.Directory()) || provider.DeclaringFlow() != "" || provider.source.Root() != nil {
					t.Fatal("development reader invented a provider name or flow")
				}
				if _, present := provider.source.RootAdmission(); present {
					t.Fatal("development reader minted runtime root authority")
				}
			}
			member := filepath.Join(dirs[0], "manifest.yaml")
			if err := os.Remove(member); err != nil {
				t.Fatal(err)
			}
			tc.replace(t, member)
			if _, err := LoadDevelopmentPlatformPackInventory(testPlatformVersion, dirs, embedded); err == nil || !strings.Contains(err.Error(), "manifest.yaml") {
				t.Fatalf("non-regular member error = %v, want rooted same-handle rejection", err)
			}
		})
	}
}
