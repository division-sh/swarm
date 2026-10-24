package runtime_test

import (
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/checkoutsource"
)

func TestPlatformPackBodiesHaveOneEmbedOwnerAndNoRetiredTeachingConfig(t *testing.T) {
	repoRoot := filepath.Clean(filepath.Join("..", ".."))
	bodyEmbeds, err := platformBodyEmbeds(repoRoot)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(bodyEmbeds, ",") != "packs/embed.go" {
		t.Fatalf("platform pack body embed owners = %v, want [packs/embed.go]", bodyEmbeds)
	}
	paths, err := platformPackTeachingFiles(repoRoot)
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range paths {
		body, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		assertNoRetiredPackConfig(t, path, string(body))
	}
	for _, path := range []string{"swarm.example.yaml", "internal/cliapp/unified_config_example.go"} {
		body, err := os.ReadFile(filepath.Join(repoRoot, path))
		if err != nil {
			t.Fatal(err)
		}
		assertNoRetiredPackConfig(t, path, string(body))
	}
}

func TestPlatformPackSourceCensusesExcludeNestedCheckout(t *testing.T) {
	repo := t.TempDir()
	localEmbed := filepath.Join(repo, "packs", "embed.go")
	foreignEmbed := filepath.Join(repo, "packs", "foreign", "embed.go")
	localTeaching := []string{
		filepath.Join(repo, ".github", "fixtures", "current.yaml"),
		filepath.Join(repo, "examples", "current.md"),
	}
	foreignTeaching := []string{
		filepath.Join(repo, ".github", "fixtures", "foreign", "hostile.yaml"),
		filepath.Join(repo, "examples", "foreign", "hostile.md"),
	}
	for _, path := range []string{localEmbed, foreignEmbed} {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte("package packs\n//go:embed provider-triggers\n"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	for _, path := range append(append([]string{}, localTeaching...), foreignTeaching...) {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte("external_dirs: retired\n"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	for _, dir := range []string{filepath.Dir(foreignEmbed), filepath.Dir(foreignTeaching[0]), filepath.Dir(foreignTeaching[1])} {
		if err := os.WriteFile(filepath.Join(dir, ".git"), []byte("gitdir: elsewhere\n"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	embeds, err := platformBodyEmbeds(repo)
	if err != nil || len(embeds) != 1 || embeds[0] != "packs/embed.go" {
		t.Fatalf("body embeds = %v, %v; want current-local owner", embeds, err)
	}
	paths, err := platformPackTeachingFiles(repo)
	if err != nil || len(paths) != len(localTeaching) {
		t.Fatalf("teaching files = %v, %v; want current-local files", paths, err)
	}
	for _, path := range append([]string{localEmbed}, localTeaching...) {
		if err := os.Remove(path); err != nil {
			t.Fatal(err)
		}
	}
	embeds, err = platformBodyEmbeds(repo)
	if err != nil || len(embeds) != 0 {
		t.Fatalf("foreign-only body embed received credit: %v, %v", embeds, err)
	}
	paths, err = platformPackTeachingFiles(repo)
	if err != nil || len(paths) != 0 {
		t.Fatalf("foreign-only teaching file entered corpus: %v, %v", paths, err)
	}
}

func platformBodyEmbeds(repoRoot string) ([]string, error) {
	var bodyEmbeds []string
	err := checkoutsource.WalkDir(repoRoot, repoRoot, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			if entry.Name() == "vendor" {
				return filepath.SkipDir
			}
			return nil
		}
		if filepath.Ext(path) != ".go" || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		body, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		text := string(body)
		if strings.Contains(text, "//go:embed") && (strings.Contains(text, "provider-triggers") || strings.Contains(text, "provider-connectors") || strings.Contains(text, "channels/*") || strings.Contains(text, "inventory.yaml")) {
			relative, err := filepath.Rel(repoRoot, path)
			if err != nil {
				return err
			}
			bodyEmbeds = append(bodyEmbeds, filepath.ToSlash(relative))
		}
		return nil
	})
	sort.Strings(bodyEmbeds)
	return bodyEmbeds, err
}

func platformPackTeachingFiles(repoRoot string) ([]string, error) {
	var paths []string
	for _, root := range []string{".github", "examples"} {
		err := checkoutsource.WalkDir(repoRoot, filepath.Join(repoRoot, root), func(path string, entry os.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if entry.IsDir() || (filepath.Ext(path) != ".yaml" && filepath.Ext(path) != ".yml" && filepath.Ext(path) != ".md") {
				return nil
			}
			if root == ".github" {
				// Git omits empty fixture directories; walk their retained parent
				// and include workflow consumers, but not historical audit prose.
				relative, err := filepath.Rel(filepath.Join(repoRoot, root), path)
				if err != nil {
					return err
				}
				relative = filepath.ToSlash(relative)
				if !strings.HasPrefix(relative, "fixtures/") && !strings.HasPrefix(relative, "workflows/") {
					return nil
				}
			}
			paths = append(paths, path)
			return nil
		})
		if err != nil {
			return nil, err
		}
	}
	sort.Strings(paths)
	return paths, nil
}

func pathSet(paths ...string) map[string]struct{} {
	result := make(map[string]struct{}, len(paths))
	for _, path := range paths {
		result[path] = struct{}{}
	}
	return result
}

func missingPaths(want, got map[string]struct{}) []string {
	var missing []string
	for path := range want {
		if _, ok := got[path]; !ok {
			missing = append(missing, path)
		}
	}
	sort.Strings(missing)
	return missing
}

func assertNoRetiredPackConfig(t *testing.T, path, body string) {
	t.Helper()
	for _, retired := range []string{"external_dirs:", "provider_triggers:\n", "provider_triggers:\r\n", "channels:\n  packs:", "channels:\r\n  packs:"} {
		if strings.Contains(body, retired) {
			t.Errorf("%s retains retired per-kind pack configuration %q", path, retired)
		}
	}
}
