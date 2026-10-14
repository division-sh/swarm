package cliapp

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestGeneratedUnifiedConfigExampleUsesOneDeploymentFile(t *testing.T) {
	text := generatedUnifiedConfigExample()
	for _, want := range []string{
		".swarm/swarm.yaml or --config <file>",
		"SWARM_CONFIG selects the same file",
		"No other config files are read",
		"# Deployment keys\n",
		"# Secret references: never store plaintext secrets in config\n",
	} {
		if !strings.Contains(text, want) {
			t.Errorf("generated example missing deployment guidance %q", want)
		}
	}
	for _, retired := range []string{
		"./swarm.yaml", "user-global", "Project-safe", "Project-contained",
		"Settings for this machine only", "local/operator", "machine/operator",
		"shared project config", "checked-in", "trust tier",
	} {
		if strings.Contains(text, retired) {
			t.Errorf("generated example still advertises retired guidance %q", retired)
		}
	}
}

func TestGeneratedUnifiedConfigExampleSectionsMatchSourcePolicy(t *testing.T) {
	_, deployment, ok := strings.Cut(generatedUnifiedConfigExample(), "# Deployment keys\n")
	if !ok {
		t.Fatal("missing deployment section")
	}
	deployment, secrets, ok := strings.Cut(deployment, "# Secret references: never store plaintext secrets in config\n")
	if !ok {
		t.Fatal("missing secret-reference section")
	}
	seen := map[string]bool{}
	for _, section := range []struct {
		text   string
		secret bool
	}{{deployment, false}, {secrets, true}} {
		var raw strings.Builder
		for _, line := range strings.Split(section.text, "\n") {
			if line == "" {
				continue
			}
			if !strings.HasPrefix(line, "# ") {
				t.Fatalf("example contains an active config line: %q", line)
			}
			raw.WriteString(strings.TrimPrefix(line, "# "))
			raw.WriteByte('\n')
		}
		var document map[string]any
		if err := yaml.Unmarshal([]byte(raw.String()), &document); err != nil {
			t.Fatalf("decode generated section (secret=%v): %v", section.secret, err)
		}
		var check func(map[string]any, []string)
		check = func(fields map[string]any, prefix []string) {
			for name, value := range fields {
				path := append(append([]string(nil), prefix...), name)
				if children, ok := value.(map[string]any); ok {
					check(children, path)
					continue
				}
				key := strings.Join(path, ".")
				if seen[key] {
					t.Errorf("example repeats leaf %s", key)
				}
				seen[key] = true
				rule, ok := unifiedConfigRule(path)
				if !ok || !rule.SupportedExampleLeaf() {
					t.Errorf("example advertises unsupported leaf %s", key)
				}
				if rule.SecretReference != section.secret {
					t.Errorf("leaf %s is in secret=%v section, source policy requires secret=%v", key, section.secret, rule.SecretReference)
				}
			}
		}
		check(document, nil)
	}
	for _, entry := range unifiedConfigExampleEntries() {
		if !seen[entry.Path] {
			t.Errorf("example dropped configured leaf %s", entry.Path)
		}
	}
}

func TestGeneratedUnifiedConfigExampleLoadsFromAdvertisedSelections(t *testing.T) {
	for _, selector := range []string{"deployment", "flag", "env"} {
		t.Run(selector, func(t *testing.T) {
			isolateCLIAPIConfigEnv(t)
			root := t.TempDir()
			path := filepath.Join(root, ".swarm", "swarm.yaml")
			wantSource := unifiedConfigSourceDeployment
			opts := unifiedConfigLoadOptions{RepoRoot: root}
			if selector != "deployment" {
				path = filepath.Join(root, "selected.yaml")
				wantSource = unifiedConfigSourceExplicit
				if selector == "flag" {
					opts.ExplicitPath = path
				} else {
					t.Setenv("SWARM_CONFIG", path)
				}
			}
			if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
				t.Fatal(err)
			}
			writeRuntimeConfigText(t, path, generatedUnifiedConfigValidationSample())
			writeRuntimeConfigText(t, filepath.Join(root, "swarm.yaml"), "runtime: null\n")
			got, err := loadUnifiedConfigForTest(t, opts)
			if err != nil {
				t.Fatalf("generated sample from %s: %v", selector, err)
			}
			if got.Path != path || got.Source != wantSource || !got.validated {
				t.Fatalf("sample admission path/source/validated = %q/%q/%v, want %q/%q/true", got.Path, got.Source, got.validated, path, wantSource)
			}
			if got.CLI.Connection.APIServer != "http://127.0.0.1:8081" || got.Config.Database.PasswordSecretKey != "postgres_password" {
				t.Fatalf("sample lost deployment target or password reference")
			}
		})
	}
}
