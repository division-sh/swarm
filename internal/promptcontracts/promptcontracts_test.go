package promptcontracts

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	runtimecontracts "swarm/internal/runtime/contracts"
	"gopkg.in/yaml.v3"
)

func TestLoadFromDir_DefaultAndModeVariant(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "prompts")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir prompts dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, "prompt-variables.yaml"), []byte("signal_threshold: 55\n"), 0o644); err != nil {
		t.Fatalf("write prompt variables: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "generic-agent.md"), []byte("default prompt"), 0o644); err != nil {
		t.Fatalf("write default prompt: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "generic-agent.corpus.md"), []byte("corpus prompt"), 0o644); err != nil {
		t.Fatalf("write mode prompt: %v", err)
	}

	got, found, err := LoadFromDir(dir, "generic-agent", "corpus")
	if err != nil {
		t.Fatalf("load mode prompt: %v", err)
	}
	if !found || got != "corpus prompt" {
		t.Fatalf("expected mode prompt, found=%v got=%q", found, got)
	}

	got, found, err = LoadFromDir(dir, "generic-agent", "saas_gap")
	if err != nil {
		t.Fatalf("load fallback prompt: %v", err)
	}
	if !found || got != "default prompt" {
		t.Fatalf("expected default fallback, found=%v got=%q", found, got)
	}
}

func TestLoadFromDir_RejectsInvalidPathTokens(t *testing.T) {
	dir := t.TempDir()
	if _, _, err := LoadFromDir(dir, "../bad", ""); err == nil {
		t.Fatal("expected invalid agent id error")
	}
	if _, _, err := LoadFromDir(dir, "good-agent", "../bad"); err == nil {
		t.Fatal("expected invalid mode error")
	}
}

func TestLoadFromDir_RendersPromptVariables(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "prompts")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir prompts dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, "prompt-variables.yaml"), []byte(`
signal_threshold: 55
blocking_red_flags:
  - complex_integration
  - high_feature_count
tier2_capabilities:
  - name: Email sending
    status: planned
`), 0o644); err != nil {
		t.Fatalf("write prompt variables: %v", err)
	}
	prompt := `
Threshold: {{signal_threshold}}
Blocking:
  {{blocking_red_flags}}
Tier2:
  {{tier2_capabilities}}
`
	if err := os.WriteFile(filepath.Join(dir, "generic-agent.md"), []byte(prompt), 0o644); err != nil {
		t.Fatalf("write prompt: %v", err)
	}

	got, found, err := LoadFromDir(dir, "generic-agent", "")
	if err != nil {
		t.Fatalf("load prompt with variables: %v", err)
	}
	if !found {
		t.Fatal("expected prompt to be found")
	}
	if strings.Contains(got, "{{") {
		t.Fatalf("expected rendered prompt with no unresolved variables, got:\n%s", got)
	}
	wantSnippets := []string{
		"Threshold: 55",
		"  - complex_integration",
		"  - high_feature_count",
		"  - name: Email sending",
		"    status: planned",
	}
	for _, snippet := range wantSnippets {
		if !strings.Contains(got, snippet) {
			t.Fatalf("expected rendered prompt to contain %q, got:\n%s", snippet, got)
		}
	}
}

func TestLoadFromDir_FailsOnUnresolvedPromptVariable(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "prompts")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir prompts dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, "prompt-variables.yaml"), []byte("signal_threshold: 55\n"), 0o644); err != nil {
		t.Fatalf("write prompt variables: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "analysis-agent.md"), []byte("Threshold={{signal_threshold}} Missing={{unknown_value}}"), 0o644); err != nil {
		t.Fatalf("write prompt: %v", err)
	}

	if _, _, err := LoadFromDir(dir, "analysis-agent", ""); err == nil {
		t.Fatal("expected unresolved prompt variable error")
	} else if !errors.Is(err, ErrUnresolvedPromptVariables) {
		t.Fatalf("expected unresolved variable name in error, got %v", err)
	}
}

func TestPromptVariablesComplete(t *testing.T) {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve caller path")
	}
	repoRoot := filepath.Clean(filepath.Join(filepath.Dir(thisFile), "..", ".."))
	contractsDir := runtimecontracts.DefaultWorkflowContractsDir(repoRoot)
	if strings.TrimSpace(contractsDir) == "" {
		t.Skip("no default workflow contracts dir")
	}

	var files []string
	if err := filepath.WalkDir(contractsDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		if filepath.Ext(path) != ".md" || filepath.Base(filepath.Dir(path)) != "prompts" {
			return nil
		}
		files = append(files, path)
		return nil
	}); err != nil {
		t.Fatalf("walk prompts: %v", err)
	}
	if len(files) == 0 {
		t.Fatalf("no prompt files found under %s", contractsDir)
	}

	missingByPrompt := make(map[string][]string)
	for _, p := range files {
		raw, err := os.ReadFile(p)
		if err != nil {
			t.Fatalf("read %s: %v", p, err)
		}
		tokens := unresolvedPromptTokens(string(raw))
		if len(tokens) == 0 {
			continue
		}
		vars := loadPromptVarsForTest(t, filepath.Dir(p))
		missing := make([]string, 0, len(tokens))
		for _, tok := range tokens {
			if _, ok := vars[tok]; ok {
				continue
			}
			if isAllowedRuntimePromptToken(tok) {
				continue
			}
			if strings.EqualFold(tok, "variable") {
				continue
			}
			missing = append(missing, tok)
		}
		if len(missing) > 0 {
			missingByPrompt[filepath.ToSlash(strings.TrimPrefix(p, contractsDir+string(filepath.Separator)))] = missing
		}
	}
	if len(missingByPrompt) > 0 {
		t.Fatalf("missing prompt variable definitions: %+v", missingByPrompt)
	}

	for _, p := range files {
		if hasAllowedRuntimePromptTokens(t, p) {
			continue
		}
		raw, err := os.ReadFile(p)
		if err != nil {
			t.Fatalf("read %s: %v", p, err)
		}
		rendered := renderPromptTemplate(string(raw), loadPromptVarsForTest(t, filepath.Dir(p)))
		if unresolved := unresolvedPromptTokens(rendered); len(unresolved) > 0 {
			t.Fatalf("rendered prompt %s still has unresolved variables: %v", filepath.Base(p), unresolved)
		}
	}
}

func loadPromptVarsForTest(t *testing.T, promptsDir string) map[string]any {
	t.Helper()

	vars := map[string]any{}
	for _, candidate := range promptVariableSources(promptsDir) {
		raw, err := os.ReadFile(candidate)
		if err != nil {
			t.Fatalf("read %s: %v", candidate, err)
		}
		loaded := map[string]any{}
		if err := yaml.Unmarshal(raw, &loaded); err != nil {
			t.Fatalf("parse %s: %v", candidate, err)
		}
		for key, value := range loaded {
			vars[key] = value
		}
	}
	for _, candidate := range promptSchemaSources(promptsDir) {
		raw, err := os.ReadFile(candidate)
		if err != nil {
			t.Fatalf("read %s: %v", candidate, err)
		}
		var loaded struct {
			InstanceVariables struct {
				Variables map[string]any `yaml:"variables"`
			} `yaml:"instance_variables"`
		}
		if err := yaml.Unmarshal(raw, &loaded); err != nil {
			t.Fatalf("parse %s: %v", candidate, err)
		}
		for key := range loaded.InstanceVariables.Variables {
			vars[key] = true
		}
	}
	for _, candidate := range promptAgentInputSources(promptsDir) {
		raw, err := os.ReadFile(candidate)
		if err != nil {
			t.Fatalf("read %s: %v", candidate, err)
		}
		loaded := map[string]struct {
			PromptInputs []string `yaml:"prompt_inputs"`
		}{}
		if err := yaml.Unmarshal(raw, &loaded); err != nil {
			t.Fatalf("parse %s: %v", candidate, err)
		}
		for _, entry := range loaded {
			for _, key := range entry.PromptInputs {
				key = strings.TrimSpace(key)
				if key == "" {
					continue
				}
				vars[key] = true
			}
		}
	}
	return vars
}

func promptContractAncestorDirs(promptsDir string) []string {
	base := filepath.Clean(filepath.Dir(promptsDir))
	dirs := make([]string, 0, 8)
	for dir := base; ; dir = filepath.Dir(dir) {
		dirs = append(dirs, dir)
		if filepath.Base(dir) == "contracts" {
			break
		}
		next := filepath.Dir(dir)
		if next == dir {
			break
		}
	}
	return dirs
}

func promptSchemaSources(promptsDir string) []string {
	var out []string
	for _, dir := range promptContractAncestorDirs(promptsDir) {
		candidate := filepath.Join(dir, "schema.yaml")
		if _, err := os.Stat(candidate); err == nil {
			out = append(out, candidate)
		}
	}
	return out
}

func promptAgentInputSources(promptsDir string) []string {
	var out []string
	for _, dir := range promptContractAncestorDirs(promptsDir) {
		candidate := filepath.Join(dir, "agents.yaml")
		if _, err := os.Stat(candidate); err == nil {
			out = append(out, candidate)
		}
	}
	return out
}

func hasAllowedRuntimePromptTokens(t *testing.T, path string) bool {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	for _, tok := range unresolvedPromptTokens(string(raw)) {
		if isAllowedRuntimePromptToken(tok) || strings.EqualFold(strings.TrimSpace(tok), "variable") {
			return true
		}
	}
	return false
}

func isAllowedRuntimePromptToken(token string) bool {
	switch strings.TrimSpace(token) {
	case "name",
		"type",
		"entity_name",
		"entity_description",
		"geography",
		"mandate_document",
		"founder_directives",
		"org_roster",
		"monthly_api_cap",
		"product_budget",
		"growth_budget":
		return true
	default:
		return false
	}
}
