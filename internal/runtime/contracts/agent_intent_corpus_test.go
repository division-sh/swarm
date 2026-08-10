package contracts

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	runtimeagentintent "github.com/division-sh/swarm/internal/runtime/agentintent"
	"gopkg.in/yaml.v3"
)

func TestCheckedInAgentIntentCorpusHasExplicitSources(t *testing.T) {
	repo := repoRoot(t)
	roots := []string{
		"examples",
		"tests",
		"internal/releasee2e/testdata",
		"internal/runtime/testdata",
		"internal/runtime/runforkexecution/testdata",
	}
	negativeFixtures := map[string]string{
		"tests/tier8-boot-verification/test-boot-prompt-missing/agents.yaml":  "missing_intent",
		"tests/tier8-boot-verification/test-boot-prompt-ref/agents.yaml":      "retired_prompt_ref",
		"tests/tier8-boot-verification/test-boot-prompt-ref-stub/agents.yaml": "retired_prompt_ref",
	}

	agentFiles := corpusFilesNamed(t, repo, roots, "agents.yaml")
	if len(agentFiles) == 0 {
		t.Fatal("checked-in agent corpus is empty")
	}
	referencedLocalFiles := map[string]string{}
	seenNegative := map[string]bool{}
	for _, absolutePath := range agentFiles {
		rel, err := filepath.Rel(repo, absolutePath)
		if err != nil {
			t.Fatal(err)
		}
		rel = filepath.ToSlash(rel)
		t.Run(rel, func(t *testing.T) {
			class, refs := classifyAgentIntentCorpusFile(t, absolutePath, negativeFixtures[rel])
			if class != "explicit_intent" && class != "empty_registry" {
				seenNegative[rel] = true
			}
			for path, owner := range refs {
				if previous, exists := referencedLocalFiles[path]; exists {
					t.Fatalf("local intent %s is declared by both %s and %s", path, previous, owner)
				}
				referencedLocalFiles[path] = owner
			}
		})
	}
	for path := range negativeFixtures {
		if !seenNegative[path] {
			t.Errorf("negative intent corpus fixture %s was not classified", path)
		}
	}

	for _, path := range corpusMarkdownUnderPrompts(t, repo, roots) {
		rel, err := filepath.Rel(repo, path)
		if err != nil {
			t.Fatal(err)
		}
		t.Run("old-prompt-directory/"+filepath.ToSlash(rel), func(t *testing.T) {
			if owner := referencedLocalFiles[filepath.Clean(path)]; owner == "" {
				t.Fatalf("old prompts/ file %s is undeclared; remove it or declare it with intent:", rel)
			}
		})
	}
}

func classifyAgentIntentCorpusFile(t testing.TB, path, negativeClass string) (string, map[string]string) {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var document yaml.Node
	if err := yaml.Unmarshal(raw, &document); err != nil {
		t.Fatalf("parse agents.yaml: %v", err)
	}
	if len(document.Content) != 1 || document.Content[0].Kind != yaml.MappingNode {
		t.Fatalf("agents.yaml must be a mapping")
	}
	registry := document.Content[0]
	if len(registry.Content) == 0 {
		if negativeClass != "" {
			t.Fatalf("negative fixture %q has no agent declaration", negativeClass)
		}
		return "empty_registry", nil
	}
	references := map[string]string{}
	for i := 0; i+1 < len(registry.Content); i += 2 {
		agentID := strings.TrimSpace(registry.Content[i].Value)
		definition := registry.Content[i+1]
		if definition.Kind != yaml.MappingNode {
			t.Fatalf("agent %q definition must be a mapping", agentID)
		}
		fields := yamlMappingFields(definition)
		switch negativeClass {
		case "missing_intent":
			if fields["intent"] != nil || fields["prompt_ref"] != nil || fields["prompt_inputs"] != nil {
				t.Fatalf("missing-intent fixture agent %q no longer represents its teaching failure", agentID)
			}
			continue
		case "retired_prompt_ref":
			if fields["prompt_ref"] == nil || fields["intent"] != nil {
				t.Fatalf("retired-prompt fixture agent %q no longer represents its teaching failure", agentID)
			}
			continue
		case "":
		default:
			t.Fatalf("unknown negative corpus classification %q", negativeClass)
		}
		if fields["prompt_ref"] != nil || fields["prompt_inputs"] != nil {
			t.Fatalf("agent %q uses a retired prompt declaration", agentID)
		}
		intentNode := fields["intent"]
		if intentNode == nil {
			t.Fatalf("agent %q has no explicit intent: source", agentID)
		}
		var source runtimeagentintent.Source
		if err := source.UnmarshalYAML(intentNode); err != nil {
			t.Fatalf("agent %q intent: %v", agentID, err)
		}
		if source.Kind != runtimeagentintent.SourceLocal {
			continue
		}
		rel, err := validateLocalIntentPath(source.Local)
		if err != nil {
			t.Fatalf("agent %q intent: %v", agentID, err)
		}
		intentPath := filepath.Clean(filepath.Join(filepath.Dir(path), filepath.FromSlash(rel)))
		info, err := os.Stat(intentPath)
		if err != nil {
			t.Fatalf("agent %q local intent %q: %v", agentID, source.Local, err)
		}
		if !info.Mode().IsRegular() {
			t.Fatalf("agent %q local intent %q is not a regular file", agentID, source.Local)
		}
		references[intentPath] = fmt.Sprintf("%s#agents.%s.intent", filepath.ToSlash(path), agentID)
	}
	if negativeClass != "" {
		return negativeClass, references
	}
	return "explicit_intent", references
}

func yamlMappingFields(node *yaml.Node) map[string]*yaml.Node {
	fields := make(map[string]*yaml.Node, len(node.Content)/2)
	for i := 0; i+1 < len(node.Content); i += 2 {
		fields[strings.TrimSpace(node.Content[i].Value)] = node.Content[i+1]
	}
	return fields
}

func corpusFilesNamed(t testing.TB, repo string, roots []string, name string) []string {
	t.Helper()
	var files []string
	for _, root := range roots {
		base := filepath.Join(repo, filepath.FromSlash(root))
		if err := filepath.WalkDir(base, func(path string, entry fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if !entry.IsDir() && entry.Name() == name {
				files = append(files, path)
			}
			return nil
		}); err != nil {
			t.Fatalf("walk %s: %v", root, err)
		}
	}
	sort.Strings(files)
	return files
}

func corpusMarkdownUnderPrompts(t testing.TB, repo string, roots []string) []string {
	t.Helper()
	var files []string
	for _, root := range roots {
		base := filepath.Join(repo, filepath.FromSlash(root))
		if err := filepath.WalkDir(base, func(path string, entry fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if entry.IsDir() || !strings.EqualFold(filepath.Ext(entry.Name()), ".md") {
				return nil
			}
			rel, err := filepath.Rel(repo, path)
			if err != nil {
				return err
			}
			for _, segment := range strings.Split(filepath.ToSlash(rel), "/") {
				if segment == "prompts" {
					files = append(files, path)
					break
				}
			}
			return nil
		}); err != nil {
			t.Fatalf("walk %s: %v", root, err)
		}
	}
	sort.Strings(files)
	return files
}
