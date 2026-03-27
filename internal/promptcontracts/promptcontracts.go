package promptcontracts

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"sync"

	"gopkg.in/yaml.v3"
)

var (
	promptAgentIDPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9-]*$`)
	promptModePattern    = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]*$`)
	promptTokenPattern   = regexp.MustCompile(`\{\{([a-zA-Z0-9_]+)\}\}`)

	promptVariablesMu    sync.RWMutex
	promptVariablesCache = map[string]map[string]any{}
)

// Load reads an agent prompt from the configured prompt directory with optional
// mode variant.
// Lookup order:
// 1) {agent-id}.{mode}.md (when mode is non-empty)
// 2) {agent-id}.md
func Load(agentID, mode string) (prompt string, found bool, err error) {
	dir, ok := ResolveDir()
	if !ok {
		return "", false, nil
	}
	return LoadFromDir(dir, agentID, mode)
}

// LoadFromDir reads an agent prompt from the provided prompt directory.
func LoadFromDir(promptsDir, agentID, mode string) (prompt string, found bool, err error) {
	agentID = strings.TrimSpace(agentID)
	mode = strings.TrimSpace(strings.ToLower(mode))
	if agentID == "" {
		return "", false, fmt.Errorf("agent id is required")
	}
	if !promptAgentIDPattern.MatchString(agentID) {
		return "", false, fmt.Errorf("invalid agent id %q", agentID)
	}
	if mode != "" && !promptModePattern.MatchString(mode) {
		return "", false, fmt.Errorf("invalid prompt mode %q", mode)
	}

	candidates := make([]string, 0, 2)
	if mode != "" {
		candidates = append(candidates, filepath.Join(promptsDir, agentID+"."+mode+".md"))
	}
	candidates = append(candidates, filepath.Join(promptsDir, agentID+".md"))

	for _, p := range candidates {
		raw, readErr := os.ReadFile(p)
		if readErr != nil {
			if os.IsNotExist(readErr) {
				continue
			}
			return "", false, readErr
		}
		rendered, renderErr := renderPromptTemplateForDir(promptsDir, string(raw))
		if renderErr != nil {
			return "", false, fmt.Errorf("render prompt %s: %w", filepath.Base(p), renderErr)
		}
		return strings.TrimSpace(rendered), true, nil
	}
	return "", false, nil
}

func renderPromptTemplateForDir(promptsDir, promptText string) (string, error) {
	if !promptTokenPattern.MatchString(promptText) {
		return promptText, nil
	}
	vars, err := loadPromptVariables(promptsDir)
	if err != nil {
		return "", err
	}
	rendered := renderPromptTemplate(promptText, vars)
	if unresolved := unresolvedPromptTokens(rendered); len(unresolved) > 0 {
		return "", &UnresolvedPromptVariablesError{Variables: unresolved}
	}
	return rendered, nil
}

func loadPromptVariables(promptsDir string) (map[string]any, error) {
	promptsDir = filepath.Clean(promptsDir)
	promptVariablesMu.RLock()
	if cached, ok := promptVariablesCache[promptsDir]; ok {
		promptVariablesMu.RUnlock()
		return cached, nil
	}
	promptVariablesMu.RUnlock()

	sources := promptVariableSources(promptsDir)
	if len(sources) == 0 {
		return nil, fmt.Errorf("missing prompt variables source near %s", promptsDir)
	}
	vars := map[string]any{}
	for _, source := range sources {
		raw, err := os.ReadFile(source)
		if err != nil {
			return nil, fmt.Errorf("read prompt variable source %s: %w", source, err)
		}
		loaded := map[string]any{}
		if err := yaml.Unmarshal(raw, &loaded); err != nil {
			return nil, fmt.Errorf("parse prompt variables from %s: %w", source, err)
		}
		for key, value := range loaded {
			vars[key] = value
		}
	}

	promptVariablesMu.Lock()
	promptVariablesCache[promptsDir] = vars
	promptVariablesMu.Unlock()
	return vars, nil
}

func promptVariableSources(promptsDir string) []string {
	base := filepath.Dir(promptsDir)
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
	sources := make([]string, 0, len(dirs))
	for i := len(dirs) - 1; i >= 0; i-- {
		for _, candidate := range []string{
			filepath.Join(dirs[i], "prompt-variables.yaml"),
			filepath.Join(dirs[i], "policy.yaml"),
			filepath.Join(dirs[i], "runtime", "policy.yaml"),
		} {
			if _, err := os.Stat(candidate); err == nil {
				sources = append(sources, candidate)
			}
		}
	}
	return sources
}

func renderPromptTemplate(promptText string, vars map[string]any) string {
	matches := promptTokenPattern.FindAllStringSubmatchIndex(promptText, -1)
	if len(matches) == 0 {
		return promptText
	}
	var out strings.Builder
	last := 0
	for _, m := range matches {
		start, end := m[0], m[1]
		keyStart, keyEnd := m[2], m[3]
		key := promptText[keyStart:keyEnd]

		out.WriteString(promptText[last:start])
		replacement := promptText[start:end]
		if value, ok := vars[key]; ok {
			replacement = renderPromptValue(value)
			prefix := promptLinePrefix(promptText, start)
			if prefix != "" {
				replacement = indentRenderedValue(replacement, prefix)
			}
		}
		out.WriteString(replacement)
		last = end
	}
	out.WriteString(promptText[last:])
	return out.String()
}

func renderPromptValue(value any) string {
	switch v := value.(type) {
	case nil:
		return ""
	case string:
		return v
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64, bool:
		return fmt.Sprintf("%v", v)
	default:
		raw, err := yaml.Marshal(v)
		if err != nil {
			return fmt.Sprintf("%v", v)
		}
		return strings.TrimSpace(string(raw))
	}
}

func promptLinePrefix(promptText string, tokenStart int) string {
	lineStart := strings.LastIndex(promptText[:tokenStart], "\n") + 1
	prefix := promptText[lineStart:tokenStart]
	if strings.TrimSpace(prefix) != "" {
		return ""
	}
	return prefix
}

func indentRenderedValue(rendered, prefix string) string {
	if prefix == "" || !strings.Contains(rendered, "\n") {
		return rendered
	}
	lines := strings.Split(rendered, "\n")
	for i := 1; i < len(lines); i++ {
		lines[i] = prefix + lines[i]
	}
	return strings.Join(lines, "\n")
}

func unresolvedPromptTokens(promptText string) []string {
	matches := promptTokenPattern.FindAllStringSubmatch(promptText, -1)
	if len(matches) == 0 {
		return nil
	}
	set := make(map[string]struct{}, len(matches))
	for _, m := range matches {
		if len(m) < 2 {
			continue
		}
		key := strings.TrimSpace(m[1])
		if key == "" {
			continue
		}
		set[key] = struct{}{}
	}
	out := make([]string, 0, len(set))
	for key := range set {
		out = append(out, key)
	}
	sort.Strings(out)
	return out
}

// ResolveDir discovers the default Swarm prompt directory. It checks:
// 1) SWARM_PROMPTS_DIR
// 2) any docs/specs/swarm-platform/*/contracts/prompts walking up from CWD
// 3) any docs/specs/swarm-platform/*/contracts/prompts relative to this repo.
func ResolveDir() (string, bool) {
	if env := strings.TrimSpace(os.Getenv("SWARM_PROMPTS_DIR")); env != "" {
		if isDir(env) {
			return filepath.Clean(env), true
		}
	}

	if cwd, err := os.Getwd(); err == nil {
		if dir, ok := findAnyPromptDirUp(cwd); ok {
			return dir, true
		}
	}

	if _, thisFile, _, ok := runtime.Caller(0); ok {
		repoRoot := filepath.Clean(filepath.Join(filepath.Dir(thisFile), "..", ".."))
		if dir, ok := findAnyPromptDirUp(repoRoot); ok {
			return dir, true
		}
	}

	return "", false
}

func allowedRuntimePromptTokens() map[string]struct{} {
	return map[string]struct{}{
		"current_date":       {},
		"agent_id":           {},
		"flow_instance_path": {},
	}
}

func isAllowedRuntimePromptToken(token string) bool {
	_, ok := allowedRuntimePromptTokens()[strings.TrimSpace(token)]
	return ok
}

func findDirUp(start string, pathParts ...string) (string, bool) {
	cur := filepath.Clean(start)
	for {
		candidate := filepath.Join(append([]string{cur}, pathParts...)...)
		if isDir(candidate) {
			return candidate, true
		}
		next := filepath.Dir(cur)
		if next == cur {
			return "", false
		}
		cur = next
	}
}

func findAnyPromptDirUp(start string) (string, bool) {
	cur := filepath.Clean(start)
	for {
		if dir, ok := findPromptDirUnder(filepath.Join(cur, "docs", "specs")); ok {
			return dir, true
		}
		next := filepath.Dir(cur)
		if next == cur {
			return "", false
		}
		cur = next
	}
}

func findPromptDirUnder(specRoot string) (string, bool) {
	if !isDir(specRoot) {
		return "", false
	}
	matches := make([]string, 0, 8)
	_ = filepath.WalkDir(specRoot, func(path string, d os.DirEntry, err error) error {
		if err != nil || !d.IsDir() {
			return nil
		}
		if filepath.Base(path) == "prompts" && filepath.Base(filepath.Dir(path)) == "contracts" {
			matches = append(matches, filepath.Clean(path))
			return filepath.SkipDir
		}
		return nil
	})
	sort.Strings(matches)
	for _, match := range matches {
		if isDir(match) {
			return match, true
		}
	}
	return "", false
}

func isDir(path string) bool {
	info, err := os.Stat(path)
	return err == nil && info.IsDir()
}
