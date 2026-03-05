package promptcontracts

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
)

var (
	promptAgentIDPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9-]*$`)
	promptModePattern    = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]*$`)
)

// Load reads an agent prompt from contracts/prompts with optional mode variant.
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
		return strings.TrimSpace(string(raw)), true, nil
	}
	return "", false, nil
}

// ResolveDir discovers contracts/prompts. It checks:
// 1) EMPIREAI_PROMPTS_DIR
// 2) contracts/prompts walking up from CWD
// 3) contracts/prompts relative to the repo root derived from this source file.
func ResolveDir() (string, bool) {
	if env := strings.TrimSpace(os.Getenv("EMPIREAI_PROMPTS_DIR")); env != "" {
		if isDir(env) {
			return filepath.Clean(env), true
		}
	}

	if cwd, err := os.Getwd(); err == nil {
		if dir, ok := findDirUp(cwd, "contracts", "prompts"); ok {
			return dir, true
		}
	}

	if _, thisFile, _, ok := runtime.Caller(0); ok {
		repoRoot := filepath.Clean(filepath.Join(filepath.Dir(thisFile), "..", ".."))
		if dir := filepath.Join(repoRoot, "contracts", "prompts"); isDir(dir) {
			return dir, true
		}
	}

	return "", false
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

func isDir(path string) bool {
	info, err := os.Stat(path)
	return err == nil && info.IsDir()
}
