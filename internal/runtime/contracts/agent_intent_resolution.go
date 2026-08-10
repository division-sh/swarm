package contracts

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"unicode/utf8"

	runtimeagentintent "github.com/division-sh/swarm/internal/runtime/agentintent"
)

var windowsAbsoluteIntentPath = regexp.MustCompile(`^[A-Za-z]:`)

func materializeAgentIntents(contractsRoot string, source ContractItemSource, entries map[string]AgentRegistryEntry) (map[string]AgentRegistryEntry, error) {
	if len(entries) == 0 {
		return entries, nil
	}
	root, err := filepath.Abs(strings.TrimSpace(contractsRoot))
	if err != nil {
		return nil, fmt.Errorf("resolve contracts root for agent intent: %w", err)
	}
	if strings.TrimSpace(source.File) == "" {
		return nil, fmt.Errorf("agent intent resolution requires the exact declaring agents.yaml source")
	}
	sourcePath, err := filepath.Abs(source.File)
	if err != nil {
		return nil, fmt.Errorf("resolve agent declaration source %q: %w", source.File, err)
	}
	sourceRel, err := pathRelativeToRoot(root, sourcePath)
	if err != nil {
		return nil, fmt.Errorf("agent declaration source %q: %w", source.File, err)
	}
	out := make(map[string]AgentRegistryEntry, len(entries))
	keys := make([]string, 0, len(entries))
	for key := range entries {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		entry := entries[key]
		if err := entry.Intent.ValidateSyntax(); err != nil {
			return nil, fmt.Errorf("%s agent %q intent: %w", source.File, key, err)
		}
		provenance, err := runtimeagentintent.NewDeclarationProvenance(filepath.ToSlash(sourceRel), strings.TrimSpace(key))
		if err != nil {
			return nil, fmt.Errorf("%s agent %q intent provenance: %w", source.File, key, err)
		}
		resolved, err := resolveAgentIntent(root, filepath.Dir(sourcePath), provenance, entry.Intent)
		if err != nil {
			return nil, fmt.Errorf("%s agent %q intent: %w", source.File, key, err)
		}
		entry.ResolvedIntent = resolved
		out[key] = entry
	}
	return out, nil
}

func resolveAgentIntent(root, declarationDir, provenance string, source runtimeagentintent.Source) (runtimeagentintent.Resolved, error) {
	switch source.Kind {
	case runtimeagentintent.SourceInline:
		return runtimeagentintent.Resolve(runtimeagentintent.SourceInline, "inline", provenance, source.Inline)
	case runtimeagentintent.SourceLocal:
		rel, err := validateLocalIntentPath(source.Local)
		if err != nil {
			return runtimeagentintent.Resolved{}, err
		}
		path, err := filepath.Abs(filepath.Join(declarationDir, filepath.FromSlash(rel)))
		if err != nil {
			return runtimeagentintent.Resolved{}, fmt.Errorf("resolve local path %q: %w", source.Local, err)
		}
		rootRel, err := pathRelativeToRoot(root, path)
		if err != nil {
			return runtimeagentintent.Resolved{}, fmt.Errorf("local path %q: %w", source.Local, err)
		}
		info, err := lstatNoSymlinkPath(root, rootRel, source.Local)
		if err != nil {
			return runtimeagentintent.Resolved{}, fmt.Errorf("local path %q cannot be read: %w", source.Local, err)
		}
		if !info.Mode().IsRegular() {
			return runtimeagentintent.Resolved{}, fmt.Errorf("local path %q must be a regular file", source.Local)
		}
		if info.Mode().Perm()&0o444 == 0 {
			return runtimeagentintent.Resolved{}, fmt.Errorf("local path %q is not readable", source.Local)
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return runtimeagentintent.Resolved{}, fmt.Errorf("local path %q cannot be read: %w", source.Local, err)
		}
		if !utf8.Valid(raw) {
			return runtimeagentintent.Resolved{}, fmt.Errorf("local path %q must contain valid UTF-8", source.Local)
		}
		return runtimeagentintent.Resolve(runtimeagentintent.SourceLocal, filepath.ToSlash(rootRel), provenance, string(raw))
	case runtimeagentintent.SourceImport:
		return runtimeagentintent.Resolved{}, fmt.Errorf("import %q is unavailable until the agent-pack source owner is gated under #1685/#1770; use local or inline intent", source.Import)
	default:
		return runtimeagentintent.Resolved{}, fmt.Errorf("intent source kind %q is unsupported", source.Kind)
	}
}

func validateLocalIntentPath(raw string) (string, error) {
	path := strings.TrimSpace(raw)
	if path == "" {
		return "", fmt.Errorf("local path must not be empty")
	}
	if strings.ContainsRune(path, '\x00') {
		return "", fmt.Errorf("local path %q contains a NUL byte", raw)
	}
	if strings.Contains(path, `\`) {
		return "", fmt.Errorf("local path %q must use forward separators and cannot use backslashes", raw)
	}
	if filepath.IsAbs(path) || strings.HasPrefix(path, "/") || strings.HasPrefix(path, "//") || windowsAbsoluteIntentPath.MatchString(path) {
		return "", fmt.Errorf("local path %q must be relative to the declaring agents.yaml", raw)
	}
	segments := strings.Split(path, "/")
	for _, segment := range segments {
		if segment == "" || segment == "." || segment == ".." {
			return "", fmt.Errorf("local path %q contains an empty, dot, or traversal segment", raw)
		}
	}
	return strings.Join(segments, "/"), nil
}

func pathRelativeToRoot(root, path string) (string, error) {
	rel, err := filepath.Rel(root, path)
	if err != nil {
		return "", err
	}
	if rel == "." || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("path %s is outside contracts root %s", path, root)
	}
	return filepath.Clean(rel), nil
}

func validateScopedAgentIntentCoordinates(bundle *WorkflowContractBundle) []error {
	if bundle == nil {
		return nil
	}
	labels := map[string]string{}
	folded := map[string]string{}
	errs := make([]error, 0)
	for _, record := range bundleAgentRecords(bundle) {
		key := contractScopeKey(record.Source, record.LogicalID)
		intent := record.Entry.ResolvedIntent
		if err := intent.Validate(); err != nil {
			errs = append(errs, fmt.Errorf("agent %s resolved intent: %w", key, err))
			continue
		}
		if intent.Kind != runtimeagentintent.SourceLocal {
			continue
		}
		label := strings.TrimSpace(intent.Coordinate)
		if existing, ok := labels[label]; ok && existing != key {
			errs = append(errs, fmt.Errorf("duplicate canonical intent coordinate %q for agents %q and %q", label, existing, key))
		} else {
			labels[label] = key
		}
		ascii := asciiFoldBundleHashLabel(label)
		if existing, ok := folded[ascii]; ok && existing != label {
			errs = append(errs, fmt.Errorf("case-colliding agent intent coordinates %q and %q", existing, label))
		} else {
			folded[ascii] = label
		}
	}
	return errs
}
