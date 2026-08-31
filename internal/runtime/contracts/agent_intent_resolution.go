package contracts

import (
	"fmt"
	"path"
	"path/filepath"
	"regexp"
	"strings"
	"unicode/utf8"

	runtimeagentintent "github.com/division-sh/swarm/internal/runtime/agentintent"
	"github.com/division-sh/swarm/internal/sourceartifact"
)

var windowsAbsoluteIntentPath = regexp.MustCompile(`^[A-Za-z]:`)

func materializeAgentIntentsFromSource(artifact *sourceartifact.AdmittedSourceArtifact, source ContractItemSource, entries map[string]AgentRegistryEntry) (map[string]AgentRegistryEntry, error) {
	if len(entries) == 0 {
		return entries, nil
	}
	declaration := strings.TrimSpace(source.File)
	if artifact == nil || declaration == "" {
		return nil, fmt.Errorf("agent intent resolution requires an admitted artifact and exact agents.yaml label")
	}
	declarationDir := path.Dir(declaration)
	if declarationDir == "." {
		declarationDir = ""
	}
	out := make(map[string]AgentRegistryEntry, len(entries))
	for _, key := range sortedContractKeys(entries) {
		entry := entries[key]
		if err := entry.Intent.ValidateSyntax(); err != nil {
			return nil, fmt.Errorf("%s agent %q intent: %w", declaration, key, err)
		}
		provenance, err := runtimeagentintent.NewDeclarationProvenance(declaration, strings.TrimSpace(key))
		if err != nil {
			return nil, fmt.Errorf("%s agent %q intent provenance: %w", declaration, key, err)
		}
		switch entry.Intent.Kind {
		case runtimeagentintent.SourceInline:
			entry.ResolvedIntent, err = runtimeagentintent.Resolve(runtimeagentintent.SourceInline, "inline", provenance, entry.Intent.Inline)
		case runtimeagentintent.SourceLocal:
			var rel string
			rel, err = validateLocalIntentPath(entry.Intent.Local)
			if err == nil {
				label := path.Join(declarationDir, rel)
				var admitted sourceartifact.Entry
				var ok bool
				admitted, ok = artifact.Entry(label)
				if !ok {
					err = fmt.Errorf("local path %q is not an admitted source member", entry.Intent.Local)
				} else if !utf8.Valid(admitted.Bytes()) {
					err = fmt.Errorf("local path %q must contain valid UTF-8", entry.Intent.Local)
				} else {
					entry.ResolvedIntent, err = runtimeagentintent.Resolve(runtimeagentintent.SourceLocal, label, provenance, string(admitted.Bytes()))
				}
			}
		case runtimeagentintent.SourceImport:
			err = fmt.Errorf("import %q is unavailable until the agent-pack source owner is gated under #1685/#1770; use local or inline intent", entry.Intent.Import)
		default:
			err = fmt.Errorf("intent source kind %q is unsupported", entry.Intent.Kind)
		}
		if err != nil {
			return nil, fmt.Errorf("%s agent %q intent: %w", declaration, key, err)
		}
		out[key] = entry
	}
	return out, nil
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
		ascii := asciiFoldContractLabel(label)
		if existing, ok := folded[ascii]; ok && existing != label {
			errs = append(errs, fmt.Errorf("case-colliding agent intent coordinates %q and %q", existing, label))
		} else {
			folded[ascii] = label
		}
	}
	return errs
}
