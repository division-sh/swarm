package contracts

import (
	"fmt"
	"sort"
	"strings"

	"github.com/division-sh/swarm/internal/platform"
	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/yamlsource"
)

func contractScopeKey(source ContractItemSource, localID string) string {
	identity, err := runtimeidentity.AdmitDeclarationIdentity(
		strings.TrimSpace(source.FlowPath),
		strings.TrimSpace(source.Family),
		strings.TrimSpace(localID),
	)
	if err != nil {
		return ""
	}
	return identity.Key()
}

func contractSameScope(a, b ContractItemSource) bool {
	return strings.TrimSpace(a.FlowPath) == strings.TrimSpace(b.FlowPath) &&
		strings.TrimSpace(a.Family) == strings.TrimSpace(b.Family)
}

func DefaultPlatformSpecFile(repoRoot string) string {
	return platform.DefaultPlatformSpecFile(repoRoot)
}

func cloneEventCatalogEntryMap(in map[string]EventCatalogEntry) map[string]EventCatalogEntry {
	return cloneEventCatalogEntries(in)
}

func cloneAgentRegistryEntry(in AgentRegistryEntry) AgentRegistryEntry {
	out := in
	out.Mock.Source = append([]byte(nil), in.Mock.Source...)
	out.AuthoredFields = cloneBoolMap(in.AuthoredFields)
	out.EffectiveFieldSources = cloneStringMap(in.EffectiveFieldSources)
	if len(in.EntityWrites) > 0 {
		out.EntityWrites = make(map[string]AgentEntityWriteDecl, len(in.EntityWrites))
		for key, value := range in.EntityWrites {
			value.Create.Fields = append([]string(nil), value.Create.Fields...)
			value.Save.Fields = append([]string(nil), value.Save.Fields...)
			out.EntityWrites[key] = value
		}
	}
	if len(in.NativeTools) > 0 {
		out.NativeTools = make(map[string]any, len(in.NativeTools))
		for key, value := range in.NativeTools {
			out.NativeTools[key] = cloneEventSchemaValue(value)
		}
	}
	out.Permissions = append([]string(nil), in.Permissions...)
	out.Subscriptions = append([]string(nil), in.Subscriptions...)
	out.SubscriptionsBootstrap = append([]string(nil), in.SubscriptionsBootstrap...)
	out.SubscribesTo = append([]string(nil), in.SubscribesTo...)
	out.Tools = append([]string(nil), in.Tools...)
	out.ToolsTier2 = append([]string(nil), in.ToolsTier2...)
	out.FlowDataAccess = append([]string(nil), in.FlowDataAccess...)
	out.Criteria = append([]string(nil), in.Criteria...)
	out.EmitEvents = append([]string(nil), in.EmitEvents...)
	return out
}

func cloneToolSchemaEntryMap(in map[string]ToolSchemaEntry) map[string]ToolSchemaEntry {
	out := make(map[string]ToolSchemaEntry, len(in))
	for name, entry := range in {
		out[name] = entry
	}
	return out
}

func normalizeStrings(in []string) []string {
	if len(in) == 0 {
		return nil
	}
	out := make([]string, 0, len(in))
	seen := make(map[string]struct{}, len(in))
	for _, item := range in {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		if _, ok := seen[item]; ok {
			continue
		}
		seen[item] = struct{}{}
		out = append(out, item)
	}
	return out
}

func appendIfMissingString(items []string, value string) []string {
	value = strings.TrimSpace(value)
	if value == "" {
		return items
	}
	for _, item := range items {
		if strings.TrimSpace(item) == value {
			return items
		}
	}
	return append(items, value)
}

func asciiFoldContractLabel(value string) string {
	raw := []byte(value)
	for index, char := range raw {
		if char >= 'A' && char <= 'Z' {
			raw[index] = char + ('a' - 'A')
		}
	}
	return string(raw)
}

func sortedContractKeys[T any](m map[string]T) []string {
	if len(m) == 0 {
		return nil
	}
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

// loadYAMLFile is reserved for the platform-owned specification. Authored
// source is decoded exclusively from AdmittedSourceArtifact snapshots.
func loadYAMLFile(path string, target any) error {
	source, err := yamlsource.LoadFile(path)
	if err != nil {
		if cause, ok := yamlsource.ParseCause(err); ok {
			return wrapLoaderDiagnosticFile(cause, path)
		}
		return fmt.Errorf("read %s: %w", path, err)
	}
	if err := source.Decode(target); err != nil {
		return wrapLoaderDiagnosticFile(err, path)
	}
	return nil
}
