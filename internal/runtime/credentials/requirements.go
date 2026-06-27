package credentials

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

type Requirement struct {
	Kind string
	Name string
}

type Descriptor struct {
	Key        string
	Present    bool
	Source     string
	Writable   bool
	UpdatedAt  *time.Time
	RequiredBy []Requirement
}

var ErrNotWritable = fmt.Errorf("credential store is not writable")

func BuildRequirementIndex(source semanticview.Source) map[string][]Requirement {
	index := map[string][]Requirement{}
	if source == nil {
		return index
	}
	appendToolRequirements(index, source, "", source.ToolEntries())
	for _, scope := range source.ProjectScopes() {
		appendToolRequirements(index, source, strings.TrimSpace(scope.OwningFlowID), scope.Tools)
	}
	for _, scope := range source.FlowScopes() {
		appendToolRequirements(index, source, strings.TrimSpace(scope.ID), scope.Tools)
	}
	if value, ok := semanticview.PolicyValueForFlow(source, "", "mcp_servers"); ok {
		for name, key := range parseMCPServerCredentialKeys(value.Value) {
			key = strings.TrimSpace(key)
			if key == "" {
				continue
			}
			index[key] = append(index[key], Requirement{Kind: "mcp_server", Name: name})
		}
	}
	if value, ok := semanticview.PolicyValueForFlow(source, "", "web_search_provider"); ok {
		for name, key := range parseWebSearchProviderCredentialKeys(value.Value) {
			key = strings.TrimSpace(key)
			if key == "" {
				continue
			}
			index[key] = append(index[key], Requirement{Kind: "web_search_provider", Name: name})
		}
	}
	for key, refs := range index {
		index[key] = normalizeRequirements(refs)
	}
	return index
}

func appendToolRequirements(index map[string][]Requirement, source semanticview.Source, flowID string, entries map[string]runtimecontracts.ToolSchemaEntry) {
	for name, entry := range entries {
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		for _, key := range entry.Credentials {
			key = strings.TrimSpace(key)
			if key == "" {
				continue
			}
			storeKey, mapped := semanticview.CredentialStoreKeyForFlow(source, flowID, key)
			if mapped && strings.TrimSpace(storeKey) == "" {
				continue
			}
			storeKey = strings.TrimSpace(storeKey)
			if storeKey == "" {
				continue
			}
			index[storeKey] = append(index[storeKey], Requirement{Kind: "tool", Name: name})
		}
	}
}

func Describe(ctx context.Context, store Store, source semanticview.Source, key string) (Descriptor, error) {
	key = strings.TrimSpace(key)
	if key == "" {
		return Descriptor{}, fmt.Errorf("credential key is required")
	}
	meta, err := inspectStore(ctx, store, key)
	if err != nil {
		return Descriptor{}, err
	}
	index := BuildRequirementIndex(source)
	return Descriptor{
		Key:        meta.Key,
		Present:    meta.Present,
		Source:     meta.Source,
		Writable:   meta.Writable,
		UpdatedAt:  meta.UpdatedAt,
		RequiredBy: append([]Requirement{}, index[key]...),
	}, nil
}

func ListDescriptors(ctx context.Context, store Store, source semanticview.Source) ([]Descriptor, error) {
	keys := make([]string, 0)
	seen := map[string]struct{}{}
	index := BuildRequirementIndex(source)
	for key := range index {
		seen[key] = struct{}{}
		keys = append(keys, key)
	}
	if store != nil {
		storeKeys, err := store.List(ctx)
		if err != nil {
			return nil, err
		}
		for _, key := range storeKeys {
			key = strings.TrimSpace(key)
			if key == "" {
				continue
			}
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	out := make([]Descriptor, 0, len(keys))
	for _, key := range keys {
		desc, err := Describe(ctx, store, source, key)
		if err != nil {
			return nil, err
		}
		out = append(out, desc)
	}
	return out, nil
}

func MissingRequired(ctx context.Context, store Store, source semanticview.Source) ([]Descriptor, error) {
	descriptors, err := ListDescriptors(ctx, store, source)
	if err != nil {
		return nil, err
	}
	out := make([]Descriptor, 0)
	for _, desc := range descriptors {
		if desc.Present || len(desc.RequiredBy) == 0 {
			continue
		}
		out = append(out, desc)
	}
	return out, nil
}

func inspectStore(ctx context.Context, store Store, key string) (Metadata, error) {
	if inspector, ok := store.(Inspector); ok && inspector != nil {
		meta, err := inspector.Inspect(ctx, key)
		if err != nil {
			return Metadata{}, err
		}
		if strings.TrimSpace(meta.Key) == "" {
			meta.Key = key
		}
		return meta, nil
	}
	if store == nil {
		return Metadata{Key: key}, nil
	}
	_, present, err := store.Get(ctx, key)
	if err != nil {
		return Metadata{}, err
	}
	return Metadata{Key: key, Present: present}, nil
}

func normalizeRequirements(items []Requirement) []Requirement {
	if len(items) == 0 {
		return nil
	}
	out := make([]Requirement, 0, len(items))
	seen := map[string]struct{}{}
	for _, item := range items {
		item.Kind = strings.TrimSpace(item.Kind)
		item.Name = strings.TrimSpace(item.Name)
		if item.Kind == "" || item.Name == "" {
			continue
		}
		key := item.Kind + "\x00" + item.Name
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, item)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Kind == out[j].Kind {
			return out[i].Name < out[j].Name
		}
		return out[i].Kind < out[j].Kind
	})
	return out
}

func parseMCPServerCredentialKeys(value any) map[string]string {
	root, ok := anyMap(value)
	if !ok {
		return nil
	}
	out := make(map[string]string, len(root))
	for name, raw := range root {
		server, ok := anyMap(raw)
		if !ok {
			continue
		}
		if key := strings.TrimSpace(anyString(server["credentials_key"])); key != "" {
			out[strings.TrimSpace(name)] = key
		}
	}
	return out
}

func parseWebSearchProviderCredentialKeys(value any) map[string]string {
	root, ok := anyMap(value)
	if !ok {
		return nil
	}
	key := strings.TrimSpace(anyString(root["credentials_key"]))
	if key == "" {
		return nil
	}
	provider := strings.TrimSpace(anyString(root["provider"]))
	if provider == "" {
		provider = "default"
	}
	return map[string]string{provider: key}
}

func anyMap(value any) (map[string]any, bool) {
	switch typed := value.(type) {
	case map[string]any:
		return typed, true
	case map[any]any:
		out := make(map[string]any, len(typed))
		for key, item := range typed {
			out[strings.TrimSpace(anyString(key))] = item
		}
		return out, true
	default:
		return nil, false
	}
}

func anyString(value any) string {
	switch typed := value.(type) {
	case nil:
		return ""
	case string:
		return typed
	default:
		raw, err := json.Marshal(typed)
		if err != nil {
			return ""
		}
		return string(raw)
	}
}
