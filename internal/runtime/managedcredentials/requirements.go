package managedcredentials

import (
	"context"
	"sort"
	"strings"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	managedcredentialmodel "github.com/division-sh/swarm/internal/runtime/managedcredentials/model"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

type Requirement struct {
	Kind         string
	Name         string
	Scopes       []string                                   `json:"scopes,omitempty"`
	GrantModel   string                                     `json:"grant_model,omitempty"`
	TokenRequest managedcredentialmodel.TokenRequestProfile `json:"token_request,omitempty"`
}

type RequirementDescriptor struct {
	Descriptor
	Present    bool          `json:"present"`
	RequiredBy []Requirement `json:"required_by,omitempty"`
}

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
	for key, refs := range index {
		index[key] = normalizeRequirements(refs)
	}
	return index
}

func appendToolRequirements(index map[string][]Requirement, source semanticview.Source, flowID string, entries map[string]runtimecontracts.ToolSchemaEntry) {
	for name, entry := range entries {
		name = strings.TrimSpace(name)
		if name == "" || entry.ManagedCredential == nil {
			continue
		}
		key := strings.TrimSpace(entry.ManagedCredential.Key)
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
		index[storeKey] = append(index[storeKey], Requirement{
			Kind:         "tool",
			Name:         name,
			Scopes:       append([]string{}, entry.ManagedCredential.Scopes...),
			GrantModel:   entry.ManagedCredential.GrantModel,
			TokenRequest: entry.ManagedCredential.TokenRequest,
		})
	}
}

func ListRequirementDescriptors(ctx context.Context, store Store, source semanticview.Source) ([]RequirementDescriptor, error) {
	index := BuildRequirementIndex(source)
	keys := make([]string, 0)
	seen := map[string]struct{}{}
	for key := range index {
		seen[key] = struct{}{}
		keys = append(keys, key)
	}
	if store != nil {
		descriptors, err := store.List(ctx)
		if err != nil {
			return nil, err
		}
		for _, desc := range descriptors {
			key := strings.TrimSpace(desc.Key)
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
	out := make([]RequirementDescriptor, 0, len(keys))
	for _, key := range keys {
		desc, present, err := describe(ctx, store, key)
		if err != nil {
			return nil, err
		}
		out = append(out, RequirementDescriptor{
			Descriptor: desc,
			Present:    present,
			RequiredBy: append([]Requirement{}, index[key]...),
		})
	}
	return out, nil
}

func MissingOrUnusableRequired(ctx context.Context, store Store, source semanticview.Source) ([]RequirementDescriptor, error) {
	descriptors, err := ListRequirementDescriptors(ctx, store, source)
	if err != nil {
		return nil, err
	}
	out := make([]RequirementDescriptor, 0)
	for _, desc := range descriptors {
		if len(desc.RequiredBy) == 0 {
			continue
		}
		if !desc.Present || desc.Status != StatusConnected {
			out = append(out, desc)
			continue
		}
		for _, req := range desc.RequiredBy {
			if err := managedcredentialmodel.GrantModelCovers(desc.GrantModel, req.GrantModel); err != nil {
				scoped := desc
				scoped.Status = StatusScopeInsufficient
				scoped.Failure = "grant-model-insufficient: " + err.Error()
				out = append(out, scoped)
				break
			}
			if err := managedcredentialmodel.TokenRequestProfileCovers(desc.TokenRequest, req.TokenRequest); err != nil {
				scoped := desc
				scoped.Status = StatusScopeInsufficient
				scoped.Failure = "token-request-insufficient: " + err.Error()
				out = append(out, scoped)
				break
			}
			if err := ensureScopes(desc.Scopes, req.Scopes); err != nil {
				scoped := desc
				scoped.Status = StatusScopeInsufficient
				scoped.Failure = "scope-insufficient: " + err.Error()
				out = append(out, scoped)
				break
			}
		}
	}
	return out, nil
}

func describe(ctx context.Context, store Store, key string) (Descriptor, bool, error) {
	key = strings.TrimSpace(key)
	if key == "" {
		return Descriptor{}, false, nil
	}
	if store == nil {
		return Descriptor{Key: key, Status: StatusUnconnected}, false, nil
	}
	record, ok, err := store.Get(ctx, key)
	if err != nil {
		return Descriptor{}, false, err
	}
	if !ok {
		return Descriptor{Key: key, Status: StatusUnconnected}, false, nil
	}
	return record.Descriptor(), true, nil
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
		item.Scopes = normalizeStrings(item.Scopes)
		item.GrantModel = managedcredentialmodel.NormalizeGrantModel(item.GrantModel)
		item.TokenRequest = managedcredentialmodel.NormalizeTokenRequestProfile(item.TokenRequest)
		if item.Kind == "" || item.Name == "" {
			continue
		}
		key := item.Kind + "\x00" + item.Name + "\x00" + strings.Join(item.Scopes, "\x00") + "\x00" + item.GrantModel + "\x00" + managedcredentialmodel.TokenRequestProfileSummary(item.TokenRequest)
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
