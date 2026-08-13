package bundlecatalog

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"

	bundlecatalogcontract "github.com/division-sh/swarm/internal/bundlecatalog"
)

const (
	bundleCatalogAgentProjectionVersion = "swarm.bundle.catalog.v2"
	bundleCatalogAgentCursorVersion     = "swarm.bundle.agents.cursor.v1"
)

type bundleCatalogAgentCursor struct {
	Version        string `json:"version"`
	BundleHash     string `json:"bundle_hash"`
	AgentNameOwner string `json:"agent_name_owner"`
}

func pageBundleCatalogAgents(bundleHash string, parsed map[string]any, opts bundlecatalogcontract.AgentListOptions) (bundlecatalogcontract.AgentsResult, error) {
	agents, err := projectBundleCatalogAgents(parsed)
	if err != nil {
		return bundlecatalogcontract.AgentsResult{}, err
	}
	opts = defaultBundleCatalogAgentListOptions(opts)
	start := 0
	if opts.Cursor != "" {
		owner, err := decodeBundleCatalogAgentCursor(opts.Cursor, bundleHash)
		if err != nil {
			return bundlecatalogcontract.AgentsResult{}, err
		}
		index := sort.Search(len(agents), func(i int) bool {
			return agents[i].AgentNameOwner >= owner
		})
		if index >= len(agents) || agents[index].AgentNameOwner != owner {
			return bundlecatalogcontract.AgentsResult{}, bundlecatalogcontract.ErrInvalidCursor
		}
		start = index + 1
	}

	result := bundlecatalogcontract.AgentsResult{Agents: []bundlecatalogcontract.AgentDefinition{}}
	for index := start; index < len(agents) && len(result.Agents) < opts.Limit; index++ {
		candidateAgents := append(append([]bundlecatalogcontract.AgentDefinition{}, result.Agents...), agents[index])
		candidate := bundlecatalogcontract.AgentsResult{Agents: candidateAgents}
		if index+1 < len(agents) {
			candidate.NextCursor = encodeBundleCatalogAgentCursor(bundleHash, agents[index].AgentNameOwner)
		}
		encoded, err := json.Marshal(candidate)
		if err != nil {
			return bundlecatalogcontract.AgentsResult{}, fmt.Errorf("encode bundle agents page: %w", err)
		}
		if len(encoded) > bundlecatalogcontract.AgentListResultByteCeiling {
			if len(result.Agents) == 0 {
				row, _ := json.Marshal(agents[index])
				return bundlecatalogcontract.AgentsResult{}, &bundlecatalogcontract.AgentDefinitionTooLargeError{
					BundleHash:        strings.TrimSpace(bundleHash),
					AgentNameOwner:    agents[index].AgentNameOwner,
					AgentID:           agents[index].AgentID,
					EncodedRowBytes:   len(row),
					ResultByteCeiling: bundlecatalogcontract.AgentListResultByteCeiling,
				}
			}
			result.NextCursor = encodeBundleCatalogAgentCursor(bundleHash, result.Agents[len(result.Agents)-1].AgentNameOwner)
			return result, nil
		}
		result = candidate
	}
	if start+len(result.Agents) < len(agents) && len(result.Agents) > 0 {
		result.NextCursor = encodeBundleCatalogAgentCursor(bundleHash, result.Agents[len(result.Agents)-1].AgentNameOwner)
	}
	return result, nil
}

func defaultBundleCatalogAgentListOptions(opts bundlecatalogcontract.AgentListOptions) bundlecatalogcontract.AgentListOptions {
	opts.Cursor = strings.TrimSpace(opts.Cursor)
	if opts.Limit <= 0 {
		opts.Limit = bundlecatalogcontract.DefaultAgentListLimit
	}
	if opts.Limit > bundlecatalogcontract.MaxAgentListLimit {
		opts.Limit = bundlecatalogcontract.MaxAgentListLimit
	}
	return opts
}

func encodeBundleCatalogAgentCursor(bundleHash, owner string) string {
	raw, _ := json.Marshal(bundleCatalogAgentCursor{
		Version:        bundleCatalogAgentCursorVersion,
		BundleHash:     strings.TrimSpace(bundleHash),
		AgentNameOwner: strings.TrimSpace(owner),
	})
	return base64.RawURLEncoding.EncodeToString(raw)
}

func decodeBundleCatalogAgentCursor(cursor, bundleHash string) (string, error) {
	raw, err := base64.RawURLEncoding.DecodeString(strings.TrimSpace(cursor))
	if err != nil {
		return "", bundlecatalogcontract.ErrInvalidCursor
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var decoded bundleCatalogAgentCursor
	if err := decoder.Decode(&decoded); err != nil {
		return "", bundlecatalogcontract.ErrInvalidCursor
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return "", bundlecatalogcontract.ErrInvalidCursor
	}
	owner := strings.TrimSpace(decoded.AgentNameOwner)
	if strings.TrimSpace(decoded.Version) != bundleCatalogAgentCursorVersion ||
		strings.TrimSpace(decoded.BundleHash) != strings.TrimSpace(bundleHash) || owner == "" {
		return "", bundlecatalogcontract.ErrInvalidCursor
	}
	return owner, nil
}

func projectBundleCatalogAgents(parsed map[string]any) ([]bundlecatalogcontract.AgentDefinition, error) {
	version, ok := parsed["projection_version"].(string)
	if !ok || strings.TrimSpace(version) != bundleCatalogAgentProjectionVersion {
		return nil, fmt.Errorf("bundle catalog agents projection failed: projection_version must be %q", bundleCatalogAgentProjectionVersion)
	}
	raw, ok := parsed["agents"]
	if !ok {
		return nil, fmt.Errorf("bundle catalog agents projection failed: agents is required")
	}
	items, ok := raw.([]any)
	if !ok {
		return nil, fmt.Errorf("bundle catalog agents projection failed: agents must be an array")
	}
	out := make([]bundlecatalogcontract.AgentDefinition, 0, len(items))
	owners := make(map[string]struct{}, len(items))
	for index, item := range items {
		definition, ok := item.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("bundle catalog agents projection failed: agents[%d] must be an object", index)
		}
		agent, err := projectBundleCatalogAgentDefinition("", "", definition)
		if err != nil {
			return nil, err
		}
		if _, exists := owners[agent.AgentNameOwner]; exists {
			return nil, fmt.Errorf("bundle catalog agents projection failed: duplicate agent_name_owner %q", agent.AgentNameOwner)
		}
		owners[agent.AgentNameOwner] = struct{}{}
		out = append(out, agent)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].AgentNameOwner < out[j].AgentNameOwner })
	return out, nil
}
