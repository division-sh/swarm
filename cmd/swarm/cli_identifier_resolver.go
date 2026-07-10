package main

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
)

const cliIdentifierCandidateLimit = 5

var cliBundleDigestPrefixPattern = regexp.MustCompile(`^[a-fA-F0-9]{1,64}$`)

type cliIdentifierCandidate struct {
	ID           string
	Status       string
	StatusFamily cliHumanCodeFamily
	CreatedAt    string
}

type cliIdentifierResolveRequest struct {
	Command  string
	Selector string
	Value    string
	Scope    map[string]string
}

func resolveCLIIdentifier(ctx context.Context, client *cliAPIClient, request cliIdentifierResolveRequest) (string, error) {
	if err := validateCLIIdentifierRegistry(); err != nil {
		return "", err
	}
	row, ok := cliIdentifierRegistration(request.Command, request.Selector)
	if !ok {
		return "", fmt.Errorf("identifier input %s %s is not registered", request.Command, request.Selector)
	}
	if row.Mode != cliIdentifierModeResolverBounded && row.Mode != cliIdentifierModeResolverScoped {
		return "", fmt.Errorf("identifier input %s %s is not resolver-backed", request.Command, request.Selector)
	}
	policy, ok := cliIdentifierFamilyPolicyFor(row.Family)
	if !ok {
		return "", fmt.Errorf("identifier family %q is not registered", row.Family)
	}
	value := strings.TrimSpace(request.Value)
	if value == "" {
		return "", &cliAPIValidationError{message: fmt.Sprintf("ERROR: %s is required.", cliIdentifierSelectorName(request.Selector))}
	}
	if policy.NormalizationMode == cliIdentifierNormalizeBundleDigest {
		if exact, err := validateBundleHashArg("bundle hash", value); err == nil {
			return exact, nil
		}
		if err := validateBundleIdentifierPrefix(value); err != nil {
			return "", err
		}
	}
	if client == nil {
		return "", fmt.Errorf("identifier resolver requires an API client")
	}

	candidates, err := cliIdentifierCandidates(ctx, client, policy, request.Scope)
	if err != nil {
		return "", err
	}
	exact, matches, err := matchCLIIdentifierCandidates(policy, value, candidates)
	if err != nil {
		return "", err
	}
	if exact != "" {
		return exact, nil
	}
	switch len(matches) {
	case 0:
		return "", newCLIIdentifierNoMatchError(row, value)
	case 1:
		return matches[0].ID, nil
	default:
		return "", newCLIIdentifierAmbiguousError(row, value, matches)
	}
}

func resolveCLIIdentifierAfterNotFound(ctx context.Context, client *cliAPIClient, request cliIdentifierResolveRequest, err error, notFoundCodes ...string) (string, error) {
	if !cliIdentifierHasApplicationCode(err, notFoundCodes...) {
		return "", err
	}
	return resolveCLIIdentifier(ctx, client, request)
}

func cliIdentifierHasApplicationCode(err error, codes ...string) bool {
	var rpcErr *jsonRPCError
	if !errors.As(err, &rpcErr) {
		return false
	}
	code := applicationErrorCode(rpcErr.Data)
	for _, candidate := range codes {
		if code == candidate {
			return true
		}
	}
	return false
}

func validateBundleIdentifierPrefix(value string) error {
	value = strings.TrimSpace(value)
	lower := strings.ToLower(value)
	switch {
	case strings.HasPrefix(lower, "bundle-v1:sha256:"):
		value = value[len("bundle-v1:sha256:"):]
	case strings.HasPrefix(lower, "sha256:"):
		value = value[len("sha256:"):]
	}
	if !cliBundleDigestPrefixPattern.MatchString(value) {
		return &cliAPIValidationError{message: "ERROR: bundle hash must be a canonical bundle hash or a hexadecimal digest prefix.\n  List bundle hashes with `swarm bundle list`."}
	}
	return nil
}

func validateBundleIdentifierInput(value string) error {
	value = strings.TrimSpace(value)
	if _, err := validateBundleHashArg("bundle hash", value); err == nil {
		return nil
	}
	return validateBundleIdentifierPrefix(value)
}

func cliIdentifierCandidates(ctx context.Context, client *cliAPIClient, policy cliIdentifierFamilyPolicy, scope map[string]string) ([]cliIdentifierCandidate, error) {
	runID := ""
	switch policy.ScopeMode {
	case cliIdentifierScopeGlobalBounded, cliIdentifierScopeBoundedCatalog:
	case cliIdentifierScopeFullRunRequired:
		runID = strings.TrimSpace(scope["run_id"])
		if runID == "" {
			return nil, &cliAPIValidationError{message: "ERROR: entity prefixes require a full run ID.\n  Pass --run-id <full-run-id>, or use the full entity ID."}
		}
	default:
		return nil, fmt.Errorf("identifier family %q scope %q is not resolver-backed", policy.Family, policy.ScopeMode)
	}

	switch policy.CandidateSource {
	case cliIdentifierSourceAgentList:
		var result agentListResult
		if err := client.call(ctx, "agent.list", map[string]any{}, &result); err != nil {
			return nil, err
		}
		if err := validateAgentListResult(result); err != nil {
			return nil, err
		}
		out := make([]cliIdentifierCandidate, 0, len(result.Agents))
		for _, agent := range result.Agents {
			out = append(out, cliIdentifierCandidate{ID: agent.AgentID, Status: agent.Status, StatusFamily: cliHumanCodeAgentStatus})
		}
		return out, nil
	case cliIdentifierSourceBundleList:
		return listAllBundleIdentifierCandidates(ctx, client)
	case cliIdentifierSourceEntityList:
		return listAllEntityIdentifierCandidates(ctx, client, runID)
	default:
		return nil, fmt.Errorf("identifier family %q candidate source %q is not resolver-backed", policy.Family, policy.CandidateSource)
	}
}

func listAllBundleIdentifierCandidates(ctx context.Context, client *cliAPIClient) ([]cliIdentifierCandidate, error) {
	return listAllCLIIdentifierCandidatePages(map[string]any{"limit": 500}, func(params map[string]any) ([]cliIdentifierCandidate, string, error) {
		var result bundleListResult
		if err := client.call(ctx, bundleListMethod, params, &result); err != nil {
			return nil, "", err
		}
		if err := validateBundleListResult(result); err != nil {
			return nil, "", err
		}
		candidates := make([]cliIdentifierCandidate, 0, len(result.Bundles))
		for _, bundle := range result.Bundles {
			candidates = append(candidates, cliIdentifierCandidate{ID: bundle.BundleHash, CreatedAt: bundle.IngestedAt})
		}
		return candidates, result.NextCursor, nil
	}, "bundle.list")
}

func listAllEntityIdentifierCandidates(ctx context.Context, client *cliAPIClient, runID string) ([]cliIdentifierCandidate, error) {
	return listAllCLIIdentifierCandidatePages(map[string]any{"run_id": runID, "limit": 500}, func(params map[string]any) ([]cliIdentifierCandidate, string, error) {
		var result entityListResult
		if err := client.call(ctx, entityListMethod, params, &result); err != nil {
			return nil, "", err
		}
		if err := validateEntityListResult(result); err != nil {
			return nil, "", err
		}
		candidates := make([]cliIdentifierCandidate, 0, len(result.Entities))
		for _, entity := range result.Entities {
			candidates = append(candidates, cliIdentifierCandidate{ID: entity.EntityID, Status: entity.CurrentState, CreatedAt: entity.CreatedAt})
		}
		return candidates, result.NextCursor, nil
	}, "entity.list")
}

func listAllCLIIdentifierCandidatePages(
	params map[string]any,
	listPage func(map[string]any) ([]cliIdentifierCandidate, string, error),
	method string,
) ([]cliIdentifierCandidate, error) {
	seen := map[string]struct{}{}
	out := []cliIdentifierCandidate{}
	for {
		candidates, nextCursor, err := listPage(params)
		if err != nil {
			return nil, err
		}
		out = append(out, candidates...)
		next := strings.TrimSpace(nextCursor)
		if next == "" {
			return out, nil
		}
		if _, ok := seen[next]; ok {
			return nil, fmt.Errorf("malformed %s result: repeated next_cursor %q", method, next)
		}
		seen[next] = struct{}{}
		params["cursor"] = next
	}
}

func matchCLIIdentifierCandidates(policy cliIdentifierFamilyPolicy, value string, candidates []cliIdentifierCandidate) (string, []cliIdentifierCandidate, error) {
	normalized, err := normalizeCLIIdentifierLookup(policy, value)
	if err != nil {
		return "", nil, err
	}
	matches := make([]cliIdentifierCandidate, 0)
	for _, candidate := range candidates {
		candidateValue, err := normalizeCLIIdentifierLookup(policy, candidate.ID)
		if err != nil {
			return "", nil, err
		}
		if normalized == candidateValue {
			return candidate.ID, nil, nil
		}
		if policy.NormalizationMode == cliIdentifierNormalizeBundleDigest {
			digest := strings.TrimPrefix(candidateValue, "bundle-v1:sha256:")
			if strings.HasPrefix(candidateValue, normalized) || strings.HasPrefix(digest, normalized) {
				matches = append(matches, candidate)
			}
			continue
		}
		if strings.HasPrefix(candidateValue, normalized) {
			matches = append(matches, candidate)
		}
	}
	sort.Slice(matches, func(i, j int) bool { return matches[i].ID < matches[j].ID })
	return "", matches, nil
}

func normalizeCLIIdentifierLookup(policy cliIdentifierFamilyPolicy, value string) (string, error) {
	value = strings.TrimSpace(value)
	switch policy.NormalizationMode {
	case cliIdentifierNormalizeCaseSensitive, cliIdentifierNormalizeFlowPath:
		return value, nil
	case cliIdentifierNormalizeBundleDigest:
		value = strings.ToLower(value)
		return strings.TrimPrefix(value, "sha256:"), nil
	default:
		return "", fmt.Errorf("identifier family %q has unsupported normalization mode %q", policy.Family, policy.NormalizationMode)
	}
}

func newCLIIdentifierNoMatchError(row cliIdentifierInputRegistration, value string) error {
	return &cliAPIValidationError{message: fmt.Sprintf(
		"ERROR: no %s matches %q.\n  Use %s to discover the full identifier.",
		cliIdentifierFamilyLabel(row.Family), value, cliIdentifierDiscoveryCommand(row.Family),
	)}
}

func newCLIIdentifierAmbiguousError(row cliIdentifierInputRegistration, value string, matches []cliIdentifierCandidate) error {
	lines := []string{fmt.Sprintf("ERROR: %s prefix %q is ambiguous.", cliIdentifierFamilyLabel(row.Family), value), "  Candidates:"}
	limit := len(matches)
	if limit > cliIdentifierCandidateLimit {
		limit = cliIdentifierCandidateLimit
	}
	for _, candidate := range matches[:limit] {
		parts := []string{"    " + candidate.ID}
		if status := strings.TrimSpace(candidate.Status); status != "" {
			parts = append(parts, "status="+formatCLIHumanCode(candidate.StatusFamily, candidate.Status))
		}
		if createdAt := strings.TrimSpace(candidate.CreatedAt); createdAt != "" {
			parts = append(parts, "created="+createdAt)
		}
		lines = append(lines, strings.Join(parts, "  "))
	}
	if len(matches) > limit {
		lines = append(lines, fmt.Sprintf("    ... and %d more", len(matches)-limit))
	}
	lines = append(lines, "  Use a longer prefix or the full identifier.")
	return &cliAPIValidationError{message: strings.Join(lines, "\n")}
}

func cliIdentifierFamilyLabel(family cliIdentifierFamily) string {
	switch family {
	case cliIdentifierFamilyAgent:
		return "agent ID"
	case cliIdentifierFamilyBundle:
		return "bundle hash"
	case cliIdentifierFamilyEntity:
		return "entity ID"
	default:
		return string(family) + " ID"
	}
}

func cliIdentifierDiscoveryCommand(family cliIdentifierFamily) string {
	switch family {
	case cliIdentifierFamilyAgent:
		return "`swarm agent list`"
	case cliIdentifierFamilyBundle:
		return "`swarm bundle list`"
	case cliIdentifierFamilyEntity:
		return "`swarm entity list --run-id <full-run-id>`"
	default:
		return "the corresponding list command"
	}
}

func cliIdentifierSelectorName(selector string) string {
	selector = strings.TrimSpace(selector)
	selector = strings.TrimPrefix(selector, "arg:")
	selector = strings.TrimPrefix(selector, "flag:")
	return strings.ReplaceAll(selector, "-", " ")
}
