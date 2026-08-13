package bundlecatalog

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"

	bundlecatalogcontract "github.com/division-sh/swarm/internal/bundlecatalog"
	runtimeagentintent "github.com/division-sh/swarm/internal/runtime/agentintent"
)

func TestPageBundleCatalogAgentsTraversesCanonicalOwnersExactlyOnce(t *testing.T) {
	bundleHash := "bundle-v1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	parsed := bundleAgentProjection(t,
		bundleAgentProjectionDefinition(t, "worker", "swarm://projects/right/agent/worker", "right"),
		bundleAgentProjectionDefinition(t, "worker", "swarm://flows/review/agent/worker", "flow"),
		bundleAgentProjectionDefinition(t, "worker", "swarm://flows/triage/agent/worker", "flow"),
		bundleAgentProjectionDefinition(t, "worker", "swarm://agent/worker", "root"),
		bundleAgentProjectionDefinition(t, "worker", "swarm://projects/left/agent/worker", "left"),
	)

	var got []string
	cursor := ""
	for pageNumber := 0; ; pageNumber++ {
		page, err := pageBundleCatalogAgents(bundleHash, parsed, bundlecatalogcontract.AgentListOptions{Limit: 2, Cursor: cursor})
		if err != nil {
			t.Fatalf("page %d: %v", pageNumber, err)
		}
		for _, agent := range page.Agents {
			got = append(got, agent.AgentNameOwner)
		}
		if page.NextCursor == "" {
			break
		}
		if page.NextCursor == cursor {
			t.Fatalf("page %d cursor did not advance", pageNumber)
		}
		cursor = page.NextCursor
	}
	want := []string{
		"swarm://agent/worker",
		"swarm://flows/review/agent/worker",
		"swarm://flows/triage/agent/worker",
		"swarm://projects/left/agent/worker",
		"swarm://projects/right/agent/worker",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("owners = %#v, want %#v", got, want)
	}
}

func TestPageBundleCatalogAgentsPreservesExecutionFrameStaticEvidence(t *testing.T) {
	bundleHash := "bundle-v1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	definition := bundleAgentProjectionDefinition(t, "worker", "swarm://agent/worker", "intent")
	definition["criteria"] = []any{"criteria/quality.md", "criteria/safety.md"}
	definition["provider_prompt"] = "intent\n\n  exact provider prompt  \n"

	result, err := pageBundleCatalogAgents(bundleHash, bundleAgentProjection(t, definition), bundlecatalogcontract.AgentListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Agents) != 1 {
		t.Fatalf("agents = %d, want 1", len(result.Agents))
	}
	agent := result.Agents[0]
	if !reflect.DeepEqual(agent.Criteria, []string{"criteria/quality.md", "criteria/safety.md"}) {
		t.Fatalf("criteria = %#v", agent.Criteria)
	}
	if agent.ProviderPrompt != definition["provider_prompt"] {
		t.Fatalf("provider prompt = %q, want exact %q", agent.ProviderPrompt, definition["provider_prompt"])
	}
}

func TestPageBundleCatalogAgentsRejectsInvalidProjectionAndCursorCoordinates(t *testing.T) {
	bundleHash := "bundle-v1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	otherHash := "bundle-v1:sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	definition := bundleAgentProjectionDefinition(t, "worker", "swarm://agent/worker", "intent")
	valid := bundleAgentProjection(t, definition)

	legacyRaw, _ := json.Marshal(map[string]any{"agent_id": "worker"})
	legacyCursor := base64.RawURLEncoding.EncodeToString(legacyRaw)
	for name, cursor := range map[string]string{
		"malformed":    "not-base64",
		"legacy":       legacyCursor,
		"cross_bundle": encodeBundleCatalogAgentCursor(otherHash, "swarm://agent/worker"),
		"unknown":      encodeBundleCatalogAgentCursor(bundleHash, "swarm://agent/unknown"),
	} {
		t.Run(name, func(t *testing.T) {
			result, err := pageBundleCatalogAgents(bundleHash, valid, bundlecatalogcontract.AgentListOptions{Cursor: cursor})
			if !errors.Is(err, bundlecatalogcontract.ErrInvalidCursor) || len(result.Agents) != 0 {
				t.Fatalf("result=%#v err=%v, want fail-closed invalid cursor", result, err)
			}
		})
	}

	duplicate := bundleAgentProjection(t, definition, definition)
	missingOwner := bundleAgentProjectionDefinition(t, "worker", "swarm://agent/worker", "intent")
	delete(missingOwner, "agent_name_owner")
	v1 := bundleAgentProjection(t, definition)
	v1["projection_version"] = "swarm.bundle.catalog.v1"
	for name, parsed := range map[string]map[string]any{
		"duplicate_owner": duplicate,
		"missing_owner":   bundleAgentProjection(t, missingOwner),
		"legacy_v1":       v1,
	} {
		t.Run(name, func(t *testing.T) {
			result, err := pageBundleCatalogAgents(bundleHash, parsed, bundlecatalogcontract.AgentListOptions{})
			if err == nil || len(result.Agents) != 0 {
				t.Fatalf("result=%#v err=%v, want projection rejection before rows", result, err)
			}
		})
	}
}

func TestPageBundleCatalogAgentsEnforcesCountAndExactResultByteCeilings(t *testing.T) {
	bundleHash := "bundle-v1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	definitions := make([]map[string]any, 0, bundlecatalogcontract.MaxAgentListLimit+1)
	for index := 0; index <= bundlecatalogcontract.MaxAgentListLimit; index++ {
		owner := "swarm://agent/" + strings.Repeat("0", 4-len(jsonNumber(index))) + jsonNumber(index)
		definitions = append(definitions, bundleAgentProjectionDefinition(t, "worker", owner, "intent"))
	}
	countPage, err := pageBundleCatalogAgents(bundleHash, bundleAgentProjection(t, definitions...), bundlecatalogcontract.AgentListOptions{Limit: bundlecatalogcontract.MaxAgentListLimit})
	if err != nil {
		t.Fatal(err)
	}
	if len(countPage.Agents) != bundlecatalogcontract.MaxAgentListLimit || countPage.NextCursor == "" {
		t.Fatalf("count page = %d cursor=%q", len(countPage.Agents), countPage.NextCursor)
	}

	large := bundleAgentProjection(t,
		bundleAgentProjectionDefinition(t, "first", "swarm://agent/first", strings.Repeat("a", 500_000)),
		bundleAgentProjectionDefinition(t, "second", "swarm://agent/second", strings.Repeat("b", 500_000)),
	)
	first, err := pageBundleCatalogAgents(bundleHash, large, bundlecatalogcontract.AgentListOptions{Limit: 2})
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(first)
	if err != nil {
		t.Fatal(err)
	}
	if len(first.Agents) != 1 || first.NextCursor == "" || len(encoded) > bundlecatalogcontract.AgentListResultByteCeiling {
		t.Fatalf("byte page agents=%d cursor=%q bytes=%d", len(first.Agents), first.NextCursor, len(encoded))
	}
	second, err := pageBundleCatalogAgents(bundleHash, large, bundlecatalogcontract.AgentListOptions{Limit: 2, Cursor: first.NextCursor})
	if err != nil {
		t.Fatal(err)
	}
	if len(second.Agents) != 1 || second.Agents[0].AgentID != "second" || second.NextCursor != "" {
		t.Fatalf("second byte page = %#v", second)
	}

	tooLargeProjection := bundleAgentProjection(t,
		bundleAgentProjectionDefinition(t, "oversized", "swarm://agent/oversized", strings.Repeat("x", bundlecatalogcontract.AgentListResultByteCeiling)),
	)
	result, err := pageBundleCatalogAgents(bundleHash, tooLargeProjection, bundlecatalogcontract.AgentListOptions{})
	var tooLarge *bundlecatalogcontract.AgentDefinitionTooLargeError
	if !errors.As(err, &tooLarge) || len(result.Agents) != 0 {
		t.Fatalf("result=%#v err=%v, want typed oversized failure", result, err)
	}
	if tooLarge.BundleHash != bundleHash || tooLarge.AgentNameOwner != "swarm://agent/oversized" || tooLarge.AgentID != "oversized" || tooLarge.EncodedRowBytes <= bundlecatalogcontract.AgentListResultByteCeiling {
		t.Fatalf("oversized details = %#v", tooLarge)
	}
	bounded, err := json.Marshal(tooLarge)
	if err != nil || len(bounded) > 1024 {
		t.Fatalf("oversized error bytes=%d err=%v", len(bounded), err)
	}
}

func bundleAgentProjection(t testing.TB, definitions ...map[string]any) map[string]any {
	t.Helper()
	items := make([]any, 0, len(definitions))
	for _, definition := range definitions {
		items = append(items, definition)
	}
	return map[string]any{"projection_version": bundleCatalogAgentProjectionVersion, "agents": items}
}

func bundleAgentProjectionDefinition(t testing.TB, agentID, owner, content string) map[string]any {
	t.Helper()
	intent, err := runtimeagentintent.Resolve(runtimeagentintent.SourceInline, "inline", "agents.yaml#agents."+agentID+".intent", content)
	if err != nil {
		t.Fatal(err)
	}
	return map[string]any{
		"agent_id":            agentID,
		"agent_name_owner":    owner,
		"memory":              false,
		"memory_source":       "platform_default",
		"intent_kind":         string(intent.Kind),
		"intent_source":       intent.Coordinate,
		"intent_provenance":   intent.Provenance,
		"intent_content_hash": intent.ContentHash,
		"intent_identity":     intent.Identity,
		"intent_content":      intent.Content,
	}
}

func jsonNumber(value int) string {
	raw, _ := json.Marshal(value)
	return string(raw)
}
