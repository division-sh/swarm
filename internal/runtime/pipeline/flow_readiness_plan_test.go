package pipeline

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/google/uuid"
)

func TestFlowReadinessPlanHashDecoderRejectsCorruptAuthority(t *testing.T) {
	plan := DynamicFlowRuntimeReadinessPlan{
		Version: dynamicFlowRuntimeReadinessVersion, RunID: uuid.NewString(), WorkflowVersion: "1", ExecutionMode: executionmode.Mock,
		BundleHash: "bundle-v2:sha256:" + strings.Repeat("a", 64),
		Identity: flowidentity.Instance{
			TemplateID: "child", ScopeKey: "child", InstanceID: "one", InstancePath: "child/one",
			EntityID: uuid.NewString(), HasStoredPath: true,
		},
	}
	raw, err := canonicaljson.MarshalPreservingNumberKinds(plan)
	if err != nil {
		t.Fatal(err)
	}
	hash, err := plan.Hash()
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name string
		raw  []byte
		hash string
		ok   bool
	}{
		{"exact", raw, hash, true},
		{"whitespace", append(append([]byte(" \n"), raw...), '\n'), hash, true},
		{"missing_hash", raw, "", false},
		{"different_hash", raw, "sha256:" + strings.Repeat("0", 64), false},
		{"malformed_hash", raw, strings.ToUpper(hash), false},
		{"changed_plan", bytes.Replace(raw, []byte(`"workflow_version":"1"`), []byte(`"workflow_version":"2"`), 1), hash, false},
		{"unsupported_version", bytes.Replace(raw, []byte(`"version":5`), []byte(`"version":4`), 1), hash, false},
		{"malformed_json", []byte(`{"version":5`), hash, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			decoded, err := DecodeFlowReadinessPlan(tc.raw, tc.hash)
			if (err == nil) != tc.ok {
				t.Fatalf("decoded=%+v err=%v, valid=%t", decoded, err, tc.ok)
			}
			if tc.ok {
				got, err := decoded.Hash()
				if err != nil || got != hash {
					t.Fatalf("decoded plan equality changed: hash=%s err=%v", got, err)
				}
			}
		})
	}
}

func TestFlowReadinessPlanHashPreservesCheckedNumberKinds(t *testing.T) {
	plan := DynamicFlowRuntimeReadinessPlan{
		Version: dynamicFlowRuntimeReadinessVersion, RunID: uuid.NewString(), WorkflowVersion: "1", ExecutionMode: executionmode.Mock,
		BundleHash: "bundle-v2:sha256:" + strings.Repeat("a", 64),
		Identity: flowidentity.Instance{
			TemplateID: "child", ScopeKey: "child", InstanceID: "one", InstancePath: "child/one",
			EntityID: uuid.NewString(), HasStoredPath: true,
		},
	}
	plan.CreationEvent = &DynamicFlowRuntimeCreationEventPlan{
		EventID: uuid.NewString(), EventType: "child/one/created", RunID: plan.RunID,
		ParentEventID: uuid.NewString(), ExecutionMode: executionmode.Mock,
		Payload: []byte(`{"a":7.0,"z":1}`), CreatedAt: time.Now().UTC(),
	}
	original, err := plan.Hash()
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name, payload string
		same          bool
	}{
		{"key_order", `{"z":1, "a":7.0}`, true},
		{"float_spelling", `{"a":7.00,"z":1}`, true},
		{"integer_substitution", `{"a":7,"z":1}`, false},
		{"fractional_substitution", `{"a":7.1,"z":1}`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			event := *plan.CreationEvent
			event.Payload = []byte(tc.payload)
			changed := plan
			changed.CreationEvent = &event
			hash, err := changed.Hash()
			if err != nil || (hash == original) != tc.same {
				t.Fatalf("planning equality changed checked number kind: hash=%s same=%v err=%v", hash, tc.same, err)
			}
		})
	}
}

func TestFlowReadinessPlanAgentEntityOwnershipIsExactAndHashed(t *testing.T) {
	runID, entityID := uuid.NewString(), uuid.NewString()
	name, err := agentidentity.RuntimeName("worker", "readiness-plan-test")
	if err != nil {
		t.Fatal(err)
	}
	route, err := agentidentity.PresentRoute("child", "one", "child/one")
	if err != nil {
		t.Fatal(err)
	}
	agent, err := agentidentity.New(runID, name, route)
	if err != nil {
		t.Fatal(err)
	}
	plan := DynamicFlowRuntimeReadinessPlan{
		RunID: runID, WorkflowVersion: "1", ExecutionMode: executionmode.Live,
		BundleHash: "bundle-v2:sha256:" + strings.Repeat("a", 64),
		Identity: flowidentity.Instance{
			TemplateID: "child", ScopeKey: "child", InstanceID: "one", InstancePath: "child/one",
			EntityID: entityID, HasStoredPath: true,
		},
		Agents: []DynamicFlowRuntimeAgentExpectation{{Identity: agent, ConfigRevision: strings.Repeat("a", 64)}},
	}
	var hashes []string
	for _, owner := range []string{"", entityID} {
		plan.Agents[0].EntityID = owner
		normalized, err := plan.Normalized()
		if err != nil {
			t.Fatal(err)
		}
		raw, err := canonicaljson.MarshalPreservingNumberKinds(normalized)
		if err != nil {
			t.Fatal(err)
		}
		hash, err := normalized.Hash()
		if err != nil {
			t.Fatal(err)
		}
		decoded, err := DecodeFlowReadinessPlan(raw, hash)
		if err != nil || decoded.Agents[0].EntityID != owner {
			t.Fatalf("agent ownership did not round-trip: %+v %v", decoded, err)
		}
		cloned := cloneWorkflowInstanceForEngineMutation(WorkflowInstance{RuntimeReadiness: &decoded})
		cloneHash, err := cloned.RuntimeReadiness.Hash()
		if err != nil || cloneHash != hash || cloned.RuntimeReadiness == &decoded || cloned.RuntimeReadiness.Agents[0].EntityID != owner {
			t.Fatalf("engine mutation clone changed readiness ownership/hash: %+v %v", cloned.RuntimeReadiness, err)
		}
		cloned.RuntimeReadiness.Agents[0].EntityID = uuid.NewString()
		if decoded.Agents[0].EntityID != owner {
			t.Fatal("engine mutation clone shares mutable agent ownership with its source")
		}
		erased := bytes.Replace(raw, []byte(`"entity_id":"`+owner+`",`), nil, 1)
		if bytes.Equal(raw, erased) {
			t.Fatal("agent owner erasure probe did not change the plan")
		}
		if _, err := DecodeFlowReadinessPlan(erased, canonicaljson.HashBytes(erased)); err == nil {
			t.Fatal("plan with erased explicit agent ownership was admitted")
		}
		hashes = append(hashes, hash)
	}
	if hashes[0] == hashes[1] {
		t.Fatal("entityless and entity-bearing agent plans share a hash")
	}
	for _, invalid := range []string{uuid.NewString(), " " + entityID} {
		plan.Agents[0].EntityID = invalid
		if _, err := plan.Normalized(); err == nil {
			t.Fatal("agent borrowed a foreign or noncanonical entity coordinate")
		}
	}
}

func TestFlowReadinessPlanRejectsMissingAndConflictingExecutionModes(t *testing.T) {
	plan := DynamicFlowRuntimeReadinessPlan{
		RunID: uuid.NewString(), WorkflowVersion: "1", ExecutionMode: executionmode.Live,
		BundleHash: "bundle-v2:sha256:" + strings.Repeat("a", 64),
		Identity: flowidentity.Instance{
			TemplateID: "child", ScopeKey: "child", InstanceID: "one", InstancePath: "child/one",
			EntityID: uuid.NewString(), HasStoredPath: true,
		},
	}
	plan.CreationEvent = &DynamicFlowRuntimeCreationEventPlan{
		EventID: uuid.NewString(), EventType: "child/one/created", RunID: plan.RunID,
		ParentEventID: uuid.NewString(), ExecutionMode: executionmode.Live,
		Payload: []byte(`{}`), CreatedAt: time.Now().UTC(),
	}
	if _, err := plan.Normalized(); err != nil {
		t.Fatal(err)
	}
	missing := plan
	missing.ExecutionMode = ""
	if _, err := missing.Normalized(); err == nil {
		t.Fatal("readiness without execution mode was accepted")
	}
	conflicting := plan
	creation := *plan.CreationEvent
	creation.ExecutionMode = executionmode.Mock
	conflicting.CreationEvent = &creation
	if _, err := conflicting.Normalized(); err == nil {
		t.Fatal("creation occurrence with a different execution mode was accepted")
	}
}
