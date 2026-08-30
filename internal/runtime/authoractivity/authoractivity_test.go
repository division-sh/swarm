package authoractivity

import (
	"bytes"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/google/uuid"
)

func TestRegistryAcceptsOnlyDeclaredKindsTransitionsAndProjectionFields(t *testing.T) {
	if len(Kinds()) != len(kindContracts) {
		t.Fatalf("Kinds() count = %d, registry count = %d", len(Kinds()), len(kindContracts))
	}
	now := time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC)
	for kind, contract := range kindContracts {
		for transition := range contract.Transitions {
			draft := testDraft(kind, transition, now)
			if failureRequired(kind, transition) {
				draft.Failure = testFailure(t)
			}
			if err := ValidateDraft(draft); err != nil {
				t.Fatalf("ValidateDraft(%s/%s): %v", kind, transition, err)
			}
		}
	}
	unknown := testDraft(KindEventEmitted, "guessed", now)
	if err := ValidateDraft(unknown); err == nil || !strings.Contains(err.Error(), "not registered") {
		t.Fatalf("unknown transition error = %v", err)
	}
	unsafe := testDraft(KindInboundReceived, "received", now)
	unsafe.Projection.ToolName = "must-not-cross-kind-boundary"
	if err := ValidateDraft(unsafe); err == nil || !strings.Contains(err.Error(), "projection field") {
		t.Fatalf("cross-kind projection error = %v", err)
	}
	missingFailure := testDraft(KindDeliveryLifecycle, "failed", now)
	if err := ValidateDraft(missingFailure); err == nil || !strings.Contains(err.Error(), "requires canonical failure") {
		t.Fatalf("missing failure error = %v", err)
	}
	invalidOptionalFailure := testDraft(KindAgentLifecycle, "failed", now)
	invalidOptionalFailure.Failure = &runtimefailures.Envelope{}
	if err := ValidateDraft(invalidOptionalFailure); err == nil || !strings.Contains(err.Error(), "failure") {
		t.Fatalf("invalid optional failure error = %v", err)
	}
}

func TestKindContractsRejectUnsafeEventAndEffectSubjectFields(t *testing.T) {
	now := time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name   string
		draft  Draft
		remove func(*Projection)
	}{
		{
			name: "event internal identity", draft: testDraft(KindEventEmitted, "emitted", now),
			remove: func(projection *Projection) { projection.ProducerID = "" },
		},
		{
			name: "effect internal identity", draft: testDraft(KindEffectLifecycle, "launched", now),
			remove: func(projection *Projection) { projection.Adapter = "" },
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			unsafe := tt.draft
			unsafe.Projection.SubjectType = "internal"
			unsafe.Projection.SubjectID = "raw-identity"
			if err := ValidateDraft(unsafe); err == nil || !strings.Contains(err.Error(), "projection field") {
				t.Fatalf("unsafe subject error = %v", err)
			}
			missing := tt.draft
			tt.remove(&missing.Projection)
			if err := ValidateDraft(missing); err == nil || !strings.Contains(err.Error(), "is required") {
				t.Fatalf("missing author-safe subject error = %v", err)
			}
		})
	}
}

func TestKindContractsRejectWrongSourceOwnerAndSubjectType(t *testing.T) {
	now := time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC)
	wrongOwner := testDraft(KindEffectLifecycle, "launched", now)
	wrongOwner.SourceOwner = "events"
	if err := ValidateDraft(wrongOwner); err == nil || !strings.Contains(err.Error(), `expected "runtime_external_effect_attempts"`) {
		t.Fatalf("wrong source owner error = %v", err)
	}

	wrongSubject := testDraft(KindDeliveryLifecycle, "delivered", now)
	wrongSubject.Projection.SubjectType = "delivery"
	if err := ValidateDraft(wrongSubject); err == nil || !strings.Contains(err.Error(), "subject_type") {
		t.Fatalf("wrong subject type error = %v", err)
	}
}

func TestRenderModesKeepHumanProseSeparateFromTypedNDJSON(t *testing.T) {
	now := time.Date(2026, 7, 14, 12, 34, 56, 0, time.UTC)
	failure := testFailure(t)
	occurrences := []Occurrence{{
		OccurrenceID: uuid.NewString(), Sequence: 1, Kind: KindDeliveryLifecycle, Version: Version,
		Transition: "failed", SourceOwner: "event_deliveries", SourceIdentity: "delivery-a", DedupKey: "delivery-a:failed",
		OccurredAt: now, RunID: "11111111-1111-1111-1111-111111111111",
		Scope:      BundleScope("22222222-2222-2222-2222-222222222222", "bundle-v2:sha256:"+strings.Repeat("a", 64)),
		Projection: Projection{SubjectType: "agent", SubjectID: "normalizer", EventType: "message.normalized"}, Failure: failure,
	}}
	var plain bytes.Buffer
	if err := Render(&plain, occurrences, RenderOptions{Mode: RenderPlain, Width: 120}); err != nil {
		t.Fatal(err)
	}
	plainText := plain.String()
	for _, forbidden := range []string{"\x1b[", failure.Message, failure.Remediation, "provider-secret"} {
		if strings.Contains(plainText, forbidden) {
			t.Fatalf("plain output leaked %q: %s", forbidden, plainText)
		}
	}
	if !strings.Contains(plainText, "normalizer ✗ failed — internal error") || !strings.Contains(plainText, "swarm logs --run 11111111-1111-1111-1111-111111111111 --level error") {
		t.Fatalf("plain output = %q", plainText)
	}

	var ndjson bytes.Buffer
	if err := Render(&ndjson, occurrences, RenderOptions{Mode: RenderNDJSON}); err != nil {
		t.Fatal(err)
	}
	var decoded Occurrence
	if err := json.Unmarshal(bytes.TrimSpace(ndjson.Bytes()), &decoded); err != nil {
		t.Fatalf("NDJSON is not typed occurrence JSON: %v\n%s", err, ndjson.String())
	}
	if !reflect.DeepEqual(decoded, occurrences[0]) {
		t.Fatalf("NDJSON occurrence = %#v, want %#v", decoded, occurrences[0])
	}
}

func TestAgentLifecycleFailureWithoutEnvelopeStillRendersDiagnosticRoute(t *testing.T) {
	now := time.Date(2026, 7, 14, 12, 34, 56, 0, time.UTC)
	occurrence := Occurrence{
		OccurrenceID: uuid.NewString(), Sequence: 1, Kind: KindAgentLifecycle, Version: Version,
		Transition: "failed", SourceOwner: "agent_lifecycle_transition_facts", SourceIdentity: "transition-a",
		DedupKey: "agent-transition:transition-a", OccurredAt: now, RunID: "11111111-1111-1111-1111-111111111111",
		Scope:   BundleScope("22222222-2222-2222-2222-222222222222", "bundle-v2:sha256:"+strings.Repeat("a", 64)),
		AgentID: "normalizer", Projection: Projection{SubjectType: "agent", SubjectID: "normalizer", NextPhase: "failed"},
	}
	var plain bytes.Buffer
	if err := Render(&plain, []Occurrence{occurrence}, RenderOptions{Mode: RenderPlain, Width: 120}); err != nil {
		t.Fatal(err)
	}
	text := plain.String()
	if !strings.Contains(text, "normalizer ✗ failed") || !strings.Contains(text, "swarm logs --run 11111111-1111-1111-1111-111111111111 --level error") {
		t.Fatalf("agent failure output = %q", text)
	}
}

func testDraft(kind Kind, transition string, at time.Time) Draft {
	identity := string(kind) + ":" + transition
	contract, ok := kindContracts[kind]
	if !ok {
		return Draft{Kind: kind, Transition: transition, OccurredAt: at}
	}
	projection := Projection{}
	switch contract.SubjectStrategy {
	case subjectTypedIdentity:
		subjectTypes := sortedSet(contract.SubjectTypes)
		projection.SubjectType = subjectTypes[0]
		projection.SubjectID = "subject-a"
		if kind == KindActivityLifecycle {
			projection.ExecutionMode = "live"
		}
	case subjectProducer:
		projection.EventType = "message.normalized"
		projection.ProducerType = "agent"
		projection.ProducerID = "normalizer"
	case subjectAdapter:
		projection.Adapter = "anthropic_api"
		projection.Transport = "https"
		projection.AuthorityKind = "normal_agent"
		projection.AuthorityID = "normalizer"
	}
	return Draft{
		OccurrenceID: uuid.NewString(), Kind: kind, Version: Version, Transition: transition,
		SourceOwner: contract.SourceOwner, SourceIdentity: identity, DedupKey: identity, OccurredAt: at,
		Scope: testScopeForContract(contract, transition), Projection: projection,
	}
}

func testScopeForContract(contract kindContract, transition string) Scope {
	switch contract.ScopeByTransition[transition] {
	case ScopeRuntime:
		return RuntimeScope("22222222-2222-2222-2222-222222222222")
	case ScopeGlobal:
		return Scope{Kind: ScopeGlobal}
	default:
		return BundleScope("22222222-2222-2222-2222-222222222222", "bundle-v2:sha256:"+strings.Repeat("a", 64))
	}
}

func testFailure(t *testing.T) *runtimefailures.Envelope {
	t.Helper()
	err := runtimefailures.New(runtimefailures.ClassConnectorFailure, "provider_unavailable", "test", "author_activity", nil)
	failure, ok := runtimefailures.EnvelopeFromError(err)
	if !ok {
		t.Fatalf("canonical failure construction returned %T", err)
	}
	return &failure
}
