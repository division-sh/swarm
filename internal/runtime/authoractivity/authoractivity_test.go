package authoractivity

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/google/uuid"
	_ "modernc.org/sqlite"
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

func TestSQLiteMutationOwnsRollbackReplayNestedBatchingAndPagination(t *testing.T) {
	db := openAuthorActivitySQLite(t)
	ctx := context.Background()
	now := time.Date(2026, 7, 14, 12, 0, 0, 123000000, time.UTC)

	if err := Require(ctx); err == nil {
		t.Fatal("raw producer context was accepted")
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	story, err := Begin(ctx, tx, DialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	if nested, err := Begin(story, tx, DialectSQLite); err != nil || nested != story {
		t.Fatalf("nested Begin = (%v, %v), want exact joined context", nested, err)
	}
	rolledBack := testDraft(KindInboundReceived, "received", now)
	rolledBack.DedupKey = "rollback"
	if err := Record(story, rolledBack); err != nil {
		t.Fatal(err)
	}
	if err := Finalize(story); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}

	first := testDraft(KindInboundReceived, "received", now.Add(time.Second))
	first.DedupKey = "first"
	first.Projection = Projection{SubjectType: "entity", SubjectID: "entity-a", Provider: "telegram"}
	second := testDraft(KindEventEmitted, "emitted", now.Add(2*time.Second))
	second.DedupKey = "second"
	second.Projection = Projection{EventType: "message.normalized", ProducerType: "agent", ProducerID: "normalizer"}
	commitDrafts(t, db, DialectSQLite, first, first, second)
	replayTx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	replayStory, err := Begin(ctx, replayTx, DialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	if occurredAt, found, err := PersistedOccurredAt(replayStory, first.DedupKey); err != nil || !found || !occurredAt.Equal(first.OccurredAt) {
		t.Fatalf("PersistedOccurredAt = (%v, %v, %v), want %v", occurredAt, found, err, first.OccurredAt)
	}
	if _, found, err := PersistedOccurredAt(replayStory, "missing"); err != nil || found {
		t.Fatalf("missing PersistedOccurredAt = (%v, %v), want not found", found, err)
	}
	if err := replayTx.Rollback(); err != nil {
		t.Fatal(err)
	}

	listed, err := List(ctx, db, DialectSQLite, ListOptions{Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	if len(listed.Occurrences) != 1 || listed.Occurrences[0].Sequence != 1 || listed.NextCursor != 1 {
		t.Fatalf("first page = %#v", listed)
	}
	listed, err = List(ctx, db, DialectSQLite, ListOptions{AfterSequence: listed.NextCursor, Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(listed.Occurrences) != 1 || listed.Occurrences[0].Sequence != 2 {
		t.Fatalf("second page = %#v", listed)
	}

	// An identical persisted replay is a no-op and does not consume sequence 3.
	commitDrafts(t, db, DialectSQLite, first)
	third := testDraft(KindCardLifecycle, "created", now.Add(3*time.Second))
	third.DedupKey = "third"
	commitDrafts(t, db, DialectSQLite, third)
	listed, err = List(ctx, db, DialectSQLite, ListOptions{AfterSequence: 2, Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(listed.Occurrences) != 1 || listed.Occurrences[0].Sequence != 3 {
		t.Fatalf("post-replay page = %#v", listed)
	}
}

func TestSQLiteMutationRejectsConflictingReplayAtomically(t *testing.T) {
	db := openAuthorActivitySQLite(t)
	now := time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC)
	created := testDraft(KindEntityLifecycle, "created", now)
	created.DedupKey = "entity-transition"
	created.Projection = Projection{SubjectType: "entity", SubjectID: "entity-a", NewState: "new"}
	commitDrafts(t, db, DialectSQLite, created)

	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	story, err := Begin(context.Background(), tx, DialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	conflict := created
	conflict.Transition = "stage_changed"
	conflict.Projection = Projection{SubjectType: "entity", SubjectID: "entity-a", OldState: "new", NewState: "active"}
	if _, err := tx.ExecContext(story, `CREATE TABLE source_effect (value TEXT NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(story, `INSERT INTO source_effect (value) VALUES ('must-rollback')`); err != nil {
		t.Fatal(err)
	}
	if err := Record(story, conflict); err != nil {
		t.Fatal(err)
	}
	if err := Finalize(story); err == nil || !strings.Contains(err.Error(), "conflicting persisted replay") {
		t.Fatalf("Finalize conflict error = %v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	var exists int
	if err := db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='source_effect'`).Scan(&exists); err != nil {
		t.Fatal(err)
	}
	if exists != 0 {
		t.Fatal("source mutation survived conflicting story rollback")
	}
}

func TestRenderModesKeepHumanProseSeparateFromTypedNDJSON(t *testing.T) {
	now := time.Date(2026, 7, 14, 12, 34, 56, 0, time.UTC)
	failure := testFailure(t)
	occurrences := []Occurrence{{
		OccurrenceID: uuid.NewString(), Sequence: 1, Kind: KindDeliveryLifecycle, Version: Version,
		Transition: "failed", SourceOwner: "event_deliveries", SourceIdentity: "delivery-a", DedupKey: "delivery-a:failed",
		OccurredAt: now, RunID: "11111111-1111-1111-1111-111111111111",
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
	if !strings.Contains(plainText, "agent[normalizer]  failed, retrying") || !strings.Contains(plainText, "swarm logs --run 11111111-1111-1111-1111-111111111111 --level error") {
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
		AgentID: "normalizer", Projection: Projection{SubjectType: "agent", SubjectID: "normalizer", NextPhase: "failed"},
	}
	var plain bytes.Buffer
	if err := Render(&plain, []Occurrence{occurrence}, RenderOptions{Mode: RenderPlain, Width: 120}); err != nil {
		t.Fatal(err)
	}
	text := plain.String()
	if !strings.Contains(text, "agent[normalizer]  failed") || !strings.Contains(text, "swarm logs --run 11111111-1111-1111-1111-111111111111 --level error") {
		t.Fatalf("agent failure output = %q", text)
	}
}

func openAuthorActivitySQLite(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", "file:"+uuid.NewString()+"?mode=memory&cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	for _, ddl := range []string{
		`CREATE TABLE author_activity_order (singleton_id INTEGER PRIMARY KEY CHECK (singleton_id = 1), last_sequence BIGINT NOT NULL CHECK (last_sequence >= 0))`,
		`CREATE TABLE author_activity_occurrences (
			occurrence_id TEXT PRIMARY KEY, sequence BIGINT NOT NULL UNIQUE CHECK (sequence > 0), kind TEXT NOT NULL,
			version INTEGER NOT NULL CHECK (version = 1), transition TEXT NOT NULL, source_owner TEXT NOT NULL,
			source_identity TEXT NOT NULL, dedup_key TEXT NOT NULL UNIQUE, run_id TEXT, entity_id TEXT, agent_id TEXT,
			flow_id TEXT, projection TEXT NOT NULL DEFAULT '{}', failure TEXT, occurred_at TIMESTAMP NOT NULL
		)`,
	} {
		if _, err := db.Exec(ddl); err != nil {
			t.Fatal(err)
		}
	}
	return db
}

func commitDrafts(t *testing.T, db *sql.DB, dialect Dialect, drafts ...Draft) {
	t.Helper()
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	story, err := Begin(context.Background(), tx, dialect)
	if err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	for _, draft := range drafts {
		if err := Record(story, draft); err != nil {
			_ = tx.Rollback()
			t.Fatal(err)
		}
	}
	if err := Finalize(story); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
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
		Projection: projection,
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
