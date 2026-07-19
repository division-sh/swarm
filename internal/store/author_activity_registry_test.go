package store

import (
	"bytes"
	"context"
	"database/sql"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	decisioncard "github.com/division-sh/swarm/internal/runtime/decisioncard"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/yamlsource"
	"github.com/google/uuid"
	"gopkg.in/yaml.v3"
	_ "modernc.org/sqlite"
)

func TestAuthorActivityDecisionCardAdapterCoversRegisteredAnchors(t *testing.T) {
	stage, err := decisioncard.NewStageGateAnchor(decisioncard.StageGateAnchor{
		FlowInstance: "root/review", FlowID: "review", EntityID: uuid.NewString(), Stage: "pending", StageActivationID: uuid.NewString(),
	})
	if err != nil {
		t.Fatal(err)
	}
	human, err := decisioncard.NewHumanTaskAnchor(decisioncard.HumanTaskAnchor{
		RequesterAgentID: "reviewer", OperationID: "human-op", Category: "review",
		Scope: decisioncard.Scope{Kind: decisioncard.ScopeFlow, FlowInstance: "root/review"},
	})
	if err != nil {
		t.Fatal(err)
	}
	proposed, err := decisioncard.NewProposedEffectAnchor(decisioncard.ProposedEffectAnchor{
		RequestEventID: uuid.NewString(), ActivityID: "send_reply", Decision: "support_reply",
		Scope: decisioncard.Scope{Kind: decisioncard.ScopeEntity, FlowInstance: "root/review", EntityID: uuid.NewString()},
	})
	if err != nil {
		t.Fatal(err)
	}
	registered := map[decisioncard.AnchorKind]decisioncard.Anchor{
		decisioncard.AnchorKindStageGate: stage, decisioncard.AnchorKindHumanTask: human, decisioncard.AnchorKindProposedEffect: proposed,
	}
	if len(registered) != len(decisioncard.RegisteredAnchorKinds()) {
		t.Fatalf("author activity decision-card adapters = %d, registered anchors = %d", len(registered), len(decisioncard.RegisteredAnchorKinds()))
	}
	for _, kind := range decisioncard.RegisteredAnchorKinds() {
		anchor, ok := registered[kind]
		if !ok {
			t.Fatalf("registered decision-card anchor %q has no author activity fixture", kind)
		}
		anchorID, _, _, err := decisionCardAuthorActivityIdentity(anchor)
		if err != nil || anchorID == "" {
			t.Fatalf("decision-card anchor %q author activity identity = %q, %v", kind, anchorID, err)
		}
	}
}

func TestAuthorActivityPlatformEventDispositionCoversSpecCatalog(t *testing.T) {
	source, err := yamlsource.LoadFile(runtimecontracts.DefaultPlatformSpecFile(authorActivityRegistryRepoRoot(t)))
	if err != nil {
		t.Fatalf("load platform spec: %v", err)
	}
	var document struct {
		PlatformEvents struct {
			Catalog map[string]yaml.Node `yaml:"catalog"`
		} `yaml:"platform_events"`
	}
	if err := source.Decode(&document); err != nil {
		t.Fatalf("decode platform event catalog: %v", err)
	}
	if len(document.PlatformEvents.Catalog) == 0 {
		t.Fatal("platform event catalog is empty")
	}
	for name := range document.PlatformEvents.Catalog {
		disposition, ok := platformEventDisposition[name]
		if !ok {
			t.Errorf("platform event %q has no author activity disposition", name)
			continue
		}
		switch disposition {
		case platformDispositionRegistered, platformDispositionHandled, platformDispositionDifferent:
		default:
			t.Errorf("platform event %q has invalid author activity disposition %q", name, disposition)
		}
	}
	for name := range platformEventDisposition {
		if _, ok := document.PlatformEvents.Catalog[name]; !ok {
			t.Errorf("author activity disposition names non-catalog platform event %q", name)
		}
	}
}

func TestAuthorActivityEffectDispositionCoversRegistrations(t *testing.T) {
	registrations := runtimeeffects.Registrations()
	seen := make(map[string]struct{}, len(registrations))
	for _, registration := range registrations {
		key := string(registration.Kind) + "/" + registration.Adapter
		if _, duplicate := seen[key]; duplicate {
			t.Errorf("duplicate effect registration %q", key)
		}
		seen[key] = struct{}{}
		if _, ok := externalEffectStoryDispositions[key]; !ok {
			t.Errorf("effect registration %q has no author activity disposition", key)
		}
	}
	var stale []string
	for key := range externalEffectStoryDispositions {
		if _, ok := seen[key]; !ok {
			stale = append(stale, key)
		}
	}
	sort.Strings(stale)
	if len(stale) > 0 {
		t.Fatalf("author activity effect dispositions without live registrations: %v", stale)
	}
}

type authoredEventOutputClassifier struct{}

func (authoredEventOutputClassifier) authorActivityEventDescriptor(_ runtimeauthoractivity.Scope, name string) (runtimeauthoractivity.EventDescriptor, bool) {
	return runtimeauthoractivity.EventDescriptor{EventType: name, Disposition: runtimeauthoractivity.StoryAuthored}, name == "phrase.completed"
}

type dynamicAuthoredEventDescriptorResolver struct {
	registry *runtimeauthoractivity.EventCatalogRegistry
}

func (r dynamicAuthoredEventDescriptorResolver) authorActivityEventDescriptor(scope runtimeauthoractivity.Scope, name string) (runtimeauthoractivity.EventDescriptor, bool) {
	return r.registry.Resolve(scope, name)
}

func (r dynamicAuthoredEventDescriptorResolver) authorActivityEventCatalogRegistered(scope runtimeauthoractivity.Scope) bool {
	return r.registry.HasScope(scope)
}

func TestDynamicAuthorActivityEventDescriptorRequiresLiveExactScopeLease(t *testing.T) {
	db := openAuthorActivityAdapterDB(t)
	scope := runtimeauthoractivity.BundleScope(uuid.NewString(), "bundle-v1:sha256:"+strings.Repeat("d", 64))
	registry := runtimeauthoractivity.NewEventCatalogRegistry()
	lease, err := registry.Register(scope, []runtimeauthoractivity.EventDescriptor{{EventType: "static.event", Disposition: runtimeauthoractivity.StoryDifferent}})
	if err != nil {
		t.Fatal(err)
	}
	resolver := dynamicAuthoredEventDescriptorResolver{registry: registry}
	dynamic := runtimeauthoractivity.EventDescriptor{
		EventType: "chat/instance-1/reply.requested", Disposition: runtimeauthoractivity.StoryAuthored, AuthorSummaryField: "text",
	}
	base := runtimeauthoractivity.WithScope(context.Background(), scope)
	base, err = runtimeauthoractivity.WithResolvedEventDescriptor(base, scope, dynamic)
	if err != nil {
		t.Fatal(err)
	}
	event := eventtest.PersistedProjection(
		uuid.NewString(), events.EventType(dynamic.EventType), "sender", "", []byte(`{"text":"hello"}`), 0,
		uuid.NewString(), "", events.EventEnvelope{}, time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC),
	)

	tx, err := db.BeginTx(base, nil)
	if err != nil {
		t.Fatal(err)
	}
	story, err := runtimeauthoractivity.Begin(base, tx, runtimeauthoractivity.DialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	if err := recordPersistedEventAuthorActivity(story, resolver, event, "sender", "agent"); err != nil {
		t.Fatal(err)
	}
	if err := runtimeauthoractivity.Finalize(story); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	page, err := runtimeauthoractivity.List(base, db, runtimeauthoractivity.DialectSQLite, runtimeauthoractivity.ListOptions{Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Occurrences) != 1 || page.Occurrences[0].AuthorSafeSummary != "hello" {
		t.Fatalf("dynamic occurrence = %#v", page.Occurrences)
	}

	lease.Release()
	tx, err = db.BeginTx(base, nil)
	if err != nil {
		t.Fatal(err)
	}
	story, err = runtimeauthoractivity.Begin(base, tx, runtimeauthoractivity.DialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	stale := eventtest.PersistedProjection(
		uuid.NewString(), events.EventType(dynamic.EventType), "sender", "", []byte(`{"text":"stale"}`), 0,
		uuid.NewString(), "", events.EventEnvelope{}, time.Date(2026, 7, 14, 12, 0, 1, 0, time.UTC),
	)
	if err := recordPersistedEventAuthorActivity(story, resolver, stale, "sender", "agent"); err == nil || !strings.Contains(err.Error(), "no live registry lease") {
		t.Fatalf("stale lease error = %v", err)
	}
	_ = tx.Rollback()
}

func TestDynamicAuthorActivityEventDescriptorRejectsStaticConflict(t *testing.T) {
	scope := runtimeauthoractivity.BundleScope(uuid.NewString(), "bundle-v1:sha256:"+strings.Repeat("e", 64))
	registry := runtimeauthoractivity.NewEventCatalogRegistry()
	lease, err := registry.Register(scope, []runtimeauthoractivity.EventDescriptor{{EventType: "message.sent", Disposition: runtimeauthoractivity.StoryAuthored, AuthorSummaryField: "text"}})
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Release()
	ctx := runtimeauthoractivity.WithScope(context.Background(), scope)
	ctx, err = runtimeauthoractivity.WithResolvedEventDescriptor(ctx, scope, runtimeauthoractivity.EventDescriptor{
		EventType: "message.sent", Disposition: runtimeauthoractivity.StoryAuthored, AuthorSummaryField: "body",
	})
	if err != nil {
		t.Fatal(err)
	}
	event := eventtest.PersistedProjection(
		uuid.NewString(), events.EventType("message.sent"), "sender", "", []byte(`{"text":"hello"}`), 0,
		uuid.NewString(), "", events.EventEnvelope{}, time.Now(),
	)
	if err := recordPersistedEventAuthorActivity(ctx, dynamicAuthoredEventDescriptorResolver{registry: registry}, event, "sender", "agent"); err == nil || !strings.Contains(err.Error(), "conflicts") {
		t.Fatalf("descriptor conflict error = %v", err)
	}
}

func TestAuthorActivityEventAndEffectAdaptersRenderExactSubjects(t *testing.T) {
	db := openAuthorActivityAdapterDB(t)
	ctx := runtimecorrelation.WithRuntimeInstanceID(testAuthorActivityContext(), uuid.NewString())
	ctx = runtimecorrelation.WithBundleSourceFact(ctx, runtimecorrelation.BundleSourceFact{BundleHash: "bundle-v1:sha256:" + strings.Repeat("a", 64)})
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	story, err := runtimeauthoractivity.Begin(ctx, tx, runtimeauthoractivity.DialectSQLite)
	if err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	now := time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC)
	event := eventtest.PersistedProjection(
		uuid.NewString(), events.EventType("phrase.completed"), "phrase-completer", "", []byte(`{}`), 0,
		uuid.NewString(), "", events.EventEnvelope{}, now,
	)
	if err := recordPersistedEventAuthorActivity(story, authoredEventOutputClassifier{}, event, "phrase-completer", "agent"); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	if err := recordExternalEffectStory(story, externalEffectStorySource{
		AttemptID: uuid.NewString(), Kind: "provider_turn", Class: "provider_call", Adapter: "anthropic_api",
		Transport: "https", AuthorityKind: "normal_agent", AuthorityID: "normalizer", AgentID: "normalizer", Ordinal: 1,
	}, runtimeeffects.StateLaunched, nil, now.Add(time.Second)); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	if err := runtimeauthoractivity.Finalize(story); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	page, err := runtimeauthoractivity.List(ctx, db, runtimeauthoractivity.DialectSQLite, runtimeauthoractivity.ListOptions{Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Occurrences) != 2 {
		t.Fatalf("occurrences = %#v, want event and effect", page.Occurrences)
	}
	if page.Occurrences[0].Projection.SubjectID != "" || page.Occurrences[1].Projection.SubjectID != "" {
		t.Fatalf("internal identities entered author projections: %#v", page.Occurrences)
	}
	var output bytes.Buffer
	if err := runtimeauthoractivity.Render(&output, page.Occurrences, runtimeauthoractivity.RenderOptions{Mode: runtimeauthoractivity.RenderPlain, Width: 120}); err != nil {
		t.Fatal(err)
	}
	want := "12:00:00  phrase-completer → phrase.completed\n"
	if output.String() != want {
		t.Fatalf("author activity output = %q, want %q", output.String(), want)
	}
}

func openAuthorActivityAdapterDB(t *testing.T) *sql.DB {
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
			version INTEGER NOT NULL CHECK (version = 2), transition TEXT NOT NULL, source_owner TEXT NOT NULL,
			source_identity TEXT NOT NULL, dedup_key TEXT NOT NULL UNIQUE, run_id TEXT, entity_id TEXT, agent_id TEXT,
			flow_id TEXT, scope_kind TEXT NOT NULL, runtime_instance_id TEXT, bundle_hash TEXT, author_safe_summary TEXT,
			projection TEXT NOT NULL DEFAULT '{}', failure TEXT, occurred_at TIMESTAMP NOT NULL
		)`,
	} {
		if _, err := db.Exec(ddl); err != nil {
			t.Fatal(err)
		}
	}
	return db
}

func authorActivityRegistryRepoRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve author activity registry test path")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
}
