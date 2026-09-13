package tools_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/agentframe"
	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/tools"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/division-sh/swarm/internal/store"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil/runlifecyclefixture"
	"github.com/google/uuid"
)

type sparseEntityToolStore interface {
	tools.EntityPersistence
	EnsureSourceArtifact(context.Context, *sourceartifact.AdmittedSourceArtifact) (sourceartifact.EnsureResult, error)
	LoadRunDebugReport(context.Context, string, operatorread.RunDebugQueryOptions) (operatorread.RunDebugReport, error)
}

func seedEntityToolSourceRun(t *testing.T, selected any, bundle *contracts.WorkflowContractBundle) context.Context {
	t.Helper()
	if bundle == nil || bundle.SourceArtifact == nil {
		t.Fatal("entity persistence fixture requires loaded source")
	}
	fact, err := correlation.NewSourceArtifactFact(bundle.SourceArtifact.BundleHash())
	if err != nil {
		t.Fatal(err)
	}
	ctx := authoractivity.WithScope(context.Background(), authoractivity.BundleScope(authorActivityTestRuntimeInstanceID, fact.BundleHash()))
	ctx = effects.WithDifferentOwner(ctx, effects.OwnerBuildTestInfrastructure)
	ctx = correlation.WithSourceArtifactFact(correlation.WithRunID(ctx, entityToolTestRunID), fact)
	fixture := runlifecyclefixture.Fixture{Origin: runlifecyclefixture.ScenarioSetupOrigin(), RunID: entityToolTestRunID, Artifact: bundle.SourceArtifact}
	switch selected.(type) {
	case *store.PostgresStore:
		runlifecyclefixture.RequirePostgres(t, ctx, storetest.DatabaseForTest(selected), fixture)
	case *store.SQLiteRuntimeStore:
		runlifecyclefixture.RequireSQLite(t, ctx, storetest.DatabaseForTest(selected), fixture)
	default:
		t.Fatalf("unsupported entity test backend %T", selected)
	}
	return ctx
}

func TestEntitySparseGeneratedToolMutation(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			actor := models.AgentConfig{ExecutionMode: "live", ID: "writer", Role: "writer"}
			bundle := loadWave1EntityToolMultiFlowBundle(t, map[string]entityToolFlowFixture{
				"work": {
					SchemaYAML: "name: work\nmode: static\ninitial_state: queued\nstates: [queued, done]\nterminal_states: [done]\ntool_surface: {role_scoped_entity_tools: true}\n",
					TypesYAML:  "types:\n  Profile:\n    name: text\n    note: text?\n",
					EntitiesYAML: `
work:
  label: text?
  profile: Profile?
  left: text?
  right:
    type: text?
    equal_to: left
  token:
    type: text
    immutable: true
  seeded:
    type: integer
    initial: 7
`,
					AgentsYAML: `
writer:
  role: writer
  intent: {inline: "Maintain accurate work records."}
  entity_writes:
    work:
      save: [label, profile, left, right, token]
`,
				},
			})
			var persistence sparseEntityToolStore
			var db *sql.DB
			if backend == "sqlite" {
				s := newSQLiteRuntimeToolStoreForTest(t)
				persistence, db = s, storetest.DatabaseForTest(s)
			} else {
				s := newPostgresHumanTaskToolStoreForTest(t)
				persistence, db = s, storetest.DatabaseForTest(s)
			}
			runID, entityID := uuid.NewString(), uuid.NewString()
			fact, err := correlation.NewSourceArtifactFact(bundle.SourceArtifact.BundleHash())
			if err != nil {
				t.Fatal(err)
			}
			ctx := authoractivity.WithScope(context.Background(), authoractivity.BundleScope(authorActivityTestRuntimeInstanceID, fact.BundleHash()))
			ctx = effects.WithDifferentOwner(ctx, effects.OwnerBuildTestInfrastructure)
			ctx = correlation.WithSourceArtifactFact(correlation.WithRunID(ctx, runID), fact)
			if _, err := persistence.EnsureSourceArtifact(ctx, bundle.SourceArtifact); err != nil {
				t.Fatal(err)
			}
			fixture := runlifecyclefixture.Fixture{Origin: runlifecyclefixture.ScenarioSetupOrigin(), RunID: runID, BundleHash: fact.BundleHash()}
			if backend == "sqlite" {
				runlifecyclefixture.RequireSQLite(t, ctx, db, fixture)
			} else {
				runlifecyclefixture.RequirePostgres(t, ctx, db, fixture)
			}
			source := semanticview.Wrap(bundle)
			if problems := tools.ValidateGeneratedToolSchemaClosureForSource(source); len(problems) != 0 {
				t.Fatalf("source-loaded sparse entity tool schemas rejected: %v", problems)
			}
			if err := persistence.CreateEntity(ctx, tools.EntityCreateRecord{
				RunID: runID, EntityID: entityID, Source: source, FlowInstance: "work/one", EntityType: "work", CurrentState: "queued",
				FieldsJSON: json.RawMessage(`{"left":"paired","right":"paired","token":"fixed"}`), CreatedAt: time.Now().UTC(),
				Writer: tools.EntityMutationWriter{Type: "system_node", ID: "creator", HandlerStep: "create_entity"},
			}); err != nil {
				t.Fatal(err)
			}
			inbound := eventtest.PersistedChildForProducer(
				uuid.NewString(), events.EventType("work.ready"), eventtest.Producer(events.EventProducerNode, "creator"), "", []byte(`{}`), 0,
				runID, uuid.NewString(), events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, entityID), "work/one"), time.Now().UTC(),
			)
			storetest.CommitSemanticEvent(t, ctx, persistence, inbound)
			ctx = tools.WithActor(bus.WithInboundEvent(ctx, inbound), actor)
			exec := tools.NewExecutorWithOptions(nil, tools.ExecutorOptions{EntityStore: persistence, WorkflowSource: source})
			identity := tools.EntityIdentity{RunID: runID, EntityID: entityID}
			read := func() map[string]any {
				t.Helper()
				row, found, err := persistence.LoadEntityState(ctx, identity)
				if err != nil || !found {
					t.Fatalf("load: found=%v err=%v", found, err)
				}
				return row
			}
			whole, err := exec.Execute(ctx, "read_work", map[string]any{})
			if err != nil {
				t.Fatal(err)
			}
			fields := whole.(map[string]any)["fields"].(map[string]any)
			if _, present := fields["label"]; present {
				t.Fatalf("read fabricated label: %#v", fields)
			}
			if testNumericValue(fields["seeded"]) != 7 {
				t.Fatalf("creation lost explicit initial: %#v", fields)
			}
			assertSparseToolContinuation(t, whole, fields)
			if _, err := exec.Execute(ctx, "read_work_label", map[string]any{}); err == nil {
				t.Fatal("unassigned typed field read returned a fabricated value")
			}
			for _, tc := range []struct {
				name  string
				value any
			}{
				{"update_work_profile_name", "no parent"},
				{"save_work_left", "unpaired"},
				{"save_work_token", "changed"},
				{"save_work_label", nil},
				{"save_work_profile", map[string]any{"note": "required name absent"}},
			} {
				before := read()
				if _, err := exec.Execute(ctx, tc.name, map[string]any{"value": tc.value}); err == nil {
					t.Fatalf("%s accepted %#v", tc.name, tc.value)
				}
				if after := read(); !reflect.DeepEqual(before, after) {
					t.Fatalf("rejected %s changed stored snapshot:\nbefore=%#v\nafter=%#v", tc.name, before, after)
				}
			}
			for _, tc := range []struct {
				name  string
				value any
			}{
				{"save_work_label", ""},
				{"save_work_left", "paired"},
				{"save_work_profile", map[string]any{"name": "first", "note": "previous"}},
				{"save_work_profile", map[string]any{"name": "first"}},
				{"update_work_profile_name", "second"},
			} {
				if _, err := exec.Execute(ctx, tc.name, map[string]any{"value": tc.value}); err != nil {
					for cause := errors.Unwrap(err); cause != nil; cause = errors.Unwrap(cause) {
						t.Logf("cause: %v", cause)
					}
					t.Fatalf("%s: %v", tc.name, err)
				}
			}
			row := read()
			fields = row["fields"].(map[string]any)
			if label, exists := fields["label"]; !exists || label != "" {
				t.Fatalf("explicit empty value lost: %#v", fields)
			}
			profile := fields["profile"].(map[string]any)
			if profile["name"] != "second" {
				t.Fatalf("nested mutation lost: %#v", fields)
			}
			if _, exists := profile["note"]; exists {
				t.Fatalf("nested optional value fabricated: %#v", fields)
			}
			whole, err = exec.Execute(ctx, "read_work", map[string]any{})
			if err != nil {
				t.Fatal(err)
			}
			assertSparseToolContinuation(t, whole, fields)

			// Exercise the persistence entry directly, without optimistic executor checks.
			before := read()
			otherSource := semanticview.Wrap(loadWave1EntityToolBundle(t, actor, "other", "work", "", "work:\n  label: text?\n"))
			for _, update := range []tools.EntityFieldUpdate{
				{Source: source, FieldPath: "token", Value: "changed"},
				{Source: source, FieldPath: "left", Value: "unpaired"},
				{Source: source, FieldPath: "label", Value: nil},
				{FieldPath: "label", Value: "unbound"},
				{Source: otherSource, FieldPath: "label", Value: "wrong source"},
			} {
				update.RunID, update.EntityID = runID, entityID
				update.Writer = tools.EntityMutationWriter{Type: "agent", ID: actor.ID, HandlerStep: "save_entity_field"}
				if _, err := persistence.SaveEntityField(ctx, update); err == nil {
					t.Fatalf("backend accepted invalid update: %#v", update)
				}
			}
			if !reflect.DeepEqual(before, read()) {
				t.Fatal("backend rejection changed state or revision")
			}
			// Inspect actual persisted history on both stores. Only PostgreSQL's
			// existing internal debug reader exposes these records; neither store
			// has a public entity.history RPC.
			historyRows, err := db.QueryContext(ctx, `SELECT entity_id, domain, path, COALESCE(new_value, 'null')
				FROM entity_mutations WHERE run_id = $1 ORDER BY created_at DESC, mutation_id DESC`, runID)
			if err != nil {
				t.Fatal(err)
			}
			var mutations []operatorread.RunDebugMutation
			for historyRows.Next() {
				var mutation operatorread.RunDebugMutation
				var value []byte
				if err := historyRows.Scan(&mutation.EntityID, &mutation.Domain, &mutation.Path, &value); err != nil {
					historyRows.Close()
					t.Fatal(err)
				}
				mutation.NewValue = append(json.RawMessage(nil), value...)
				mutations = append(mutations, mutation)
			}
			if err := historyRows.Err(); err != nil {
				historyRows.Close()
				t.Fatal(err)
			}
			historyRows.Close()
			if backend == "postgres" {
				report, err := persistence.LoadRunDebugReport(ctx, runID, operatorread.RunDebugQueryOptions{MutationLimit: 100})
				if err != nil {
					t.Fatal(err)
				}
				if len(report.Mutations) != len(mutations) {
					t.Fatalf("debug history omitted records: got %d want %d", len(report.Mutations), len(mutations))
				}
				mutations = report.Mutations
			}
			labels, initials, withNote, withoutNote, nestedNames := 0, 0, 0, 0, 0
			for _, mutation := range mutations {
				if mutation.EntityID != entityID || mutation.Domain != "authored_field" {
					continue
				}
				switch mutation.Path {
				case "label":
					labels++
					if string(mutation.NewValue) != `""` {
						t.Fatalf("history lost explicit empty label: %+v", mutation)
					}
				case "seeded":
					initials++
					if string(mutation.NewValue) != "7" {
						t.Fatalf("history changed explicit initial: %+v", mutation)
					}
				case "profile.name":
					nestedNames++
					if string(mutation.NewValue) != `"second"` {
						t.Fatalf("history changed nested update: %+v", mutation)
					}
				case "profile":
					var value map[string]any
					if err := json.Unmarshal(mutation.NewValue, &value); err != nil {
						t.Fatal(err)
					}
					if note, present := value["note"]; present {
						if note != "previous" {
							t.Fatalf("history fabricated note: %+v", mutation)
						}
						withNote++
					} else {
						withoutNote++
					}
				}
			}
			if labels != 1 || initials != 1 || withNote != 1 || withoutNote != 1 || nestedNames != 1 {
				t.Fatalf("history presence counts label=%d initial=%d with_note=%d without_note=%d nested_name=%d", labels, initials, withNote, withoutNote, nestedNames)
			}
		})
	}
}

func assertSparseToolContinuation(t *testing.T, result any, wantFields map[string]any) {
	t.Helper()
	raw, err := json.Marshal([]map[string]any{{"name": "read_work", "ok": true, "result": result}})
	if err != nil {
		t.Fatal(err)
	}
	continuation, err := agentframe.NewToolContinuation("agent-frame:v1:"+uuid.NewString(), raw)
	if err != nil {
		t.Fatal(err)
	}
	durable, err := continuation.Encode()
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := agentframe.DecodeToolContinuation(durable)
	if err != nil {
		t.Fatal(err)
	}
	var batch []struct {
		Result struct {
			Fields map[string]any `json:"fields"`
		} `json:"result"`
	}
	if err := json.Unmarshal(decoded.ToolResult(), &batch); err != nil || len(batch) != 1 {
		t.Fatalf("decode canonical tool result: %v", err)
	}
	// Compare serialized JSON because the persistence reader and execution
	// normalizer intentionally use different in-memory integer representations.
	want, _ := json.Marshal(wantFields)
	got, _ := json.Marshal(batch[0].Result.Fields)
	if string(got) != string(want) {
		t.Fatalf("tool continuation changed field presence: got=%s want=%s", got, want)
	}
}
