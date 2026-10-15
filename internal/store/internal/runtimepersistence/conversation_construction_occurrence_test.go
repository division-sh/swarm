package runtimepersistence

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	runtimemutationlog "github.com/division-sh/swarm/internal/runtime/mutationlog"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	privatemutationlog "github.com/division-sh/swarm/internal/store/internal/backend/mutationlog"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	"github.com/google/uuid"
)

func TestConversationConstructionOccurrenceRefusalsBothStores(t *testing.T) {
	for _, sqlite := range []bool{true, false} {
		backend := "postgres"
		if sqlite {
			backend = "sqlite"
		}
		t.Run(backend, func(t *testing.T) {
			selected, _ := newLifecycleDiagnosticTestStore(t, sqlite)
			bundle := conversationForkConstructionBundle(t)
			ctx := testAuthorActivityContextForBundle(bundle.SourceArtifact.BundleHash())
			item := createConversationForkLifecycleDiagnostic(t, ctx, selected, bundle)
			ctx = correlation.WithRunID(ctx, item.Identity.RunID)
			fixture := ConversationForkSourceFixture{RunID: item.Identity.RunID, EntityID: uuid.NewString(), Turn1At: time.Date(2026, 1, 1, 0, 0, 30, 0, time.UTC), kind: conversationForkNativeChat}
			fixture.Construction = prepareNativeConversationForkConstruction(t, ctx, selected, bundle, fixture)
			record, err := fixture.Construction.Child.PersistenceRecord()
			if err != nil {
				t.Fatal(err)
			}
			before, err := ReadFiniteRunStartStorageCountsForTest(ctx, selected)
			if err != nil {
				t.Fatal(err)
			}
			var ended *mutationprotocol.Attempt
			var retained *privatemutationlog.ConstructionOccurrence
			err = runSelectedFixtureMutation(ctx, selected, "capture construction occurrence", func(txctx context.Context, attempt *mutationprotocol.Attempt) error {
				ended = attempt
				var err error
				retained, err = privatemutationlog.ConversationForkConstructionOccurrenceForTest(txctx, attempt, fixture.Construction.Parent, fixture.Construction.Child)
				return err
			})
			if err != nil {
				t.Fatal(err)
			}
			if err := retained.ValidateBirthOccurrence(ctx, ended, record.State); err == nil || err.Error() != "mutation attempt is not active" {
				t.Fatalf("completed attempt retained occurrence: %v", err)
			}
			for _, name := range []string{"absent", "zero", "cross_attempt", "cross_run", "cross_source", "cross_entity", "wrong_time", "wrong_operation", "later_same_entity", "wrong_birth_value"} {
				t.Run(name, func(t *testing.T) {
					err := runSelectedFixtureMutation(ctx, selected, "construction occurrence refusal", func(txctx context.Context, attempt *mutationprotocol.Attempt) error {
						occurrence, err := privatemutationlog.ConversationForkConstructionOccurrenceForTest(txctx, attempt, fixture.Construction.Parent, fixture.Construction.Child)
						if err != nil {
							return err
						}
						state, writeCtx := record.State, txctx
						previous := runtimemutationlog.EntityStateProjection{}
						next := runtimemutationlog.EntityStateProjection{Fields: map[string]any{"name": "Before"}}
						writer := runtimemutationlog.Writer{Type: "platform", ID: "entity_initial_value", HandlerStep: "create_entity"}
						want := ""
						switch name {
						case "absent":
							occurrence, want = nil, "construction occurrence evidence is required"
						case "zero":
							occurrence, want = &privatemutationlog.ConstructionOccurrence{}, "belongs to another attempt/run/source"
						case "cross_attempt":
							occurrence, want = retained, "belongs to another attempt/run/source"
						case "cross_run":
							writeCtx, want = correlation.WithRunID(txctx, uuid.NewString()), "belongs to another attempt/run/source"
						case "cross_source":
							writeCtx, want = correlation.WithSourceArtifactFact(txctx, mustStoreTestSourceArtifactFact(authorActivityTestBundleHash)), "belongs to another attempt/run/source"
						case "cross_entity":
							state.EntityID, want = uuid.NewString(), "does not match its admitted entity/birth"
						case "wrong_time":
							state.UpdatedAt, want = state.UpdatedAt.Add(time.Second), "requires its exact birth operation/time"
						case "wrong_operation":
							state.Transition, want = pipeline.WorkflowEngineStateTransitionUpdateStateAndCompanion, "requires its exact birth operation/time"
						case "later_same_entity":
							previous.Fields, next.Fields, want = map[string]any{"name": "Before"}, map[string]any{"name": "Later"}, "refuses a non-birth or repeated mutation"
						case "wrong_birth_value":
							next.Fields, want = map[string]any{"name": "Wrong"}, "refuses a non-birth or repeated mutation"
						}
						// No lifecycle owner is supplied: each refusal must precede SQL,
						// not be hidden by a later missing-owner or rollback error.
						err = privatemutationlog.InsertConstructionEntityStateDiff(writeCtx, attempt, nil, state, previous, next, writer, occurrence, !sqlite)
						if err == nil || !strings.Contains(err.Error(), want) {
							t.Fatalf("%s refusal = %v, want %s", name, err, want)
						}
						return nil // Commit the empty attempt; rollback cannot hide effects.
					})
					if err != nil {
						t.Fatal(err)
					}
					after, err := ReadFiniteRunStartStorageCountsForTest(ctx, selected)
					if err != nil || after != before {
						t.Fatalf("refusal changed physical families: before=%+v after=%+v err=%v", before, after, err)
					}
					history, err := ObserveEntityMutationHistoryForTest(ctx, selected, fixture.RunID)
					if err != nil || len(history) != 0 {
						t.Fatalf("refusal retained mutation history: %+v err=%v", history, err)
					}
					activity, err := selected.(authoractivity.Reader).ListAuthorActivity(ctx, authoractivity.ListOptions{RunID: fixture.RunID, EntityID: fixture.EntityID, Limit: 100})
					if err != nil || len(activity.Occurrences) != 0 {
						t.Fatalf("refusal retained entity activity: %+v err=%v", activity, err)
					}
				})
			}
			if err := runSelectedFixtureMutation(ctx, selected, "copied construction evidence", func(txctx context.Context, attempt *mutationprotocol.Attempt) error {
				occurrence, err := privatemutationlog.ConversationForkConstructionOccurrenceForTest(txctx, attempt, fixture.Construction.Parent, fixture.Construction.Child)
				if err != nil {
					return err
				}
				fixture.Construction.Child.Instance.Fields["name"] = "mutated after admission"
				return occurrence.ValidateBirthOccurrence(txctx, attempt, record.State)
			}); err != nil {
				t.Fatalf("caller mutation changed captured evidence: %v", err)
			}
		})
	}
}
