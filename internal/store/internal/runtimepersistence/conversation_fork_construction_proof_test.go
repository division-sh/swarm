package runtimepersistence

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/google/uuid"
)

func TestConversationForkFixtureCompleteReadinessBothStores(t *testing.T) {
	for _, sqlite := range []bool{true, false} {
		backend := "postgres"
		if sqlite {
			backend = "sqlite"
		}
		for _, kind := range []conversationForkFixtureKind{conversationForkServedSource, conversationForkNativeChat, conversationForkDataOnlyChat} {
			t.Run(backend+"/"+[]string{"served", "native_chat", "data_only"}[kind], func(t *testing.T) {
				selected, _ := newLifecycleDiagnosticTestStore(t, sqlite)
				bundle := conversationForkConstructionBundle(t)
				ctx := testAuthorActivityContextForBundle(bundle.SourceArtifact.BundleHash())
				item := createConversationForkLifecycleDiagnostic(t, ctx, selected, bundle)
				at := time.Now().UTC().Truncate(time.Second)
				fixture := ConversationForkSourceFixture{Identity: item.Identity, RunID: item.Identity.RunID, EntityID: uuid.NewString(), SessionID: uuid.NewString(), Event1ID: uuid.NewString(), Event2ID: uuid.NewString(), CreatedAt: at, Turn1At: at.Add(-2 * time.Minute), Turn2At: at.Add(-time.Minute), kind: kind}
				if kind != conversationForkDataOnlyChat {
					fixture.Construction = prepareNativeConversationForkConstruction(t, ctx, selected, bundle, fixture)
				}
				result, err := SeedConversationForkSourceForTest(ctx, selected, fixture)
				if err != nil {
					t.Fatal(err)
				}
				observed, err := selected.(pipeline.DynamicFlowRuntimeReadinessPersistence).InspectDynamicFlowRuntimeReadinessForSource(ctx, mustStoreTestSourceArtifactFact(bundle.SourceArtifact.BundleHash()))
				if err != nil {
					t.Fatalf("complete construction is unreadable: %v", err)
				}
				members := append(observed.CurrentCompleted, observed.CurrentPending...)
				if len(observed.SourceTransitionRequired) != 0 {
					t.Fatalf("fixture acquired foreign-source readiness: %+v", observed)
				}
				if kind == conversationForkDataOnlyChat {
					if len(members) != 0 || result.Construction.Acknowledged {
						t.Fatalf("data-only import acquired construction: %+v", observed)
					}
					return
				}
				if !result.Construction.Created || !result.Construction.Acknowledged || !result.Construction.Plan.OccurredAt.Equal(fixture.Turn1At.Add(-30*time.Second)) {
					t.Fatal("historical construction lost acknowledgement/occurrence")
				}
				if len(members) != 2 {
					t.Fatalf("wrong exact parent/child readiness membership: %+v", observed)
				}
				seen := map[string]string{}
				for _, member := range members {
					if member.Plan.RunID != fixture.RunID || member.Plan.BundleHash != bundle.SourceArtifact.BundleHash() || seen[member.InstancePath] != "" {
						t.Fatalf("readiness changed scope or duplicated membership: %+v", observed)
					}
					seen[member.InstancePath] = member.Plan.Identity.EntityID
				}
				if !reflect.DeepEqual(seen, map[string]string{fixture.RunID: fixture.RunID, "flow/forkchat": fixture.EntityID}) {
					t.Fatalf("readiness identities = %v", seen)
				}
			})
		}
	}
}

func TestConversationForkHistoricalConstructionRefusalsBothStores(t *testing.T) {
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
			at := time.Now().UTC().Truncate(time.Second)
			fixture := ConversationForkSourceFixture{RunID: item.Identity.RunID, EntityID: uuid.NewString(), Turn1At: at.Add(-time.Minute), kind: conversationForkNativeChat}
			fixture.Construction = prepareNativeConversationForkConstruction(t, ctx, selected, bundle, fixture)
			before, err := ReadFiniteRunStartStorageCountsForTest(ctx, selected)
			if err != nil {
				t.Fatal(err)
			}
			for _, name := range []string{"missing_time", "foreign_source", "mismatched_time", "missing_parent_time"} {
				t.Run(name, func(t *testing.T) {
					bad, writeCtx, want := fixture, ctx, ""
					switch name {
					case "missing_time":
						bad.Construction.Child.OccurredAt = time.Time{}
						want = "requires exact occurrence time"
					case "foreign_source":
						writeCtx = correlation.WithSourceArtifactFact(ctx, mustStoreTestSourceArtifactFact(authorActivityTestBundleHash))
						want = "does not match fixed historical source/identity/time"
					case "mismatched_time":
						bad.Turn1At = bad.Turn1At.Add(time.Second)
						want = "does not match fixed historical source/identity/time"
					case "missing_parent_time":
						parent := *bad.Construction.Parent
						parent.OccurredAt = time.Time{}
						bad.Construction.Parent = &parent
						want = "conversation construction requires its exact fieldless parent"
					}
					if _, err := SeedConversationForkSourceForTest(writeCtx, selected, bad); err == nil || !strings.Contains(err.Error(), want) {
						t.Fatalf("historical refusal = %v, want %s", err, want)
					}
					after, err := ReadFiniteRunStartStorageCountsForTest(ctx, selected)
					if err != nil || after != before {
						t.Fatalf("historical refusal changed physical families: before=%+v after=%+v err=%v", before, after, err)
					}
					mutations, err := ObserveEntityMutationHistoryForTest(ctx, selected, fixture.RunID)
					if err != nil || len(mutations) != 0 {
						t.Fatalf("historical refusal retained mutations: %+v err=%v", mutations, err)
					}
					readiness, err := selected.(pipeline.DynamicFlowRuntimeReadinessPersistence).InspectDynamicFlowRuntimeReadinessForSource(ctx, mustStoreTestSourceArtifactFact(bundle.SourceArtifact.BundleHash()))
					if err != nil || len(readiness.CurrentCompleted)+len(readiness.CurrentPending)+len(readiness.SourceTransitionRequired) != 0 {
						t.Fatalf("historical refusal retained construction/readiness: %+v err=%v", readiness, err)
					}
				})
			}
		})
	}
}

func TestConversationForkHistoricalClockDoesNotLeakToOrdinaryConstructionBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			files := canonicalrouting.ConversationForkConstructionDocuments()
			files["schema.yaml"] = canonicalrouting.ConversationForkHistoryRootDocument()
			files["events.yaml"] = "conversation.created:\n  name: text\n"
			f := newReceiverConfigActivationFixtureWithDocuments(t, backend, false, files, nil)
			run := correlation.RunIDFromContext(f.ctx)
			cut := time.Date(2026, 1, 1, 0, 0, 30, 0, time.UTC)
			fixture := ConversationForkSourceFixture{RunID: run, EntityID: uuid.NewString(), Turn1At: cut, kind: conversationForkNativeChat}
			var err error
			fixture.Construction, err = PrepareConversationForkConstructionForTest(f.ctx, f.store, semanticview.Wrap(f.bundle), f.manager, fixture)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := SeedConversationForkSourceForTest(f.ctx, f.store, fixture); err != nil {
				t.Fatal(err)
			}
			born := cut.Add(-30 * time.Second)
			assertConversationConstructionBirthClock(t, f.ctx, f.store, run, fixture.EntityID, "Before", born, born)
			root := flowidentity.Stored(semanticview.Wrap(f.bundle), ".", run, run, run, "")
			child, err := flowidentity.KeyedChild(semanticview.Wrap(f.bundle), root, "flow", "ordinary")
			if err != nil {
				t.Fatal(err)
			}
			event := eventtest.ExistingRunRootIngress(uuid.NewString(), "flow/conversation.created", "ordinary-constructor", "", []byte(`{"name":"ordinary"}`), 0, run, events.EventEnvelope{}, born)
			plan, err := f.manager.PrepareFlowInstanceActivation(f.ctx, pipeline.FlowInstanceActivationRequest{ContractBundle: semanticview.Wrap(f.bundle), Instance: child, ConstructorInput: "conversation.created", ResolvedKey: "ordinary", TriggerEvent: event, OccurredAt: born})
			if err != nil {
				t.Fatal(err)
			}
			before := time.Now().UTC().Truncate(time.Microsecond)
			committed, err := (agentFixtureFlowActivationCommitter{store: f.store}).CommitFlowInstanceActivation(f.ctx, plan)
			after := time.Now().UTC()
			if err != nil || !committed.Created || !committed.Acknowledged {
				t.Fatalf("ordinary construction: %+v err=%v", committed, err)
			}
			if backend == "sqlite" {
				before, after = born, born
			}
			assertConversationConstructionBirthClock(t, f.ctx, f.store, run, plan.Identity.EntityID, "ordinary", before, after)
		})
	}
}

func assertConversationConstructionBirthClock(t *testing.T, ctx context.Context, selected any, run, entity, name string, earliest, latest time.Time) {
	t.Helper()
	mutations, err := ObserveEntityMutationHistoryForTest(ctx, selected, run)
	if err != nil {
		t.Fatal(err)
	}
	ids, clocks := map[string]string{}, map[string]time.Time{}
	for _, row := range mutations {
		key := row.Domain + "/" + row.Path
		if row.EntityID != entity || string(row.OldValue) != "null" || (key != "authored_field/name" && key != "lifecycle_state/" && key != "bookkeeping/stage_entry") {
			continue
		}
		if ids[key] != "" {
			t.Fatalf("duplicate birth mutation %s", key)
		}
		at := row.CreatedAt
		if at.IsZero() || at.Before(earliest) || at.After(latest) {
			t.Fatalf("birth clock %s = %v, want [%v,%v]", key, at, earliest, latest)
		}
		if key == "authored_field/name" {
			var got string
			if err := json.Unmarshal(row.NewValue, &got); err != nil || got != name {
				t.Fatalf("birth name=%q err=%v", got, err)
			}
		}
		ids[key], clocks[row.MutationID] = row.MutationID, at
	}
	if len(ids) != 3 || ids["authored_field/name"] == "" || ids["lifecycle_state/"] == "" || ids["bookkeeping/stage_entry"] == "" {
		t.Fatalf("exact birth evidence changed: %v", ids)
	}
	activity, err := selected.(authoractivity.Reader).ListAuthorActivity(ctx, authoractivity.ListOptions{RunID: run, EntityID: entity, Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	matched := 0
	for _, row := range activity.Occurrences {
		if row.Kind == authoractivity.KindEntityLifecycle && row.Transition == "created" {
			matched++
			if row.SourceOwner != "entity_mutations" || row.SourceIdentity != ids["lifecycle_state/"] || !row.OccurredAt.Equal(clocks[row.SourceIdentity]) {
				t.Fatalf("birth mutation/activity clock or identity split: %+v", row)
			}
		}
	}
	if matched != 1 {
		t.Fatalf("lifecycle birth activities=%d, want1", matched)
	}
}
