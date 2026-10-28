package runtimepersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/operatorread"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/sourceartifact"
	storerunlifecycle "github.com/division-sh/swarm/internal/store/internal/backend/runlifecycle"
	"github.com/division-sh/swarm/internal/testutil/runlifecyclefixture"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
	"github.com/google/uuid"
)

type runOriginReadStore interface {
	LoadRunHeader(context.Context, string) (operatorread.RunHeader, error)
	ListRunHeaders(context.Context, operatorread.RunHeaderListOptions) ([]operatorread.RunHeader, string, error)
}

func TestEventAndScenarioRunOriginLifecycleParity(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		backend := backend
		t.Run(backend, func(t *testing.T) {
			fixture := openRunLifecycleCandidateParityFixture(t, backend)
			reader := fixture.store.(runOriginReadStore)
			ctx := testAuthorActivityContextForBundle(runLifecycleCandidateParityBundleHash)
			ctx = originalStoreFixtureContext(t, ctx, fixture.store)
			rooted, _ := sourceartifactfixture.Rooted(t, "fixture", sourceartifactfixture.Artifact())
			sourceartifactfixture.RequireArtifact(t, ctx, fixture.store, rooted.Artifact())
			ctx = runtimecorrelation.WithRootAdmission(ctx, rooted.Admission())
			at := time.Date(2026, 7, 29, 16, 0, 0, 0, time.UTC)

			t.Run("event_origin_survives_later_event", func(t *testing.T) {
				runID := uuid.NewString()
				trigger := eventtest.RunCreatingRootIngress(
					uuid.NewString(), "origin.trigger", "ingress", "", json.RawMessage(`{}`), 0,
					runID, "", events.EventEnvelope{}, at,
				)
				admitted := c4AdmitCreatingInput(t, trigger, runLifecycleCandidateParityBundleHash)
				if _, err := commitAdmittedSemanticEventFixtureOutcome(ctx, fixture.store, admitted, nil, pipelineobligation.ScopeDirect); err != nil {
					t.Fatalf("commit run-creating event: %v", err)
				}
				want, err := runtimerunlifecycle.EventRunOrigin(trigger.ID(), string(trigger.Type()))
				if err != nil {
					t.Fatal(err)
				}
				requireRunOriginHeader(t, ctx, reader, runID, want, 1)
				c4RequireExclusiveStart(t, ctx, fixture.store, runID, runLifecycleCandidateParityBundleHash, trigger.ID())
				c4RequireRevisionCount(t, ctx, fixture.store, runID, 1)
				if _, err := commitAdmittedSemanticEventFixtureOutcome(ctx, fixture.store, admitted, nil, pipelineobligation.ScopeDirect); err != nil {
					t.Fatalf("replay run-creating event: %v", err)
				}
				requireRunOriginHeader(t, ctx, reader, runID, want, 1)
				c4RequireExclusiveStart(t, ctx, fixture.store, runID, runLifecycleCandidateParityBundleHash, trigger.ID())
				c4RequireRevisionCount(t, ctx, fixture.store, runID, 1)

				later := eventtest.ExistingRunRootIngress(
					uuid.NewString(), "origin.later", "ingress", "", json.RawMessage(`{}`), 0,
					runID, events.EventEnvelope{}, at.Add(time.Second),
				)
				if err := commitSemanticEventFixture(ctx, fixture.store, later); err != nil {
					t.Fatalf("commit later event: %v", err)
				}
				requireRunOriginHeader(t, ctx, reader, runID, want, 2)
				c4RequireExclusiveStart(t, ctx, fixture.store, runID, runLifecycleCandidateParityBundleHash, trigger.ID())
			})

			t.Run("scenario_origin_survives_publication_and_lifecycle", func(t *testing.T) {
				runID := uuid.NewString()
				source := mustStoreTestSourceArtifactFact(runLifecycleCandidateParityBundleHash)
				request := runtimerunlifecycle.CreateRequest{
					RunID: runID, Origin: runtimerunlifecycle.ScenarioSetupRunOrigin(),
					Source: source, RootAdmission: rooted.Admission(), StartedAt: at.Add(time.Minute),
				}
				if disposition, err := createRunLifecycleParity(fixture, ctx, request); err != nil ||
					disposition != runtimerunlifecycle.MutationApplied {
					t.Fatalf("create scenario run = %s/%v", disposition, err)
				}
				if disposition, err := createRunLifecycleParity(fixture, ctx, request); err != nil ||
					disposition != runtimerunlifecycle.MutationExactNoop {
					t.Fatalf("replay scenario run creation = %s/%v", disposition, err)
				}
				want := runtimerunlifecycle.ScenarioSetupRunOrigin()
				requireRunOriginHeader(t, ctx, reader, runID, want, 0)
				c4RequireExclusiveStart(t, ctx, fixture.store, runID, runLifecycleCandidateParityBundleHash, "")
				c4RequireRevisionCount(t, ctx, fixture.store, runID, 1)
				requireListedRunOrigin(t, ctx, reader, runID, want)

				later := eventtest.ExistingRunRootIngress(
					uuid.NewString(), "scenario.later", "setup", "", json.RawMessage(`{}`), 0,
					runID, events.EventEnvelope{}, at.Add(time.Minute+time.Second),
				)
				if err := commitSemanticEventFixture(ctx, fixture.store, later); err != nil {
					t.Fatalf("commit scenario event: %v", err)
				}
				requireRunOriginHeader(t, ctx, reader, runID, want, 1)
				c4RequireExclusiveStart(t, ctx, fixture.store, runID, runLifecycleCandidateParityBundleHash, "")

				replacement := mustStoreTestSourceArtifactFact(runLifecycleCandidateParityReplacementHash)
				replacementRoot, _ := sourceartifactfixture.Rooted(t, "candidate-replacement", runLifecycleCandidateParityReplacementArtifact)
				if disposition, err := reviseRunLifecycleSourceParity(fixture, ctx, runID, replacement, replacementRoot); err != nil ||
					disposition != runtimerunlifecycle.MutationApplied {
					t.Fatalf("revise scenario source = %s/%v", disposition, err)
				}
				ctx = startupownership.CarryMutationAuthority(runtimecorrelation.WithRootAdmission(testAuthorActivityContextForBundle(runLifecycleCandidateParityReplacementHash), replacementRoot.Admission()), ctx)
				requireRunOriginHeader(t, ctx, reader, runID, want, 1)
				c4RequireExclusiveStart(t, ctx, fixture.store, runID, runLifecycleCandidateParityBundleHash, "")
				for _, state := range []runtimerunlifecycle.State{
					runtimerunlifecycle.StatePaused,
					runtimerunlifecycle.StateRunning,
				} {
					if disposition, err := transitionRunLifecycleParity(fixture, ctx, runID, state); err != nil ||
						disposition != runtimerunlifecycle.MutationApplied {
						t.Fatalf("transition scenario run to %s = %s/%v", state, disposition, err)
					}
					requireRunOriginHeader(t, ctx, reader, runID, want, 1)
					c4RequireExclusiveStart(t, ctx, fixture.store, runID, runLifecycleCandidateParityBundleHash, "")
				}
				if _, disposition, err := terminalizeRunLifecycleCandidateParity(
					fixture, ctx, runID, runtimerunlifecycle.StateCancelled, at.Add(2*time.Minute),
				); err != nil || disposition != runtimerunlifecycle.MutationApplied {
					t.Fatalf("cancel scenario run = %s/%v", disposition, err)
				}
				requireRunOriginHeader(t, ctx, reader, runID, want, 1)
				c4RequireExclusiveStart(t, ctx, fixture.store, runID, runLifecycleCandidateParityBundleHash, "")
			})
		})
	}
}

func decodeMutationProjectionValue(t *testing.T, raw any) any {
	t.Helper()
	var encoded []byte
	switch typed := raw.(type) {
	case nil:
		return nil
	case []byte:
		encoded = typed
	case string:
		encoded = []byte(typed)
	default:
		t.Fatalf("unexpected mutation projection JSON type %T", raw)
	}
	var value any
	if err := json.Unmarshal(encoded, &value); err != nil {
		t.Fatalf("decode mutation projection JSON %q: %v", string(encoded), err)
	}
	return value
}

func TestRunOriginStorageConstraintParity(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		backend := backend
		t.Run(backend, func(t *testing.T) {
			fixture := openRunLifecycleCandidateParityFixture(t, backend)
			ctx := context.Background()
			rooted, _ := sourceartifactfixture.Rooted(t, "fixture", sourceartifactfixture.Artifact())
			eventID := uuid.NewString()
			sourceRunID := uuid.NewString()
			sourceEventID := uuid.NewString()
			cases := []struct {
				name                       string
				kind, eventID, eventType   string
				sourceRunID, sourceEventID string
			}{
				{name: "unknown_kind", kind: "unknown"},
				{name: "event_missing_id", kind: string(runtimerunlifecycle.OriginEvent), eventType: "origin.event"},
				{name: "event_missing_type", kind: string(runtimerunlifecycle.OriginEvent), eventID: eventID},
				{name: "scenario_with_trigger", kind: string(runtimerunlifecycle.OriginScenarioSetup), eventID: eventID, eventType: "origin.event"},
				{name: "deployment_with_trigger", kind: string(runtimerunlifecycle.OriginDeployment), eventID: eventID, eventType: "origin.event"},
				{name: "deployment_with_fork_pair", kind: string(runtimerunlifecycle.OriginDeployment), sourceRunID: sourceRunID, sourceEventID: sourceEventID},
				{name: "retired_standing_origin", kind: "standing_generation"},
				{name: "fork_missing_source_run", kind: string(runtimerunlifecycle.OriginForkMaterialization), sourceEventID: sourceEventID},
				{name: "fork_missing_source_event", kind: string(runtimerunlifecycle.OriginForkMaterialization), sourceRunID: sourceRunID},
				{name: "scenario_with_fork_pair", kind: string(runtimerunlifecycle.OriginScenarioSetup), sourceRunID: sourceRunID, sourceEventID: sourceEventID},
			}
			for _, test := range cases {
				test := test
				t.Run(test.name, func(t *testing.T) {
					err := insertRawRunOrigin(
						ctx, fixture.db, backend, uuid.NewString(), rooted.Admission(),
						test.kind, test.eventID, test.eventType,
						test.sourceRunID, test.sourceEventID,
					)
					if err == nil {
						t.Fatalf("invalid run origin %s was persisted", test.name)
					}
				})
			}

		})
	}
}

func requireRunOriginHeader(
	t *testing.T,
	ctx context.Context,
	reader runOriginReadStore,
	runID string,
	want runtimerunlifecycle.RunOrigin,
	wantEventCount int,
) {
	t.Helper()
	header, err := reader.LoadRunHeader(ctx, runID)
	if err != nil {
		t.Fatalf("load run header %s: %v", runID, err)
	}
	if !header.Origin.Equal(want) || header.EventCount != wantEventCount {
		t.Fatalf("run %s header origin/count = %#v/%d, want %#v/%d", runID, header.Origin, header.EventCount, want, wantEventCount)
	}
}

func requireListedRunOrigin(
	t *testing.T,
	ctx context.Context,
	reader runOriginReadStore,
	runID string,
	want runtimerunlifecycle.RunOrigin,
) {
	t.Helper()
	headers, _, err := reader.ListRunHeaders(ctx, operatorread.RunHeaderListOptions{Limit: 500})
	if err != nil {
		t.Fatalf("list run headers: %v", err)
	}
	for _, header := range headers {
		if header.RunID == runID {
			if !header.Origin.Equal(want) {
				t.Fatalf("listed run %s origin = %#v, want %#v", runID, header.Origin, want)
			}
			return
		}
	}
	t.Fatalf("run %s missing from canonical listing", runID)
}

func insertRawRunOrigin(
	ctx context.Context,
	db *sql.DB,
	backend string,
	runID string,
	root sourceartifact.RootAdmission,
	kind string,
	eventID string,
	eventType string,
	sourceRunID string,
	sourceEventID string,
) error {
	snapshot := runlifecyclefixture.CorruptSnapshot{
		RunID: runID, State: string(runtimerunlifecycle.StateRunning),
		BundleHash: root.BundleHash(), RootDirectory: root.RootDirectory(),
		OriginKind: kind, TriggerEventID: eventID, TriggerEventType: eventType,
		ForkedFromRunID: sourceRunID, ForkedFromEventID: sourceEventID,
		StartedAt: time.Date(2026, 7, 29, 17, 0, 0, 0, time.UTC),
	}
	if backend == "postgres" {
		return storerunlifecycle.AttemptCorruptPostgresSnapshotForTest(ctx, db, snapshot)
	}
	return storerunlifecycle.AttemptCorruptSQLiteSnapshotForTest(ctx, db, snapshot)
}
