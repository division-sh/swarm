package runtimepersistence

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	apiidempotencycontract "github.com/division-sh/swarm/internal/apiidempotency"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/store/internal/apiidempotency"
	"github.com/division-sh/swarm/internal/store/internal/backend/delivery"
	"github.com/division-sh/swarm/internal/store/internal/backend/pipelinepersistence"
	"github.com/google/uuid"
)

func TestPublicationFaultCleanupReconcilesUncertainCommitBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			selected, _, connector := newStopCommitStore(t, backend)
			for _, cut := range []PublicationFailureCut{PublicationAfterMaterialization, PublicationBeforeReplayScope, PublicationBeforeAPICompletion} {
				for _, phase := range []string{"install_committed", "install_rolled_back", "restore_committed", "restore_rolled_back"} {
					t.Run(fmt.Sprintf("%d/%s", cut, phase), func(t *testing.T) {
						provePublicationFaultUncertainty(t, selected, connector, cut, phase)
					})
				}
			}
		})
	}
}

func TestPublicationFaultPrerequisitesRejectForeignEventRunAndTemplateBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			provePublicationFaultForeignPrerequisites(t, backend.open(t).store)
		})
	}
}

func provePublicationFaultForeignPrerequisites(t *testing.T, selected any) {
	t.Helper()
	ctx := testAuthorActivityContext()
	first, second := runForkSnapshotFixtureEvent(t, ctx, selected), runForkSnapshotFixtureEvent(t, ctx, selected)
	for _, event := range []events.Event{first, second} {
		at := time.Now().UTC()
		commitPreparedWorkflowAggregateFixture(t, ctx, selected.(agentFixtureFlowStore), event.RunID(), pipeline.WorkflowInstance{
			InstanceID: "foreign", StorageRef: "telegram-chat/foreign", EntityID: uuid.NewString(), WorkflowName: "telegram-chat", WorkflowVersion: "fixture",
			Mode: "static", StageDefined: true, CurrentState: "ready", Fields: map[string]any{}, CreatedAt: at, EnteredStageAt: at,
		}, at)
	}
	prior := publicationFaultRootEvent()
	if err := commitSemanticEventFixtureWithAgents(ctx, selected, prior, []string{"prior-agent"}); err != nil {
		t.Fatal(err)
	}
	for _, cut := range []PublicationFailureCut{PublicationAfterMaterialization, PublicationBeforeReplayScope} {
		event := publicationFaultRootEvent()
		fault := ownPublicationFailureControl(t, selected, cut, "telegram-chat")
		if err := fault.InstallForTest(ctx); err != nil {
			t.Fatal(err)
		}
		var err error
		marker := "replay fault prerequisite: exact event/delivery absent"
		if cut == PublicationAfterMaterialization {
			err = commitSemanticEventFixtureWithAgents(ctx, selected, event, []string{"new-agent"})
			marker = "materialization fault prerequisite: exact lifecycle absent"
		} else {
			err = commitSemanticEventFixture(ctx, selected, event)
		}
		if err == nil || !strings.Contains(err.Error(), marker) {
			t.Fatalf("foreign-run/template/delivery satisfied exact prerequisite: %v want %q", err, marker)
		}
		if count, err := CountTemplateEventIdentityForTest(ctx, selected, event.ID()); err != nil || count != 0 {
			t.Fatalf("prerequisite refusal retained provisional event: %d %v", count, err)
		}
		if err := fault.RestoreForTest(ctx); err != nil {
			t.Fatal(err)
		}
	}
	proveAPICompletionFaultRejectsForeignDelivery(t, ctx, selected)
}

func publicationFaultRootEvent() events.Event {
	return eventtest.RunCreatingRootIngress(uuid.NewString(), "snapshot.fixture", "fixture", "", []byte(`{}`), 0, uuid.NewString(), "", events.EventEnvelope{}, time.Now().UTC())
}

func ownPublicationFailureControl(t testing.TB, selected any, cut PublicationFailureCut, template string) *PublicationFailure {
	t.Helper()
	fault, err := NewPublicationFailureForTest(selected, cut, template)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := fault.RestoreForTest(context.Background()); err != nil {
			t.Error(err)
		}
	})
	return fault
}

func proveAPICompletionFaultRejectsForeignDelivery(t testing.TB, ctx context.Context, selected any) {
	t.Helper()
	event := publicationFaultRootEvent()
	if err := commitSemanticEventFixture(ctx, selected, event); err != nil {
		t.Fatal(err)
	}
	fault := ownPublicationFailureControl(t, selected, PublicationBeforeAPICompletion, "telegram-chat")
	if err := fault.InstallForTest(ctx); err != nil {
		t.Fatal(err)
	}
	owner := selected.(interface {
		WithAPIIdempotency(context.Context, apiidempotencycontract.Request, func(context.Context) (apiidempotencycontract.Completion, error)) (apiidempotencycontract.Completion, bool, error)
	})
	req := apiidempotencycontract.Request{Method: "event.publish", Actor: apiidempotencycontract.BearerActor("publication-prerequisite"), IdempotencyKey: uuid.NewString(), RequestHash: "exact-publication-fault", ResourceID: event.ID(), Now: time.Now().UTC(), TTL: time.Hour}
	_, _, err := owner.WithAPIIdempotency(ctx, req, func(context.Context) (apiidempotencycontract.Completion, error) {
		return apiidempotencycontract.Completion{ResourceID: event.ID(), Response: []byte(`{}`)}, nil
	})
	if err == nil || !strings.Contains(err.Error(), "API completion fault prerequisite: exact publication absent") {
		t.Fatalf("foreign delivery satisfied API completion prerequisite: %v", err)
	}
	if count, err := ReadAPIIdempotencyCardinalityForTest(ctx, selected, req.Method, req.IdempotencyKey); err != nil || count != 0 {
		t.Fatalf("prerequisite refusal retained API completion: %d %v", count, err)
	}
}

func provePublicationFaultUncertainty(t *testing.T, selected any, connector *stopCommitConnector, cut PublicationFailureCut, phase string) {
	t.Helper()
	fault, err := NewPublicationFailureForTest(selected, cut, "exact-template")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		connector.arm(nil)
		if err := fault.RestoreForTest(context.Background()); err != nil {
			t.Error(err)
		}
	})
	operation := fault.InstallForTest
	if phase == "restore_committed" || phase == "restore_rolled_back" {
		if err := fault.InstallForTest(context.Background()); err != nil {
			t.Fatal(err)
		}
		operation = fault.RestoreForTest
	}
	cause, commits := errors.New("publication fault COMMIT acknowledgment lost"), 0
	connector.arm(func(tx driver.Tx) error {
		commits++
		if phase == "install_rolled_back" || phase == "restore_rolled_back" {
			return errors.Join(cause, tx.Rollback())
		}
		return errors.Join(cause, tx.Commit())
	})
	if err := operation(context.Background()); !errors.Is(err, cause) || commits != 1 {
		t.Fatalf("uncertainty did not reach exactly one native COMMIT: commits=%d err=%v", commits, err)
	}
	want := 0
	if phase == "install_committed" || phase == "restore_rolled_back" {
		want = 1
		if _, postgres := selected.(*PostgresStore); postgres {
			want = 2
		}
	}
	requirePublicationFaultObjects(t, fault, want)
	for range 2 {
		if err := fault.RestoreForTest(context.Background()); err != nil {
			t.Fatal(err)
		}
		requirePublicationFaultObjects(t, fault, 0)
	}
}

func requirePublicationFaultObjects(t testing.TB, fault *PublicationFailure, want int) {
	t.Helper()
	_, postgres := fault.selected.(*PostgresStore)
	got, err := readFanOutSemanticStorage(context.Background(), fault.selected, nil, func(ctx context.Context, tx *sql.Tx) (int, error) {
		switch fault.cut {
		case PublicationAfterMaterialization:
			return delivery.CountPublicationMaterializationFaultObjectsTx(ctx, tx, postgres, fault.id)
		case PublicationBeforeReplayScope:
			return pipelinepersistence.CountPublicationReplayFaultObjectsTx(ctx, tx, postgres, fault.id)
		case PublicationBeforeAPICompletion:
			return apiidempotency.CountPublicationCompletionFaultObjectsTx(ctx, tx, postgres, fault.id)
		default:
			return 0, errors.New("unknown publication fault")
		}
	})
	if err != nil || got != want {
		t.Fatalf("publication fault objects=%d want=%d: %v", got, want, err)
	}
}

func TestPublicationFaultRefusesCancellationForeignOwnerAndInvalidCutBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			provePublicationFaultRefusals(t, backend.open(t).store)
		})
	}
}

func provePublicationFaultRefusals(t *testing.T, selected any) {
	t.Helper()
	if _, err := NewPublicationFailureForTest(&sql.DB{}, PublicationBeforeReplayScope, "consumer"); err == nil {
		t.Fatal("raw foreign owner acquired a fault")
	}
	for _, cut := range []PublicationFailureCut{0, 99} {
		if _, err := NewPublicationFailureForTest(selected, cut, "consumer"); err == nil {
			t.Fatal("unnamed temporal cut acquired a fault")
		}
	}
	fault, err := NewPublicationFailureForTest(selected, PublicationBeforeReplayScope, "consumer")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := fault.RestoreForTest(context.Background()); err != nil {
			t.Error(err)
		}
	})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := fault.InstallForTest(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled installation mutated: %v", err)
	}
	requirePublicationFaultObjects(t, fault, 0)
	if err := fault.RestoreForTest(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := fault.InstallForTest(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := fault.RestoreForTest(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled cleanup discarded responsibility: %v", err)
	}
	if err := fault.RestoreForTest(context.Background()); err != nil {
		t.Fatal(err)
	}
	requirePublicationFaultObjects(t, fault, 0)
	closed := ownPublicationFailureControlForClosedOwner(t, selected)
	if err := closed.InstallForTest(context.Background()); err == nil {
		t.Fatal("closed original owner installed a publication fault")
	}
}

func ownPublicationFailureControlForClosedOwner(t testing.TB, selected any) *PublicationFailure {
	t.Helper()
	fault, err := NewPublicationFailureForTest(selected, PublicationBeforeReplayScope, "consumer")
	if err != nil {
		t.Fatal(err)
	}
	switch owner := selected.(type) {
	case *PostgresStore:
		err = owner.backend.Close()
	case *SQLiteRuntimeStore:
		err = owner.backend.Close()
	}
	if err != nil {
		t.Fatal(err)
	}
	return fault
}
