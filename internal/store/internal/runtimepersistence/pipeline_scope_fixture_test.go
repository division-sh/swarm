package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/store/internal/backend/pipelinepersistence"
	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
	"github.com/google/uuid"
)

func TestPipelineScopeFixturePreservesExactFaultReceiptAndOriginalOwnersBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture, ctx := backend.open(t), testAuthorActivityContext()
			selected := fixture.store.(interface {
				semanticEventFixtureStore
				preparedPublishEventReadbackStore
				PipelineObligations() runtimepipelineobligation.Store
			})
			event := eventtest.RunCreatingRootIngress(uuid.NewString(), "scope.fixture.target", "runtime", "", []byte(`{}`), 0, uuid.NewString(), "", events.EventEnvelope{}, time.Now().UTC())
			sibling := eventtest.RunCreatingRootIngress(uuid.NewString(), "scope.fixture.sibling", "runtime", "", []byte(`{}`), 0, uuid.NewString(), "", events.EventEnvelope{}, event.CreatedAt())
			for _, item := range []events.Event{event, sibling} {
				if err := commitSemanticEventFixture(ctx, selected, item); err != nil {
					t.Fatal(err)
				}
			}
			settlePipelineParityEvent(t, ctx, selected.PipelineObligations(), event.ID(), runtimepipelineobligation.Acknowledged("scope_fixture"))
			before, found, err := selected.LoadPreparedPublishEvent(ctx, sibling.ID())
			if err != nil || !found {
				t.Fatalf("prepare sibling scope: found=%t,err=%v", found, err)
			}
			beforeScope, err := readPipelineScopeFixture(t, ctx, selected, sibling.ID())
			if err != nil {
				t.Fatal(err)
			}
			probe, restore, err := InstallTransactionProbeForTest(selected, transactiontest.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer restore()
			want := ExactPipelineReceiptOutcomeReason{Outcome: "success", Reason: "scope_fixture"}
			if out, err := ReadExactPipelineReceiptOutcomeReasonForTest(ctx, selected, event.ID()); err != nil || out != want {
				t.Fatalf("exact receipt=%+v,err=%v", out, err)
			}
			if err := DeleteCommittedReplayScopeForTest(ctx, selected, event.ID()); err != nil {
				t.Fatal(err)
			}
			if counts := probe.Snapshot(); counts.Total.WriteCommits != 1 || counts.Total.ReadCommits != 1 || counts.Active != 0 {
				t.Fatalf("scope fault/receipt escaped original coordinator: %+v", counts)
			}
			if _, err := readPipelineScopeFixture(t, ctx, selected, event.ID()); !errors.Is(err, runtimepipelineobligation.ErrMissingScope) {
				t.Fatalf("scope fault did not reach canonical missing-scope readback: %v", err)
			}
			if id, err := ReadLatestNamedEventIdentityStorageForTest(ctx, selected, string(event.Type()), ""); err != nil || id != event.ID() {
				t.Fatalf("scope fault destroyed original event: %q,%v", id, err)
			}
			if out, err := ReadExactPipelineReceiptOutcomeReasonForTest(ctx, selected, event.ID()); err != nil || out != want {
				t.Fatalf("scope fault changed receipt: %+v,%v", out, err)
			}
			if err := DeleteCommittedReplayScopeForTest(ctx, selected, uuid.NewString()); err == nil {
				t.Fatal("wrong event accepted a scope fault")
			}
			cancelled, cancel := context.WithCancel(ctx)
			cancel()
			if err := DeleteCommittedReplayScopeForTest(cancelled, selected, sibling.ID()); !errors.Is(err, context.Canceled) {
				t.Fatalf("cancelled scope fault=%v", err)
			}
			if out, err := ReadExactPipelineReceiptOutcomeReasonForTest(cancelled, selected, event.ID()); !errors.Is(err, context.Canceled) || out != (ExactPipelineReceiptOutcomeReason{}) {
				t.Fatalf("cancelled receipt returned evidence: %+v,%v", out, err)
			}
			after, found, err := selected.LoadPreparedPublishEvent(ctx, sibling.ID())
			if err != nil || !found || !reflect.DeepEqual(before, after) {
				t.Fatalf("rejected faults changed sibling scope: %+v,found=%t,err=%v", after, found, err)
			}
			if afterScope, err := readPipelineScopeFixture(t, ctx, selected, sibling.ID()); err != nil || afterScope != beforeScope {
				t.Fatalf("rejected fault changed exact sibling scope: %q -> %q,%v", beforeScope, afterScope, err)
			}
			switch owner := selected.(type) {
			case *PostgresStore:
				if err := owner.Close(); err != nil {
					t.Fatal(err)
				}
			case *SQLiteRuntimeStore:
				if err := owner.Close(); err != nil {
					t.Fatal(err)
				}
			}
			if err := DeleteCommittedReplayScopeForTest(ctx, selected, sibling.ID()); err == nil {
				t.Fatal("closed owner accepted scope fault")
			}
			if out, err := ReadExactPipelineReceiptOutcomeReasonForTest(ctx, selected, event.ID()); err == nil || out != (ExactPipelineReceiptOutcomeReason{}) {
				t.Fatalf("closed receipt returned evidence: %+v,%v", out, err)
			}
		})
	}
	for _, invalid := range []any{nil, (*PostgresStore)(nil), (*SQLiteRuntimeStore)(nil), struct{}{}} {
		if err := DeleteCommittedReplayScopeForTest(context.Background(), invalid, uuid.NewString()); err == nil {
			t.Fatal("missing owner accepted scope fault")
		}
		if out, err := ReadExactPipelineReceiptOutcomeReasonForTest(context.Background(), invalid, uuid.NewString()); err == nil || out != (ExactPipelineReceiptOutcomeReason{}) {
			t.Fatalf("missing owner returned receipt: %+v,%v", out, err)
		}
	}
}

func readPipelineScopeFixture(t *testing.T, ctx context.Context, selected any, eventID string) (runtimepipelineobligation.CommittedScope, error) {
	t.Helper()
	_, postgres := selected.(*PostgresStore)
	var scope runtimepipelineobligation.CommittedScope
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		scope, err = pipelinepersistence.LoadCommittedScope(ctx, tx, eventID, postgres)
		return err
	})
	return scope, err
}
