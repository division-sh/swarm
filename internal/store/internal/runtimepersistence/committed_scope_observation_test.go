package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
	"github.com/google/uuid"
)

func TestCommittedPipelineScopeObservationUsesOriginalOwnerBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			selected, _, _ := selectedForkDiscardTestStore(t, backend)
			ctx := testAuthorActivityContext()
			for _, expected := range []runtimepipelineobligation.CommittedScope{runtimepipelineobligation.ScopeDirect, runtimepipelineobligation.ScopeSubscribed} {
				t.Run(string(expected), func(t *testing.T) {
					runID, at := uuid.NewString(), time.Now().UTC()
					requireRunningRunForTest(t, ctx, selected, runID, at.Add(-time.Second))
					event := eventtest.ExistingRunRootIngress(uuid.NewString(), "fixture.scope", "api.v1", "", []byte(`{}`), 0, runID, events.EventEnvelope{}, at)
					var agents []string
					if expected == runtimepipelineobligation.ScopeSubscribed {
						agents = []string{"scope-agent"}
					}
					if err := commitSemanticEventFixtureWithAgents(ctx, selected, event, agents); err != nil {
						t.Fatal(err)
					}
					probe, restore, err := InstallTransactionProbeForTest(selected, transactiontest.Options{})
					if err != nil {
						t.Fatal(err)
					}
					defer restore()
					scope, err := ReadCommittedPipelineScopeForTest(ctx, selected, event.ID())
					if err != nil || scope != expected {
						t.Fatalf("actual committed scope=%q want=%q err=%v", scope, expected, err)
					}
					if counts := probe.Snapshot(); counts.Total.Begun != 1 || counts.Total.ReadCommits != 1 || counts.Total.WriteCommits != 0 || counts.Active != 0 {
						t.Fatalf("scope escaped original read coordinator: %+v", counts)
					}
				})
			}
			if scope, err := ReadCommittedPipelineScopeForTest(ctx, selected, uuid.NewString()); !errors.Is(err, runtimepipelineobligation.ErrMissingScope) || scope != "" {
				t.Fatalf("missing scope granted evidence: %q %v", scope, err)
			}
		})
	}
}

func TestCommittedPipelineScopeObservationRefusesInvalidOwnersBothStores(t *testing.T) {
	ctx := context.Background()
	for _, selected := range []any{nil, (*PostgresStore)(nil), (*SQLiteRuntimeStore)(nil), &PostgresStore{}, &SQLiteRuntimeStore{}, &sql.DB{}, &sql.Tx{}} {
		if scope, err := ReadCommittedPipelineScopeForTest(ctx, selected, uuid.NewString()); err == nil || scope != "" {
			t.Fatalf("foreign scope observer granted evidence: %q %v", scope, err)
		}
	}
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			selected, _, _ := selectedForkDiscardTestStore(t, backend)
			if scope, err := ReadCommittedPipelineScopeForTest(ctx, selected, "invalid"); err == nil || scope != "" {
				t.Fatalf("invalid event granted scope: %q %v", scope, err)
			}
			canceled, cancel := context.WithCancel(ctx)
			cancel()
			if scope, err := ReadCommittedPipelineScopeForTest(canceled, selected, uuid.NewString()); !errors.Is(err, context.Canceled) || scope != "" {
				t.Fatalf("canceled read granted scope: %q %v", scope, err)
			}
			if err := selected.(interface{ Close() error }).Close(); err != nil {
				t.Fatal(err)
			}
			if scope, err := ReadCommittedPipelineScopeForTest(ctx, selected, uuid.NewString()); err == nil || scope != "" {
				t.Fatalf("closed reader granted scope: %q %v", scope, err)
			}
		})
	}
}
