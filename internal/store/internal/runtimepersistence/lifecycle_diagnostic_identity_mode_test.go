package runtimepersistence

import (
	"context"
	"testing"

	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/google/uuid"
)

func TestLifecycleDiagnosticSettlementIdentityAndModeOnBothStores(t *testing.T) {
	for _, sqlite := range []bool{true, false} {
		name := "postgres"
		if sqlite {
			name = "sqlite"
		}
		t.Run(name, func(t *testing.T) {
			for _, firstLive := range []bool{true, false} {
				mode, first, retry, origin := "live", executionposture.Live, executionposture.MockOnly, executionmode.Mock
				if !firstLive {
					mode, first, retry, origin = "mock", executionposture.MockOnly, executionposture.Live, executionmode.Live
				}
				t.Run(mode, func(t *testing.T) {
					store, db := newLifecycleDiagnosticTestStore(t, sqlite)
					item := enqueueDiagnosticTestIdentity(t, store, testAgentIdentity(t, "mode-worker", "global"), runtimemanager.LifecycleDiagnosticOrigin{
						Owner: runtimemanager.LifecycleDiagnosticNormal, Causality: runtimemanager.LifecycleDiagnosticObservation,
					}, origin)
					if err := runtimepkg.NewRuntimeLogger(store, first, nil).ProjectLifecycleDiagnostic(context.Background(), item); err != nil {
						t.Fatal(err)
					}
					wantID := uuid.NewSHA1(uuid.NameSpaceOID, []byte("swarm:lifecycle-diagnostic:"+item.OutboxID)).String()
					var eventID, eventMode, originMode, receipt string
					if err := db.QueryRow(`SELECT execution_mode, projection FROM agent_lifecycle_diagnostic_outbox WHERE outbox_id=$1 AND projected_at IS NOT NULL`, item.OutboxID).Scan(&originMode, &receipt); err != nil {
						t.Fatal(err)
					}
					if err := db.QueryRow(`SELECT event_id, execution_mode FROM events WHERE event_id=$1`, wantID).Scan(&eventID, &eventMode); err != nil {
						t.Fatal(err)
					}
					if originMode == mode || eventMode != mode || eventID == item.OutboxID {
						t.Fatalf("origin=%s event=%s eventID=%s outboxID=%s", originMode, eventMode, eventID, item.OutboxID)
					}
					if err := runtimepkg.NewRuntimeLogger(store, retry, nil).ProjectLifecycleDiagnostic(context.Background(), item); err != nil {
						t.Fatal(err)
					}
					var count int
					if err := db.QueryRow(`SELECT COUNT(*) FROM events WHERE event_id=$1 AND execution_mode=$2`, wantID, mode).Scan(&count); err != nil || count != 1 {
						t.Fatalf("retry changed settled event: count=%d err=%v", count, err)
					}
					var after string
					if err := db.QueryRow(`SELECT projection FROM agent_lifecycle_diagnostic_outbox WHERE outbox_id=$1`, item.OutboxID).Scan(&after); err != nil || after != receipt {
						t.Fatalf("retry changed receipt: err=%v", err)
					}
					if err := db.QueryRow(`SELECT COUNT(*) FROM events WHERE event_id=$1`, item.OutboxID).Scan(&count); err != nil || count != 0 {
						t.Fatalf("outbox occurrence used as event identity: count=%d err=%v", count, err)
					}
				})
			}
		})
	}
}
