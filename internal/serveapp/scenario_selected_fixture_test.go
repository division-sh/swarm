package serveapp

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/config"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/servedparity"
	storebackend "github.com/division-sh/swarm/internal/store/backendselection"
	storeselected "github.com/division-sh/swarm/internal/store/selected"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
)

// The retained DSN is a location, not a pool or a reconstructed mutation owner.
// Each serve occurrence constructs and closes its own selected runtime owner.
func installScenarioPostgresRuntime(t *testing.T) {
	t.Helper()
	dsn := testutil.StartEmptyPostgresDSN(t)
	previous := buildStoresForServe
	buildStoresForServe = func(ctx context.Context, selection storebackend.Selection, cfg *config.Config) (*selectedStoreOwner, error) {
		if selection.Backend != storebackend.BackendPostgres {
			return nil, fmt.Errorf("scenario fixture requires the selected PostgreSQL backend")
		}
		return storeselected.OpenRuntime(ctx, storeselected.RuntimeRequest{
			Selection: selection, PostgresDSN: dsn, SessionLockTTL: runtimeSessionLockTTL(cfg),
		})
	}
	t.Cleanup(func() { buildStoresForServe = previous })
}

func installPublicInputPublicationFailure(t *testing.T, selected receiverProofStateReader, rt *runtimepkg.Runtime, cut storetest.PublicationFailureCut) {
	t.Helper()
	fault, err := storetest.NewPublicationFailure(selected, cut, "telegram-chat")
	if err != nil {
		t.Fatal(err)
	}
	ownPublicInputPublicationFailure(t, rt, fault)
}

func ownPublicInputPublicationFailure(t *testing.T, rt *runtimepkg.Runtime, fault *storetest.PublicationFailure) {
	t.Helper()
	t.Cleanup(func() {
		// Runtime admission and every local consumer retire before DDL removal;
		// the enclosing serve process still owns the open selected store here.
		if err := rt.Shutdown(); err != nil {
			t.Error(err)
			return
		}
		if err := fault.RestoreForTest(context.Background()); err != nil {
			t.Error(err)
		}
	})
	if err := fault.InstallForTest(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestPublicInputPublicationFaultRefusesWrongTemplateBothStores(t *testing.T) {
	for _, backend := range servedparity.RequiredBackends {
		for _, stage := range []struct {
			cut    storetest.PublicationFailureCut
			marker string
		}{
			{storetest.PublicationAfterMaterialization, "materialization fault prerequisite: exact lifecycle absent"},
			{storetest.PublicationBeforeReplayScope, "replay fault prerequisite: exact lifecycle absent"},
			{storetest.PublicationBeforeAPICompletion, "API completion fault prerequisite: exact lifecycle absent"},
		} {
			t.Run(fmt.Sprintf("%s/%d", backend, stage.cut), func(t *testing.T) {
				var reader receiverProofStateReader
				captureSelectedRuntimePersistence(t, func(p serveRuntimePersistence) { reader = requireReceiverProofStateReader(t, p.deps.EventStore) })
				endpoint, rt, hash := startPublicInputRollbackRuntime(t, backend)
				fault, err := storetest.NewPublicationFailure(reader, stage.cut, "not-telegram-chat")
				if err != nil {
					t.Fatal(err)
				}
				ownPublicInputPublicationFailure(t, rt, fault)
				failure := requireServedJSONRPCError(t, endpoint, "event.publish", map[string]any{
					"bundle_hash": hash, "event_name": "inbound.telegram.text_message",
					"payload":         map[string]any{"conversation_reference": "42", "conversation_scope": "direct", "external_account_reference": "42", "provider_message_reference": 1, "text": "wrong template witness"},
					"idempotency_key": "wrong-template-proof",
				})
				requirePublicInputPublicationFailureMarker(t, failure, stage.marker)
				event, run := publicInputFailureIdentity(t, failure)
				requirePublicInputRollbackNoResidue(t, reader, backend, event, run)
				if count := servedEventPublishAPIIdempotencyCount(t, reader, servedBackendLabel(backend), "event.publish", "wrong-template-proof"); count != 0 {
					t.Fatalf("wrong-template prerequisite failure left an API receipt: %d", count)
				}
			})
		}
	}
}

func requirePublicInputPublicationFailureMarker(t testing.TB, rpcErr *servedJSONRPCError, marker string) {
	t.Helper()
	// A prerequisite error is a different cut and cannot prove late rollback.
	if !strings.Contains(fmt.Sprint(rpcErr), marker) {
		t.Fatalf("publication did not reach its exact late fault %q: %+v", marker, rpcErr)
	}
}
