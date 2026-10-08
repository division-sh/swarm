package runtimepersistence

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/store/internal/backend/eventrecord"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	"github.com/google/uuid"
)

func TestReviewer2589TerminalSnapshotUUIDAlias(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			fixture := openRunLifecycleCandidateParityFixture(t, backend)
			writer := selectedAdmissionLifecycleWriter(fixture.store)
			ctx := testAuthorActivitySourceArtifactContext()
			runID := "abcdefab-cdef-4abc-8def-abcdefabcdef"
			started := time.Now().UTC().Round(time.Microsecond)
			ensureRunLifecycleCandidateParityRun(t, fixture, ctx, runID, started)
			event := eventtest.RunCreatingRootIngress(uuid.NewString(), "revision.spelling", "gateway", "counter-visibility", []byte(`{"value":1}`), 0, runID, "", events.EventEnvelope{}, started)
			event, err := eventtest.AdmitPayload(event, "", "revision.spelling")
			if err != nil {
				t.Fatal(err)
			}
			admitted, err := events.AdmitForPersistence(event, events.AdmissionOptions{RequirePersistentUUIDIdentity: true})
			if err != nil {
				t.Fatal(err)
			}
			record, err := eventrecord.FromAdmitted(admitted, testRouteSettlement(admitted.Event(), nil))
			if err != nil {
				t.Fatal(err)
			}
			readID := runID
			if fixture.postgres {
				readID = strings.ToUpper(runID)
			}
			err = runSelectedFixtureMutation(ctx, fixture.store, "review counter UUID identity", func(txctx context.Context, attempt *mutationprotocol.Attempt) error {
				inserted, err := insertExactSpellingEvent(txctx, fixture.postgres, attempt, record)
				if err != nil {
					return err
				}
				if !inserted {
					return fmt.Errorf("expected physical event insert")
				}
				snapshot, _, err := writer.MarkTerminalTx(txctx, attempt, runtimerunlifecycle.TerminalRequest{RunID: readID, State: runtimerunlifecycle.StateCancelled, EndedAt: started.Add(time.Second)})
				if err != nil {
					return err
				}
				if snapshot.EventCount != 1 {
					return fmt.Errorf("terminal snapshot returned count=%d for physical event count=1", snapshot.EventCount)
				}
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}
