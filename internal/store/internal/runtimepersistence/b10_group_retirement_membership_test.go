package runtimepersistence

import (
	"context"
	"errors"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/pipelineobligation"
)

// A static-membership failure must not return retirement authority over any
// valid prefix. Prove callbacks survive by consuming the real grouped path,
// not by accepting fallback recovery or merely observing unchanged SQL rows.
func TestB10HostileGroupInputCannotRetireValidCallbacks(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, operation := range []string{"finalize", "dispatch"} {
			for _, attack := range []string{"wrong_group", "foreign_member", "foreign_bus_value", "noncanonical_value"} {
				t.Run(backend+"/"+operation+"/"+attack, func(t *testing.T) {
					f := newGroupProofFixture(t, backend, 2)
					other := newGroupProofFixtureOn(t, backend, 2, f)
					if attack != "foreign_bus_value" {
						other.bus = f.bus
					}
					values := stageB10RetirementGroup(t, f)
					otherValues := stageB10RetirementGroup(t, other)
					probe := &b12DeferredInterceptor{calls: map[string]int{}}
					f.bus.SetInterceptors(probe)
					other.bus.SetInterceptors(probe)
					before := f.snapshot(t)
					badValues := append([]engine.CommittedDurablePublication(nil), values...)
					badGroup := f.group
					switch attack {
					case "wrong_group":
						badGroup = other.group
					case "foreign_member", "foreign_bus_value":
						badValues[1] = otherValues[1]
					case "noncanonical_value":
						badValues[1] = nil
					}
					for attempt := 0; attempt < 2; attempt++ {
						var err error
						if operation == "finalize" {
							err = f.bus.FinalizeFanOutPublications(f.ctx, badGroup, badValues)
						} else {
							err = f.bus.DispatchFanOutPublications(f.ctx, badGroup, badValues)
						}
						if err == nil {
							t.Fatal("hostile group/value input was accepted")
						}
						if len(probe.calls) != 0 {
							t.Fatalf("hostile input executed callbacks: %v", probe.calls)
						}
						f.unchanged(t, before)
					}
					requireB10SurvivingGroupedCallbacks(t, f, values, probe)
					requireB10SurvivingGroupedCallbacks(t, other, otherValues, probe)
					for _, current := range []*groupProofFixture{f, other} {
						if err := current.group.Close(context.Background()); err != nil {
							t.Fatal(err)
						}
					}
					if err := f.bus.ResetInMemoryState(); err != nil {
						t.Fatal(err)
					}
					if other.bus != f.bus {
						if err := other.bus.ResetInMemoryState(); err != nil {
							t.Fatal(err)
						}
					}
				})
			}
		}
	}
}

func TestB10FinalizeEarlyFailureRetiresExactSiblingsOnly(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newGroupProofFixture(t, backend, 2)
			other := newGroupProofFixtureOn(t, backend, 2, f)
			other.bus = f.bus
			values := stageB10RetirementGroup(t, f)
			otherValues := stageB10RetirementGroup(t, other)
			probe := &b12DeferredInterceptor{calls: map[string]int{}}
			f.bus.SetInterceptors(probe)
			before := f.snapshot(t)
			ctx, cancel := context.WithCancel(f.ctx)
			cancel()
			if err := f.bus.FinalizeFanOutPublications(ctx, f.group, values); !errors.Is(err, context.Canceled) {
				t.Fatalf("did not reach exact-membership/dynamic-validation failure: %v", err)
			}
			if err := f.group.Close(context.Background()); err != nil {
				t.Fatal(err)
			}
			// Static cleanup remains valid after close and is idempotent. It
			// must never extend to another live group on the very same bus.
			if err := f.bus.FinalizeFanOutPublications(f.ctx, f.group, values); err == nil {
				t.Fatal("closed group regained finalization authority")
			}
			f.unchanged(t, before)
			if len(probe.calls) != 0 {
				t.Fatalf("finalization executed callbacks: %v", probe.calls)
			}
			requireB10SurvivingGroupedCallbacks(t, other, otherValues, probe)
			if err := other.group.Close(context.Background()); err != nil {
				t.Fatal(err)
			}
			if err := f.bus.ResetInMemoryState(); err != nil {
				t.Fatalf("early finalization retained closed sibling callbacks: %v", err)
			}
			if err := f.bus.ResetInMemoryState(); err != nil {
				t.Fatal(err)
			}
			for _, event := range f.events {
				if probe.calls[event.ID()] != 0 {
					t.Fatalf("failed group's callback executed: %s", event.ID())
				}
				var receipts int
				if err := f.db.QueryRow(`SELECT COUNT(*) FROM event_receipts WHERE event_id=$1`, event.ID()).Scan(&receipts); err != nil || receipts != 0 {
					t.Fatalf("retirement minted receipt: count=%d err=%v", receipts, err)
				}
				work, err := f.store().ClaimEvent(f.ctx, event.ID(), pipelineobligation.PurposeRecovery)
				if err != nil || work.Claim.EventID() != event.ID() {
					t.Fatalf("sibling durable recovery lost: work=%+v err=%v", work, err)
				}
				if err := f.store().Release(f.ctx, work.Claim); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func stageB10RetirementGroup(t *testing.T, f *groupProofFixture) []engine.CommittedDurablePublication {
	t.Helper()
	f.prepare(t)
	f.seal(t)
	committed, err := f.owner.CommitFanOutChunk(f.ctx, f.command)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.bus.FinalizeFanOutPublications(f.ctx, f.group, committed.Publications); err != nil {
		t.Fatal(err)
	}
	return committed.Publications
}

func requireB10SurvivingGroupedCallbacks(t *testing.T, f *groupProofFixture, values []engine.CommittedDurablePublication, probe *b12DeferredInterceptor) {
	t.Helper()
	beforeRevision := countP16RunRevisions(t, f.db, f.seed.runID)
	observed := &b12ObservedGroup{PublicationGroup: f.group}
	if err := f.bus.DispatchFanOutPublications(f.ctx, observed, values); err != nil {
		t.Fatalf("valid queued callbacks did not survive hostile/foreign cleanup: %v", err)
	}
	if observed.settles != 1 || observed.reads != 0 || len(observed.outcome.Results) != 2 {
		t.Fatalf("callbacks disappeared into fallback/split settlement: %+v", observed)
	}
	for i, result := range observed.outcome.Results {
		if result.Claim != f.claims[i] || !result.Outcome.Committed() || !result.Outcome.DeliveryHandoffCommitted() {
			t.Fatalf("valid callback lost exact committed/handoff evidence: %+v", result)
		}
		if probe.calls[result.Claim.EventID()] != 1 {
			t.Fatalf("callback not executed exactly once: %v", probe.calls)
		}
		assertDirectiveReceipt(t, f.db, result.Claim.EventID(), "processed", nil)
	}
	if got := countP16RunRevisions(t, f.db, f.seed.runID); got != beforeRevision+1 {
		t.Fatalf("surviving group minted split/missing revision: before=%d after=%d", beforeRevision, got)
	}
}
