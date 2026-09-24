package runtimepersistence

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/pipelineobligation"
)

func TestFanOutPublicationGroupPreparationFailureBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for position := 0; position < 3; position++ {
			for _, phase := range []string{"claim", "plan", "seal", "semantic", "untyped-payload"} {
				t.Run(fmt.Sprintf("%s/member%d/%s", backend, position, phase), func(t *testing.T) {
					f := newGroupProofFixture(t, backend, 3)
					before := f.snapshot(t)
					var requests []pipeline.FanOutPublicationRequest
					for ordinal, event := range f.events {
						requests = append(requests, pipeline.FanOutPublicationRequest{Ordinal: ordinal, Intent: engine.EmitIntent{Event: event}})
					}
					var competing pipelineobligation.Claim
					if phase == "claim" {
						ordered := append([]events.Event{}, f.events...)
						sort.Slice(ordered, func(i, j int) bool { return ordered[i].ID() < ordered[j].ID() })
						var err error
						competing, err = f.store().ClaimPublication(f.ctx, ordered[position].ID())
						if err != nil {
							t.Fatal(err)
						}
					}
					if phase == "untyped-payload" {
						e := f.events[position]
						bad := eventtest.ChildForProducerWithRoutingSource(
							e.ID(), e.Type(), e.Producer(), e.TaskID(), []byte(`[]`), e.ChainDepth(),
							f.intent.Request.Capsule.Lineage, e.Envelope(), e.RoutingSource(), e.CreatedAt(),
						)
						requests[position].Intent.Event = bad
					}
					var fault error = errors.New("selected publication preparation read failure")
					if phase == "semantic" {
						fault = &engine.EmitPayloadContractError{Event: string(f.events[position].Type()), Kind: engine.EmitPayloadSchemaMismatch, Path: "$.value", Constraint: "type", Expected: "string", Actual: "number", Detail: "injected canonical planner semantic rejection"}
					}
					seen := 0
					if phase == "plan" || phase == "semantic" {
						f.probe.set(func(at, q string) error {
							if at == "before_query" && strings.Contains(strings.ToLower(q), "from events") {
								seen++
								if seen == position+1 {
									return fault
								}
							}
							return nil
						})
					}
					prepared, err := f.bus.PrepareFanOutPublications(f.ctx, f.group, requests)
					f.probe.set(nil)
					if phase == "claim" {
						if !errors.Is(err, pipelineobligation.ErrBusy) || len(prepared) != 0 {
							t.Fatalf("claim failure returned partial plans: %+v %v", prepared, err)
						}
						if err := f.store().Release(f.ctx, competing); err != nil {
							t.Fatalf("batch stole competing claim: %v", err)
						}
					} else {
						if err != nil || len(prepared) != 3 {
							t.Fatalf("prepare=%+v %v", prepared, err)
						}
						var claims []pipelineobligation.Claim
						var plans []engine.DurablePublicationPlan
						for i, row := range prepared {
							if (phase == "plan" || phase == "semantic" || phase == "untyped-payload") && i == position {
								if row.Err == nil || row.Publication != nil {
									t.Fatalf("failed row%d=%+v", i, row)
								}
							} else if row.Err != nil || row.Publication == nil {
								t.Fatalf("healthy row%d=%+v", i, row)
							}
							if row.Publication != nil {
								plans = append(plans, row.Publication)
								claims = append(claims, row.Publication.(bus.EnginePublicationPlan).PublicationCommand().Commit.PipelineClaim)
							}
						}
						if phase == "plan" && seen < position+1 {
							t.Fatal("plan fault did not run")
						}
						if phase == "untyped-payload" && engine.IsEmitPayloadContractFailure(prepared[position].Err) {
							t.Fatal("raw bus payload error became semantic rejection")
						}
						if phase == "seal" {
							foreign, _ := pipelineobligation.NewClaimIssuer().Issue(claims[position].EventID(), pipelineobligation.PurposePublication)
							bad := append([]pipelineobligation.Claim{}, claims...)
							bad[position] = foreign
							if err := f.group.Seal(f.ctx, 3, bad); err == nil {
								t.Fatal("foreign member sealed")
							}
						}
						if phase == "semantic" {
							if err := f.group.Seal(f.ctx, 3, claims); err != nil {
								t.Fatal(err)
							}
							for i, row := range prepared {
								outcome := pipeline.FanOutChunkOutcome{Ordinal: i, Publication: row.Publication}
								if row.Err != nil {
									failure := engine.NormalizeFailure(row.Err, "runtime.fan_out", "issue_ordinal")
									if failure == nil || !engine.IsEmitPayloadContractFailure(row.Err) {
										t.Fatalf("semantic failure untyped: %v", row.Err)
									}
									outcome.Failure, err = json.Marshal(failure.Failure)
									if err != nil {
										t.Fatal(err)
									}
								}
								f.command.Outcomes = append(f.command.Outcomes, outcome)
							}
							f.commit(t)
							assertFanOutCursorAndOutcomeCount(t, f.ctx, f.db, f.seed, 3, 3)
							var n int
							if err := f.db.QueryRow(`SELECT COUNT(*) FROM events WHERE event_id=$1`, f.events[position].ID()).Scan(&n); err != nil || n != 0 {
								t.Fatalf("semantic rejection fabricated event: %d %v", n, err)
							}
						} else {
							if err := f.bus.ReleaseEnginePublications(context.Background(), plans); err != nil {
								t.Fatal(err)
							}
						}
					}
					if err := f.group.Close(context.Background()); err != nil {
						t.Fatal(err)
					}
					if phase != "semantic" {
						f.unchanged(t, before)
					}
					for _, event := range f.events {
						claim, err := f.store().ClaimPublication(f.ctx, event.ID())
						if err != nil {
							t.Fatalf("preparation left claim: %v", err)
						}
						if err := f.store().Release(f.ctx, claim); err != nil {
							t.Fatal(err)
						}
					}
				})
			}
		}
	}
}

func TestFanOutPublicationGroupExactDuplicateAppendBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newGroupProofFixture(t, backend, 1)
			// Existing exact publication has no other ordinal owner. Reusing an
			// event already assigned to another ordinal is separately forbidden.
			if err := f.bus.Publish(f.ctx, f.events[0]); err != nil {
				t.Fatal(err)
			}
			before := f.snapshot(t)
			f.prepare(t)
			f.seal(t)
			committed, err := f.owner.CommitFanOutChunk(f.ctx, f.command)
			if err != nil {
				t.Fatal(err)
			}
			if len(committed.Publications) != 1 || committed.Publications[0].(bus.CommittedEnginePublication).NewlyInserted() {
				t.Fatal("exact duplicate reported new occurrence")
			}
			if err := f.bus.FinalizeFanOutPublications(f.ctx, f.group, committed.Publications); err != nil {
				t.Fatal(err)
			}
			if err := f.bus.DispatchFanOutPublications(f.ctx, f.group, committed.Publications); err != nil {
				t.Fatal(err)
			}
			if err := f.group.Close(f.ctx); err != nil {
				t.Fatal(err)
			}
			after := f.snapshot(t)
			for _, table := range []string{"events", "event_deliveries", "event_receipts", "event_delivery_attempts"} {
				if !reflect.DeepEqual(before[table], after[table]) {
					t.Fatalf("duplicate changed %s", table)
				}
			}
			if _, err := f.owner.CommitFanOutChunk(f.ctx, f.command); err == nil {
				t.Fatal("consumed duplicate command succeeded twice")
			}
			f.unchanged(t, after)
		})
	}
}
