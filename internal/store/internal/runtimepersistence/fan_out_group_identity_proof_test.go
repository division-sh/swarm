package runtimepersistence

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
	"github.com/google/uuid"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/pipelineobligation"
)

func TestFanOutPublicationGroupSealedRangeIdentityBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, count := range []int{1, 32} {
			t.Run(fmt.Sprintf("%s/cap%d", backend, count), func(t *testing.T) {
				f := newGroupProofFixture(t, backend, count)
				before := f.snapshot(t)
				base := pipelineobligation.PublicationClaimRequest{Ordinal: 0, Event: f.events[0]}
				cases := [][]pipelineobligation.PublicationClaimRequest{{base, base}, {{Ordinal: -1, Event: f.events[0]}}, {{Ordinal: 32, Event: f.events[0]}}}
				if count > 1 {
					cases = append(cases, []pipelineobligation.PublicationClaimRequest{base, {Ordinal: 0, Event: f.events[1]}}, []pipelineobligation.PublicationClaimRequest{base, {Ordinal: 1, Event: f.events[0]}})
				}
				tooMany := make([]pipelineobligation.PublicationClaimRequest, 33)
				for i := range tooMany {
					tooMany[i] = base
				}
				cases = append(cases, tooMany)
				for _, requests := range cases {
					if _, err := f.group.ClaimBatch(f.ctx, requests); err == nil {
						t.Fatal("malformed claim range accepted")
					}
					f.unchanged(t, before)
				}
				if claims, err := f.group.ClaimBatch(f.ctx, nil); err != nil || len(claims) != 0 {
					t.Fatalf("empty batch=%v %v", claims, err)
				}
				f.prepare(t)
				for _, claims := range [][]pipelineobligation.Claim{nil, append(append([]pipelineobligation.Claim{}, f.claims...), f.claims[0])} {
					if err := f.group.Seal(f.ctx, count, claims); err == nil {
						t.Fatal("omitted or duplicate sealed token admitted")
					}
					f.unchanged(t, before)
				}
				f.seal(t)
				if _, err := f.group.ClaimBatch(f.ctx, []pipelineobligation.PublicationClaimRequest{base}); err == nil {
					t.Fatal("post-seal acquisition")
				}
				wrong := f.command
				wrong.Outcomes = append([]pipeline.FanOutChunkOutcome{}, wrong.Outcomes...)
				wrong.Outcomes[0].Ordinal++
				if _, err := f.owner.CommitFanOutChunk(f.ctx, wrong); err == nil {
					t.Fatal("wrong contiguous range committed")
				}
				f.unchanged(t, before)
				wrong = f.command
				wrong.Outcomes = wrong.Outcomes[:len(wrong.Outcomes)-1]
				if _, err := f.owner.CommitFanOutChunk(f.ctx, wrong); err == nil {
					t.Fatal("wrong end committed")
				}
				f.unchanged(t, before)
				f.commit(t)
				if err := f.group.ValidateCommitted(f.ctx, f.claims); err != nil {
					t.Fatal(err)
				}
				out, err := f.group.Settle(f.ctx, f.members())
				if err != nil || len(out.Results) != count {
					t.Fatalf("full range settlement=%+v err=%v", out, err)
				}
			})
		}
		t.Run(backend+"/empty-accepted", func(t *testing.T) {
			f := newGroupProofFixture(t, backend, 3)
			if err := f.group.Seal(f.ctx, 3, nil); err != nil {
				t.Fatal(err)
			}
			f.command = rejectedFanOutChunk(f.claim, 0, 3, f.command.Now)
			f.commit(t)
			assertFanOutCursorAndOutcomeCount(t, f.ctx, f.db, f.seed, 3, 3)
			var n int
			if err := f.db.QueryRow(`SELECT COUNT(*) FROM events WHERE run_id=$1`, f.seed.runID).Scan(&n); err != nil || n != 1 {
				t.Fatalf("rejections invented publications: %d %v", n, err)
			}
			if err := f.group.ValidateCommitted(f.ctx, nil); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestFanOutPublicationGroupForeignClaimSetBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newGroupProofFixture(t, backend, 2)
			f.prepare(t)
			f.seal(t)
			f.commit(t)
			before := f.snapshot(t)
			for _, purpose := range []pipelineobligation.Purpose{pipelineobligation.PurposePublication, pipelineobligation.PurposeRecovery, pipelineobligation.PurposeDecisionRoute} {
				foreign, err := pipelineobligation.NewClaimIssuer().Issue(f.claims[0].EventID(), purpose)
				if err != nil {
					t.Fatal(err)
				}
				mixed := f.members()
				mixed[0].Claim = foreign
				if _, err := f.group.Settle(f.ctx, mixed); err == nil {
					t.Fatal("foreign issuer/purpose with same event ID settled")
				}
				if err := f.group.ValidateCommitted(f.ctx, []pipelineobligation.Claim{foreign, f.claims[1]}); err == nil {
					t.Fatal("foreign issuer authorized effects")
				}
				f.unchanged(t, before)
			}
			if err := f.store().Release(f.ctx, f.claims[0]); err != nil {
				t.Fatal(err)
			}
			successor, err := f.store().ClaimPublication(f.ctx, f.claims[0].EventID())
			if err != nil {
				t.Fatal(err)
			}
			defer f.store().Release(context.Background(), successor)
			mixed := f.members()
			mixed[0].Claim = successor
			if _, err := f.group.Settle(f.ctx, mixed); err == nil {
				t.Fatal("independently issued current claim entered group")
			}
			if _, err := f.group.Settle(f.ctx, f.members()); err == nil {
				t.Fatal("retired group token settled successor")
			}
			f.unchanged(t, before)
			if err := f.group.Close(f.ctx); err != nil {
				t.Fatal(err)
			}
			if err := f.store().Release(f.ctx, successor); err != nil {
				t.Fatalf("group cleanup stole successor: %v", err)
			}
		})
	}
}

func TestFanOutPublicationGroupSafeRollbackBisectionBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for position := 1; position <= 4; position++ {
			t.Run(fmt.Sprintf("%s/member%d", backend, position), func(t *testing.T) {
				f := newGroupProofFixture(t, backend, 4)
				f.prepare(t)
				f.seal(t)
				before := f.snapshot(t)
				var failure failures.Envelope
				if err := json.Unmarshal(rejectedFanOutChunk(f.claim, 0, 1, f.command.Now).Outcomes[0].Failure, &failure); err != nil {
					t.Fatal(err)
				}
				cause := pipeline.NewFanOutSafeAggregateError(failure, errors.New("selected SQL publication fault"))
				seen := 0
				f.probe.set(func(phase, q string) error {
					if (phase == "after_exec" || phase == "after_query_row") && strings.Contains(strings.ToLower(q), "insert into events") {
						seen++
						if seen == position {
							return cause
						}
					}
					return nil
				})
				_, err := f.owner.CommitFanOutChunk(f.ctx, f.command)
				f.probe.set(nil)
				if _, safe := pipeline.FanOutSafeAggregateFailure(err); !safe || seen != position {
					t.Fatalf("actual SQL safe failure seen=%d err=%v", seen, err)
				}
				f.unchanged(t, before)
				for _, claim := range f.claims[2:] {
					if err := f.store().Release(f.ctx, claim); err != nil {
						t.Fatal(err)
					}
				}
				if err := f.group.Seal(f.ctx, 2, f.claims[:2]); err != nil {
					t.Fatalf("known rollback did not authorize lower prefix: %v", err)
				}
				f.command.Outcomes = f.command.Outcomes[:2]
				f.commit(t)
				assertFanOutCursorAndOutcomeCount(t, f.ctx, f.db, f.seed, 2, 2)
				out, err := f.group.Settle(f.ctx, f.members()[:2])
				if err != nil || len(out.Results) != 2 {
					t.Fatalf("reduced prefix %+v %v", out, err)
				}
				if err := f.group.Close(f.ctx); err != nil {
					t.Fatal(err)
				}
				for _, event := range f.events[2:] {
					claim, err := f.store().ClaimPublication(f.ctx, event.ID())
					if err != nil {
						t.Fatal(err)
					}
					if err := f.store().Release(f.ctx, claim); err != nil {
						t.Fatal(err)
					}
				}
			})
		}
	}
}

func TestFanOutPublicationGroupForeignExecutionMatrixBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newGroupProofFixture(t, backend, 2)
			original := f.events[0]
			for _, change := range []string{"run", "causal-parent", "producer", "depth", "task", "source-occurrence"} {
				t.Run(change, func(t *testing.T) {
					lineage := f.intent.Request.Capsule.Lineage
					facts := events.EventFacts{ID: original.ID(), Type: original.Type(), Producer: events.ProducerClaim{Type: original.Producer().Type(), ID: original.Producer().ID()}, Payload: original.Payload(), ChainDepth: original.ChainDepth(), Envelope: original.Envelope(), RoutingSource: original.RoutingSource(), CreatedAt: original.CreatedAt(), ExecutionMode: original.ExecutionMode(), TaskID: original.TaskID()}
					switch change {
					case "run":
						lineage.RunID = uuid.NewString()
					case "causal-parent":
						lineage.ParentEventID = uuid.NewString()
					case "producer":
						facts.Producer.ID = "other-node"
					case "depth":
						facts.ChainDepth++
					case "task":
						lineage.TaskID = uuid.NewString()
						facts.TaskID = lineage.TaskID
					case "source-occurrence":
						facts.RoutingSource = eventtest.StaticFlowRoutingSource("producer", "foreign-occurrence", uuid.NewString())
						facts.Envelope = events.EventEnvelope{}
					}
					foreign := eventtest.ChildForProducerWithRoutingSource(
						facts.ID, facts.Type, eventtest.Producer(facts.Producer.Type, facts.Producer.ID),
						facts.TaskID, facts.Payload, facts.ChainDepth, lineage, facts.Envelope,
						facts.RoutingSource, facts.CreatedAt,
					)
					before := f.snapshot(t)
					if _, err := f.group.Claim(f.ctx, 0, foreign); err == nil {
						t.Fatal("foreign execution carrier acquired member")
					}
					f.unchanged(t, before)
				})
			}
			f.prepare(t)
			f.seal(t)
			f.commit(t)
			evidence, err := f.grant.Evidence()
			if err != nil {
				t.Fatal(err)
			}
			originalRaw, err := json.Marshal(evidence)
			if err != nil {
				t.Fatal(err)
			}
			update := func(e startupownership.GrantEvidence) {
				raw, err := json.Marshal(e)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := f.db.Exec(`UPDATE runtime_generation_grants SET snapshot=$1 WHERE grant_id=$2 AND state_version=$3`, string(raw), evidence.GrantID, evidence.StateVersion); err != nil {
					t.Fatal(err)
				}
			}
			t.Cleanup(func() {
				_, _ = f.db.Exec(`UPDATE runtime_generation_grants SET snapshot=$1 WHERE grant_id=$2 AND state_version=$3`, string(originalRaw), evidence.GrantID, evidence.StateVersion)
			})
			for _, change := range []string{"grant", "process-authority", "process-owner", "process-boot", "bundle", "runtime-occurrence", "runtime-generation", "source-set", "full-grant"} {
				t.Run(change, func(t *testing.T) {
					foreign := evidence
					switch change {
					case "grant":
						foreign.GrantID = uuid.NewString()
					case "process-authority":
						foreign.ProcessAuthorityID = uuid.NewString()
					case "process-owner":
						foreign.ProcessOwnerID = "foreign-owner"
					case "process-boot":
						foreign.ProcessBootID = uuid.NewString()
					case "bundle":
						foreign.BundleHash = strings.Repeat("b", 64)
					case "runtime-occurrence":
						foreign.RuntimeInstanceID = uuid.NewString()
					case "runtime-generation":
						foreign.RuntimeGeneration++
					case "source-set":
						foreign.SourceSetRevision = strings.Repeat("b", 64)
					case "full-grant":
						foreign.ProbeSurfaceIDs = []string{"foreign-probe"}
					}
					update(foreign)
					defer update(evidence)
					before := f.snapshot(t)
					if err := f.group.ValidateCommitted(f.ctx, f.claims); err == nil {
						t.Fatal("foreign current grant authorized receiver effects")
					}
					if _, err := f.group.Settle(f.ctx, f.members()); err == nil {
						t.Fatal("foreign current grant authorized settlement")
					}
					f.unchanged(t, before)
				})
			}
			out, err := f.group.Settle(f.ctx, f.members())
			if err != nil || len(out.Results) != 2 {
				t.Fatalf("restored exact execution %+v %v", out, err)
			}
		})
	}
}

func TestFanOutPublicationGroupForeignStoreAndRunBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newGroupProofFixture(t, backend, 2)
			other := newGroupProofFixtureOn(t, backend, 2, f)
			foreignStore := newGroupProofFixture(t, backend, 2)
			for _, g := range []*groupProofFixture{f, other, foreignStore} {
				g.prepare(t)
				g.seal(t)
				g.commit(t)
			}
			if f.postgres {
				first, err := f.raw.(*PostgresStore).pipelinePostgresOwner.PostgresPipelineClaimStateForTest(f.claims[0])
				if err != nil {
					t.Fatal(err)
				}
				second, err := other.raw.(*PostgresStore).pipelinePostgresOwner.PostgresPipelineClaimStateForTest(other.claims[0])
				if err != nil {
					t.Fatal(err)
				}
				if first.LeaseForTest().Session() == second.LeaseForTest().Session() {
					t.Fatal("independent groups adopted the same designated session")
				}
			}
			for _, foreign := range []*groupProofFixture{other, foreignStore} {
				before, foreignBefore := f.snapshot(t), foreign.snapshot(t)
				mixed := f.members()
				mixed[1] = foreign.members()[1]
				if _, err := f.group.Settle(f.ctx, mixed); err == nil {
					t.Fatal("foreign run/store segment accepted")
				}
				if err := f.group.ValidateCommitted(f.ctx, []pipelineobligation.Claim{f.claims[0], foreign.claims[1]}); err == nil {
					t.Fatal("foreign run/store authorized effects")
				}
				f.unchanged(t, before)
				foreign.unchanged(t, foreignBefore)
			}
		})
	}
}

func TestFanOutPublicationGroupCleanupMembershipBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newGroupProofFixture(t, backend, 2)
			foreign := newGroupProofFixture(t, backend, 2)
			for _, g := range []*groupProofFixture{f, foreign} {
				g.prepare(t)
				g.seal(t)
			}
			if err := f.group.ValidateCommittedMembership(f.claims); err == nil {
				t.Fatal("uncommitted membership authorized cleanup")
			}
			for _, g := range []*groupProofFixture{f, foreign} {
				g.commit(t)
			}
			before, foreignBefore := f.snapshot(t), foreign.snapshot(t)
			collector, restore, err := InstallTransactionProbeForTest(f.raw, transactiontest.Options{})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(restore)
			check := func() {
				f.probe.set(func(string, string) error {
					t.Error("cleanup membership attempted SQL")
					return errors.New("static membership must not query")
				})
				defer f.probe.set(nil)
				if err := f.group.ValidateCommittedMembership(f.claims); err != nil {
					t.Fatal(err)
				}
				for _, bad := range [][]pipelineobligation.Claim{nil, f.claims[:1], {f.claims[0], f.claims[0]}, foreign.claims, {f.claims[0], foreign.claims[1]}} {
					if err := f.group.ValidateCommittedMembership(bad); err == nil {
						t.Fatal("wrong/incomplete/duplicate set authorized callback retirement")
					}
				}
			}
			check()
			ctx, cancel := context.WithCancel(f.ctx)
			cancel()
			if err := f.group.ValidateCommitted(ctx, f.claims); err == nil {
				t.Fatal("canceled dynamic validation authorized execution")
			}
			check()
			if err := f.group.Close(context.Background()); err != nil {
				t.Fatal(err)
			}
			check()
			if err := f.group.ValidateCommitted(f.ctx, f.claims); err == nil {
				t.Fatal("closed group authorized execution")
			}
			if got := collector.Snapshot(); got.Total.ReadCommits != 0 || got.Total.WriteCommits != 0 {
				t.Fatalf("static cleanup proof performed SQL commits: %+v", got)
			}
			restore()
			f.unchanged(t, before)
			foreign.unchanged(t, foreignBefore)
			if err := foreign.group.ValidateCommitted(foreign.ctx, foreign.claims); err != nil {
				t.Fatalf("wrong-group cleanup damaged foreign members: %v", err)
			}
		})
	}
}

func TestFanOutPublicationGroupJoinedFailureCannotAuthorizeBisectionBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, mode := range []string{"rollback-cleanup", "cancellation"} {
			t.Run(backend+"/"+mode, func(t *testing.T) {
				f := newGroupProofFixture(t, backend, 4)
				f.prepare(t)
				f.seal(t)
				before := f.snapshot(t)
				var envelope failures.Envelope
				if err := json.Unmarshal(rejectedFanOutChunk(f.claim, 0, 1, f.command.Now).Outcomes[0].Failure, &envelope); err != nil {
					t.Fatal(err)
				}
				cause := pipeline.NewFanOutSafeAggregateError(envelope, errors.New("known operation rejection"))
				cleanup := errors.New("independent rollback acknowledgement failure")
				ctx, cancel := context.WithCancel(f.ctx)
				defer cancel()
				f.probe.set(func(phase, q string) error {
					if (phase == "after_exec" || phase == "after_query_row") && strings.Contains(strings.ToLower(q), "insert into events") {
						if mode == "cancellation" {
							cancel()
						}
						return cause
					}
					if mode == "rollback-cleanup" && phase == "after_rollback" {
						return cleanup
					}
					return nil
				})
				_, err := f.owner.CommitFanOutChunk(ctx, f.command)
				f.probe.set(nil)
				if !errors.Is(err, cause) {
					t.Fatalf("operation cause lost: %v", err)
				}
				if mode == "rollback-cleanup" && !errors.Is(err, cleanup) {
					t.Fatalf("cleanup cause lost: %v", err)
				}
				if mode == "cancellation" && !errors.Is(err, context.Canceled) {
					t.Fatalf("cancellation cause lost: %v", err)
				}
				f.unchanged(t, before)
				for _, claim := range f.claims[2:] {
					if err := f.store().Release(f.ctx, claim); err != nil && !errors.Is(err, pipelineobligation.ErrStaleClaim) {
						t.Fatal(err)
					}
				}
				if err := f.group.Seal(f.ctx, 2, f.claims[:2]); err == nil {
					t.Fatal("joined independent failure authorized reduced-prefix execution")
				}
				if err := f.group.Close(f.ctx); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

func TestFanOutPublicationGroupRequiredForGrantedPublicationBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newGroupProofFixture(t, backend, 1)
			if err := f.group.Close(f.ctx); err != nil {
				t.Fatal(err)
			}
			plans, err := f.bus.PrepareEnginePublications(f.ctx, []engine.EmitIntent{{Event: f.events[0]}})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := f.bus.ReleaseEnginePublications(context.Background(), plans); err != nil {
					t.Error(err)
				}
			})
			before := f.snapshot(t)
			command := pipeline.FanOutChunkCommand{Claim: f.claim, Now: f.command.Now, Outcomes: []pipeline.FanOutChunkOutcome{{Ordinal: 0, Publication: plans[0]}}}
			if _, err := f.owner.CommitFanOutChunk(f.ctx, command); err == nil {
				t.Fatal("granted chunk publication bypassed sealed group using ordinary claim")
			}
			f.unchanged(t, before)
		})
	}
}
