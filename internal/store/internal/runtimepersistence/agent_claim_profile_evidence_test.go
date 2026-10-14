package runtimepersistence

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/llm"
	"github.com/division-sh/swarm/internal/runtime/llm/selection"
	"github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/store/testutil/agentfixture"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
	"github.com/google/uuid"
)

type agentClaimProfileFixture struct {
	ctx       context.Context
	store     agentFixtureFlowStore
	authority deliverylifecycle.ExecutionAuthority
	route     events.DeliveryRoute
	record    manager.PersistedAgent
	source    correlation.SourceArtifactFact
}

func newAgentClaimProfileFixture(t *testing.T, backend string) agentClaimProfileFixture {
	t.Helper()
	ctx, selected := newAgentFixtureAuthorityStore(t, backend)
	return admitAgentClaimProfileFixture(t, ctx, selected)
}

func admitAgentClaimProfileFixture(t *testing.T, ctx context.Context, selected agentFixtureFlowStore) agentClaimProfileFixture {
	t.Helper()
	source := grantReceiverEntitySource(t)
	bundle, _ := semanticview.Bundle(source)
	fact := sourceartifactfixture.RequireArtifact(t, ctx, selected, bundle.SourceArtifact)
	ctx = correlation.WithRunID(correlation.WithSourceArtifactFact(ctx, fact), uuid.NewString())
	ctx = authoractivity.WithScope(ctx, authoractivity.BundleScope(authorActivityTestRuntimeInstanceID, fact.BundleHash()))
	identity := mustTestAgentIdentityForRun(correlation.RunIDFromContext(ctx), "profile-worker", "global")
	record := agentFixtureStaticRecord(t, identity)
	profile, err := selection.ResolveActiveBackend(selection.BackendAnthropic)
	if err != nil {
		t.Fatal(err)
	}
	resolved, err := llm.ResolveAgentExecution(executionposture.Live, profile, nil, record.Config)
	if err != nil {
		t.Fatal(err)
	}
	record.Config = resolved.Actor
	if err := agentfixture.UpsertStaticForSource(t, ctx, selected, record, fact); err != nil {
		t.Fatal(err)
	}
	plan := currentAgentFixtureSourceSet(t, ctx, selected)
	grant, err := agentfixture.AdmitGeneration(t, ctx, selected, plan, plan.Sources[0])
	if err != nil {
		t.Fatal(err)
	}
	if _, err := grant.MarkProbesSettled(ctx, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := grant.AdmitExecution(ctx); err != nil {
		t.Fatal(err)
	}
	evidence, err := grant.Evidence()
	if err != nil {
		t.Fatal(err)
	}
	authority, err := deliverylifecycle.NewNormalExecutionAuthority(fact, evidence.GrantID, evidence.RuntimeGeneration)
	if err != nil {
		t.Fatal(err)
	}
	if err := selected.ActivateDeliveryAuthority(ctx, authority); err != nil {
		t.Fatal(err)
	}
	return agentClaimProfileFixture{ctx: ctx, store: selected, authority: authority, record: record, source: fact, route: events.DeliveryRoute{
		Recipient: events.MustAgentDeliveryRecipient(identity.AgentID()), AgentIdentity: identity,
		Target: events.MustEntitylessReceiverTarget(events.RouteIdentity{FlowID: "global", FlowInstance: "global"}),
	}}
}

func (f agentClaimProfileFixture) claim(t *testing.T) (events.Event, deliverylifecycle.ClaimedObligation, deliverylifecycle.AgentClaimProfile) {
	t.Helper()
	event := eventtest.ExistingRunRootIngress(uuid.NewString(), "test.profile_claim", "operator", "", []byte(`{}`), 0,
		f.route.AgentIdentity.RunID, events.EventEnvelope{}, time.Now().UTC())
	if err := commitSemanticEventFixtureWithRoutes(f.ctx, f.store, event, []events.DeliveryRoute{f.route}); err != nil {
		t.Fatal(err)
	}
	if err := f.store.ActivateDeliveryAuthority(f.ctx, f.authority); err != nil {
		t.Fatal(err)
	}
	result, err := f.store.ClaimDelivery(f.ctx, f.authority, event, f.route)
	claimed, acquired := result.Acquired()
	if err != nil || !acquired || result.Snapshot.ClaimVersion != 1 {
		t.Fatalf("real first agent claim: %+v err=%v", result, err)
	}
	profile, err := ReadFirstAgentClaimProfileForTest(f.ctx, f.store, result.Snapshot)
	if err != nil || !profile.MatchesFirstAdmission(event, f.route, f.authority) {
		t.Fatalf("native immutable profile: %+v err=%v", profile, err)
	}
	return event, claimed, profile
}

func profileFixtureSnapshot(t *testing.T, f agentClaimProfileFixture) map[string]SelectedForkStorageTableSnapshot {
	t.Helper()
	rows, err := ReadSelectedForkApplicationStorageSnapshotForTest(f.ctx, f.store)
	if err != nil {
		t.Fatal(err)
	}
	return rows
}

func TestAgentFirstClaimProfileRetainsOriginalAcrossReplacementAndReclaimBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newAgentClaimProfileFixture(t, backend)
			event, first, original := f.claim(t)
			replacement := agentFixtureStaticRecord(t, f.route.AgentIdentity)
			profile, err := selection.ResolveActiveBackend(selection.BackendOpenAICompatible)
			if err != nil {
				t.Fatal(err)
			}
			resolved, err := llm.ResolveAgentExecution(executionposture.Live, profile, nil, replacement.Config)
			if err != nil {
				t.Fatal(err)
			}
			replacement.Config = resolved.Actor
			if err := agentfixture.UpsertStaticForSource(t, f.ctx, f.store, replacement, f.source); err != nil {
				t.Fatal(err)
			}
			state, found, err := f.store.LoadAgentLifecycleState(f.ctx, f.route.AgentIdentity)
			if err != nil || !found || state.ConfigRevision == original.ConfigurationRevision {
				t.Fatalf("replacement did not change exact current configuration: %+v found=%t err=%v", state, found, err)
			}
			current, err := f.store.Snapshot(f.ctx, first.Claim.DeliveryID())
			if err != nil {
				t.Fatal(err)
			}
			captured, err := ReadFirstAgentClaimProfileForTest(f.ctx, f.store, current)
			if err != nil || captured != original || captured.ProfileID == profile.ID {
				t.Fatalf("replacement changed original claim capture: %+v err=%v", captured, err)
			}
			if err := ExpireExactDeliveryClaimFaultForTest(f.ctx, f.store, first.Claim); err != nil {
				t.Fatal(err)
			}
			nextAuthority, err := deliverylifecycle.NewNormalExecutionAuthority(f.source, state.ProcessBinding.GenerationGrantID, state.ProcessBinding.RuntimeGeneration)
			if err != nil {
				t.Fatal(err)
			}
			if err := f.store.ActivateDeliveryAuthority(f.ctx, nextAuthority); err != nil {
				t.Fatal(err)
			}
			reclaimed, err := f.store.ClaimDelivery(f.ctx, nextAuthority, event, f.route)
			next, acquired := reclaimed.Acquired()
			if err != nil || !acquired || next.Claim.Version() != 2 {
				t.Fatalf("real successor reclaim: %+v err=%v", reclaimed, err)
			}
			captured, err = ReadFirstAgentClaimProfileForTest(f.ctx, f.store, next.Snapshot)
			if err != nil || captured != original {
				t.Fatalf("reclaim replaced immutable first capture: %+v err=%v", captured, err)
			}
			before := profileFixtureSnapshot(t, f)
			for _, claim := range []deliverylifecycle.Claim{first.Claim, next.Claim} {
				if err := ApplyAgentClaimProfileFaultForTest(f.ctx, f.store, claim, original, AgentClaimProfileMissing, deliverylifecycle.Claim{}); err == nil {
					t.Fatal("stale or non-first version modified profile evidence")
				}
				if after := profileFixtureSnapshot(t, f); !reflect.DeepEqual(before, after) {
					t.Fatal("refused stale/non-first capture fault changed storage")
				}
			}
		})
	}
}

func TestAgentFirstClaimProfileFaultCASBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, variant := range []AgentClaimProfileFault{AgentClaimProfileMissing, AgentClaimProfileCorrupt, AgentClaimProfileForeign} {
			t.Run(backend+"/"+string(variant), func(t *testing.T) {
				f := newAgentClaimProfileFixture(t, backend)
				_, first, expected := f.claim(t)
				_, other, _ := f.claim(t)
				before := profileFixtureSnapshot(t, f)
				for _, refusal := range []string{"invalid_variant", "wrong_capture", "foreign_claim", "canceled", "wrong_owner", "unexpected_foreign"} {
					t.Run(refusal, func(t *testing.T) {
						ctx, owner, claim, capture, fault, foreign := f.ctx, any(f.store), first.Claim, expected, AgentClaimProfileCorrupt, deliverylifecycle.Claim{}
						switch refusal {
						case "invalid_variant":
							fault = "arbitrary"
						case "wrong_capture":
							capture.AgentGeneration++
						case "foreign_claim":
							claim = other.Claim
						case "canceled":
							canceled, cancel := context.WithCancel(ctx)
							cancel()
							ctx = canceled
						case "wrong_owner":
							owner = struct{}{}
						case "unexpected_foreign":
							foreign = other.Claim
						}
						if err := ApplyAgentClaimProfileFaultForTest(ctx, owner, claim, capture, fault, foreign); err == nil {
							t.Fatal("invalid profile fault accepted")
						}
						if after := profileFixtureSnapshot(t, f); !reflect.DeepEqual(before, after) {
							t.Fatal("refused fault changed complete physical storage")
						}
					})
				}
				foreign := deliverylifecycle.Claim{}
				if variant == AgentClaimProfileForeign {
					foreign = other.Claim
				}
				if err := ApplyAgentClaimProfileFaultForTest(f.ctx, f.store, first.Claim, expected, variant, foreign); err != nil {
					t.Fatal(err)
				}
				after := profileFixtureSnapshot(t, f)
				assertOnlyAgentProfileCellChanged(t, before, after, first.Claim.DeliveryID())
				if _, err := ReadFirstAgentClaimProfileForTest(f.ctx, f.store, first.Snapshot); err == nil {
					t.Fatal("missing/corrupt/foreign durable profile was accepted")
				}
				if err := ApplyAgentClaimProfileFaultForTest(f.ctx, f.store, first.Claim, expected, variant, foreign); err == nil {
					t.Fatal("stale original capture CAS was accepted")
				}
				if replay := profileFixtureSnapshot(t, f); !reflect.DeepEqual(after, replay) {
					t.Fatal("stale capture CAS changed storage")
				}
			})
		}
	}
}

func TestAgentFirstClaimProfileReadbackRefusesInexactEvidenceBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newAgentClaimProfileFixture(t, backend)
			_, first, _ := f.claim(t)
			before := profileFixtureSnapshot(t, f)
			for _, name := range []string{"missing_row", "event", "run", "route", "source", "generation", "unclaimed"} {
				t.Run(name, func(t *testing.T) {
					bad := first.Snapshot
					switch name {
					case "missing_row":
						bad.DeliveryID = uuid.NewString()
					case "event":
						bad.EventID = uuid.NewString()
					case "run":
						bad.RunID = uuid.NewString()
					case "route":
						bad.Route.Target = events.MustEntitylessReceiverTarget(events.RouteIdentity{FlowID: "global", FlowInstance: "sibling"})
					case "source", "generation":
						source, generation := f.source, f.authority.Generation()
						if name == "source" {
							var err error
							source, err = correlation.NewSourceArtifactFact("bundle-v2:sha256:" + strings.Repeat("a", 64))
							if err != nil || source.Matches(f.source) {
								t.Fatalf("independent source control: %v", err)
							}
						} else {
							generation++
						}
						var err error
						bad.Authority, err = deliverylifecycle.NewNormalExecutionAuthority(source, f.authority.ExecutionID(), generation)
						if err != nil {
							t.Fatal(err)
						}
					case "unclaimed":
						bad.ClaimVersion = 0
					}
					if _, err := ReadFirstAgentClaimProfileForTest(f.ctx, f.store, bad); err == nil {
						t.Fatal("inexact historical profile read accepted")
					}
					if after := profileFixtureSnapshot(t, f); !reflect.DeepEqual(before, after) {
						t.Fatal("refused profile read changed complete storage")
					}
				})
			}
			foreign := newAgentClaimProfileFixture(t, backend)
			_, foreignClaim, foreignCapture := foreign.claim(t)
			foreignBefore := profileFixtureSnapshot(t, foreign)
			if err := ApplyAgentClaimProfileFaultForTest(f.ctx, f.store, foreignClaim.Claim, foreignCapture, AgentClaimProfileMissing, deliverylifecycle.Claim{}); err == nil {
				t.Fatal("missing exact claim row accepted capture mutation")
			}
			if after := profileFixtureSnapshot(t, f); !reflect.DeepEqual(before, after) {
				t.Fatal("missing-row fault changed destination storage")
			}
			if after := profileFixtureSnapshot(t, foreign); !reflect.DeepEqual(foreignBefore, after) {
				t.Fatal("missing-row fault changed independent source storage")
			}
		})
	}
}

func TestAgentFirstClaimProfileCorruptionRefusesReclaimBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, variant := range []AgentClaimProfileFault{AgentClaimProfileMissing, AgentClaimProfileCorrupt, AgentClaimProfileForeign} {
			t.Run(backend+"/"+string(variant), func(t *testing.T) {
				f := newAgentClaimProfileFixture(t, backend)
				event, first, original := f.claim(t)
				foreign := deliverylifecycle.Claim{}
				if variant == AgentClaimProfileForeign {
					_, other, _ := f.claim(t)
					foreign = other.Claim
				}
				if err := ApplyAgentClaimProfileFaultForTest(f.ctx, f.store, first.Claim, original, variant, foreign); err != nil {
					t.Fatal(err)
				}
				if err := ExpireExactDeliveryClaimFaultForTest(f.ctx, f.store, first.Claim); err != nil {
					t.Fatal(err)
				}
				before := profileFixtureSnapshot(t, f)
				refused, err := f.store.ClaimDelivery(f.ctx, f.authority, event, f.route)
				if _, acquired := refused.Acquired(); acquired || err == nil || !strings.Contains(err.Error(), "first-claim agent profile") {
					t.Fatalf("corrupt first capture reached successor claim or the wrong gate: result=%+v err=%v", refused, err)
				}
				if after := profileFixtureSnapshot(t, f); !reflect.DeepEqual(before, after) {
					t.Fatal("refused corrupt-capture reclaim changed complete storage")
				}
			})
		}
	}
}

func assertOnlyAgentProfileCellChanged(t *testing.T, before, after map[string]SelectedForkStorageTableSnapshot, deliveryID string) {
	t.Helper()
	beforeBytes, err := json.Marshal(before)
	if err != nil {
		t.Fatal(err)
	}
	var expected map[string]SelectedForkStorageTableSnapshot
	if err := json.Unmarshal(beforeBytes, &expected); err != nil {
		t.Fatal(err)
	}
	prior, err := DecodeSelectedForkSnapshotRowsForTest(before["event_delivery_attempts"])
	if err != nil {
		t.Fatal(err)
	}
	current, err := DecodeSelectedForkSnapshotRowsForTest(after["event_delivery_attempts"])
	if err != nil || len(prior) != len(current) {
		t.Fatalf("fault changed attempt cardinality: %v", err)
	}
	changes := 0
	for _, old := range prior {
		oldID, err := old["delivery_id"].Text()
		if err != nil || oldID != deliveryID {
			continue
		}
		for index, row := range current {
			id, err := row["delivery_id"].Text()
			if err != nil || id != oldID {
				continue
			}
			if reflect.DeepEqual(old["agent_claim_profile"], row["agent_claim_profile"]) {
				t.Fatal("fault did not change its one captured field")
			}
			old["agent_claim_profile"] = row["agent_claim_profile"]
			if !reflect.DeepEqual(old, row) {
				t.Fatal("fault changed another attempt field")
			}
			table := expected["event_delivery_attempts"]
			table.Rows[index] = after["event_delivery_attempts"].Rows[index]
			expected["event_delivery_attempts"] = table
			changes++
		}
	}
	if changes != 1 || !reflect.DeepEqual(expected, after) {
		t.Fatal("fault changed storage outside exactly one capture cell")
	}
}
