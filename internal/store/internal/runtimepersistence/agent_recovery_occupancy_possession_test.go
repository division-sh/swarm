package runtimepersistence

import (
	"context"
	"path/filepath"
	"reflect"
	"runtime"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/agenttopology"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/runtime/startuprecovery"
	"github.com/division-sh/swarm/internal/store/testutil/agentfixture"
	"github.com/division-sh/swarm/internal/testutil"
)

// These proofs exercise existing ownership and claim admission, not an S6
// candidate consumer. A coherent count is never a substitute for either owner.
func TestAgentRecoveryOccupancyStampIsNotLivePossessionBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			open := agentClaimProfileReopener(t, backend)
			f := admitAgentClaimProfileFixture(t, testAuthorActivityContext(), open())
			first := claimAgentProfileRecoveryEvent(t, f)
			pending := publishAgentProfileRecoveryEvent(t, f)
			process, grant := agentRecoveryFixtureOwners(t, f)
			candidate := readNativeAgentRecoveryCandidate(t, f)
			assertAgentRecoveryCandidateOwner(t, candidate, process, grant)
			before := profileFixtureSnapshot(t, f)
			if owned, err := grant.InspectRunExecutionOwnership(f.ctx, first.Snapshot.RunID); err != nil || owned != manager.RunExecutionOwned {
				t.Fatalf("native current run owner: %v err=%v", owned, err)
			}
			foreign := newAgentClaimProfileFixture(t, backend)
			foreignProcess, foreignGrant := agentRecoveryFixtureOwners(t, foreign)
			if err := foreignProcess.ProveCurrent(f.ctx); err != nil {
				t.Fatal(err)
			}
			if owned, err := foreignGrant.InspectRunExecutionOwnership(f.ctx, first.Snapshot.RunID); err == nil || owned != 0 {
				t.Fatalf("foreign live process acquired this run: %v err=%v", owned, err)
			}
			assertAgentRecoveryClaimRefused(t, f, pending, foreign.authority, deliverylifecycle.ClaimWrongAuthority)
			wrongGeneration, err := deliverylifecycle.NewNormalExecutionAuthority(f.source, f.authority.ExecutionID(), f.authority.Generation()+1)
			if err != nil {
				t.Fatal(err)
			}
			assertAgentRecoveryClaimRefused(t, f, pending, wrongGeneration, deliverylifecycle.ClaimWrongAuthority)
			if after := profileFixtureSnapshot(t, f); !reflect.DeepEqual(before, after) {
				t.Fatal("ownership observations or foreign admission changed complete storage")
			}
			if err := grant.Retire(f.ctx); err != nil {
				t.Fatal(err)
			}
			if err := process.ProveCurrent(f.ctx); err != nil {
				t.Fatalf("grant retirement released native process possession: %v", err)
			}
			assertAgentRecoveryRetiredGrant(t, f.ctx, grant, first.Snapshot.RunID)
			assertAgentRecoveryClaimRefused(t, f, pending, f.authority, deliverylifecycle.ClaimDeferred)
			if got := readNativeAgentRecoveryCandidate(t, f); !reflect.DeepEqual(candidate, got) {
				t.Fatal("retiring a grant changed the durable recovery candidate")
			}
			if err := process.Release(f.ctx); err != nil {
				t.Fatal(err)
			}
			if err := process.ProveCurrent(f.ctx); err == nil {
				t.Fatal("released process proved possession from its retained stamp")
			}
			if got := readNativeAgentRecoveryCandidate(t, f); !reflect.DeepEqual(candidate, got) {
				t.Fatal("releasing possession changed the durable recovery candidate")
			}
			if err := f.store.(interface{ Close() error }).Close(); err != nil {
				t.Fatal(err)
			}
			f.store = open()
			before = profileFixtureSnapshot(t, f)
			if got := readNativeAgentRecoveryCandidate(t, f); !reflect.DeepEqual(candidate, got) {
				t.Fatal("read-only reopen before acquisition substituted new ownership")
			}
			assertAgentRecoveryRetiredGrant(t, f.ctx, grant, first.Snapshot.RunID)
			assertAgentRecoveryClaimRefused(t, f, pending, f.authority, deliverylifecycle.ClaimDeferred)
			assertAgentProfileProcessNoAttempts(t, f)
			if after := profileFixtureSnapshot(t, f); !reflect.DeepEqual(before, after) {
				t.Fatal("unpossessed reopen or refused claim changed complete storage")
			}
			t.Log("counterexample: identical complete candidate and valid durable stamp survive grant retirement, native release and unpossessed reopen; existing owners still refuse execution")
		})
	}
}

func TestAgentRecoveryOccupancyUnpossessedCrashImageBothStores(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("process-death evidence pipe requires inherited file descriptors")
	}
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			location := filepath.Join(t.TempDir(), "occupancy-process.db")
			if backend == "postgres" {
				location = testutil.StartPostgresDSN(t)
			}
			previous := killAgentProfileProcessAtClaim(t, backend, location)
			selected := openAgentProfileProcessStore(t, backend, location)
			fact := mustStoreTestSourceArtifactFact(previous.Profile.BundleHash)
			f := agentClaimProfileFixture{ctx: correlation.WithRunID(correlation.WithSourceArtifactFact(testAuthorActivityContext(), fact), previous.Profile.Identity.RunID),
				store: selected, source: fact}
			before := profileFixtureSnapshot(t, f)
			candidate, err := startuprecovery.InspectAgentRecoveryOccupancyCandidate(f.ctx, selected.(startuprecovery.AgentRecoveryReader))
			if err != nil || candidate.Image.Process.Authority == nil || *candidate.Image.Process.Authority != previous.Process ||
				candidate.Image.Process.Possession != nil || len(candidate.Image.Members) != 1 ||
				!reflect.DeepEqual(candidate.InProgressByProfile, map[string]int{previous.Profile.ProfileID: 1}) ||
				candidate.Image.Members[0].FirstClaim == nil || *candidate.Image.Members[0].FirstClaim != previous.Profile ||
				!reflect.DeepEqual(candidate.Image.Members[0].Lifecycle, previous.State) {
				t.Fatalf("dead process still has complete historical image, never possession: %+v err=%v", candidate, err)
			}
			if after := profileFixtureSnapshot(t, f); !reflect.DeepEqual(before, after) {
				t.Fatal("unpossessed post-crash inspection changed complete storage")
			}
			// Exercise the original native acquisition, Manager takeover, grant
			// admission, exact reclaim and immutable-history refusal controls.
			proveAgentProfileProcessRecovery(t, selected, previous)
			process, grant := agentRecoveryFixtureOwners(t, f)
			before = profileFixtureSnapshot(t, f)
			recovered, err := startuprecovery.InspectAgentRecoveryOccupancyCandidate(f.ctx, selected.(startuprecovery.AgentRecoveryReader))
			if err != nil || !reflect.DeepEqual(candidate.InProgressByProfile, recovered.InProgressByProfile) ||
				recovered.Image.Process.Authority == nil || *recovered.Image.Process.Authority == previous.Process {
				t.Fatalf("native takeover did not retain the complete durable count under a new stamp: %+v err=%v", recovered, err)
			}
			assertAgentRecoveryCandidateOwner(t, recovered, process, grant)
			assertAgentProfileProcessNoAttempts(t, f)
			if after := profileFixtureSnapshot(t, f); !reflect.DeepEqual(before, after) {
				t.Fatal("post-takeover candidate/ownership reads changed complete storage")
			}
			t.Log("killed and joined predecessor leaves the complete original count and admitted durable stamp; only original native takeover/rebind/admission establishes the successor, without provider launch")
		})
	}
}

func TestAgentRecoveryOccupancyHeldAcrossProcessReplacementBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) { proveAgentRecoveryOccupancyProcessCut(t, backend) })
	}
}

func proveAgentRecoveryOccupancyProcessCut(t *testing.T, backend string) {
	t.Helper()
	open := agentClaimProfileReopener(t, backend)
	f := admitAgentClaimProfileFixture(t, testAuthorActivityContext(), open())
	first := claimAgentProfileRecoveryEvent(t, f)
	profile, err := ReadFirstAgentClaimProfileForTest(f.ctx, f.store, first.Snapshot)
	if err != nil {
		t.Fatal(err)
	}
	pending := publishAgentProfileRecoveryEvent(t, f)
	process, grant := agentRecoveryFixtureOwners(t, f)
	original := readNativeAgentRecoveryCandidate(t, f)
	assertAgentRecoveryCandidateOwner(t, original, process, grant)
	state, found, err := f.store.LoadAgentLifecycleState(f.ctx, f.route.AgentIdentity)
	if err != nil || !found {
		t.Fatalf("exact original lifecycle: found=%t err=%v", found, err)
	}
	previous := agentProfileProcessEvidence{Profile: profile, Process: *original.Image.Process.Authority,
		State: state, Attempts: profileFixtureSnapshot(t, f)["event_delivery_attempts"]}
	var held context.Context
	var next agentClaimProfileFixture
	var written map[string]SelectedForkStorageTableSnapshot
	reader := f.store.(startuprecovery.AgentRecoveryReader)
	if err := reader.InspectSnapshot(f.ctx, func(ctx context.Context) error {
		held = ctx
		read := f
		read.ctx = ctx
		before := profileFixtureSnapshot(t, read)
		if got := readNativeAgentRecoveryCandidate(t, read); !reflect.DeepEqual(original, got) {
			t.Fatal("held inspection did not retain the original complete candidate")
		}
		if err := process.Release(f.ctx); err != nil {
			return err
		}
		assertAgentRecoveryRetiredGrant(t, f.ctx, grant, first.Snapshot.RunID)
		next = f
		next.store = open()
		if got := readNativeAgentRecoveryCandidate(t, next); !reflect.DeepEqual(original, got) {
			t.Fatal("reopened reader invented possession before native acquisition")
		}
		nextProcess, err := agentfixture.ProcessCapability(t, next.ctx, next.store)
		if err != nil {
			return err
		}
		stamp, err := nextProcess.Evidence()
		if err != nil || stamp.Validate() != nil || stamp.AuthorityGeneration != previous.Process.AuthorityGeneration+1 ||
			stamp.AuthorityID == previous.Process.AuthorityID || stamp.BootID == previous.Process.BootID {
			t.Fatalf("native ownership replacement: old=%+v new=%+v err=%v", previous.Process, stamp, err)
		}
		plan, found, err := nextProcess.CurrentSourceSet(next.ctx)
		if err != nil || !found || !reflect.DeepEqual(plan, original.Image.SourceHead) {
			t.Fatalf("replacement changed the complete source plan: %+v found=%t err=%v", plan, found, err)
		}
		nextGrant, err := agentfixture.AdmitGeneration(t, next.ctx, next.store, plan,
			agenttopology.SourceCoordinate{BundleHash: next.source.BundleHash()})
		if err != nil {
			return err
		}
		evidence, err := nextGrant.Evidence()
		if err != nil || evidence.State != startupownership.GrantPrepared {
			t.Fatalf("reopened owner bypassed prepared generation: %+v err=%v", evidence, err)
		}
		rebindAgentProfileProcessLifecycle(t, next, nextGrant, previous)
		next.authority, err = deliverylifecycle.NewNormalExecutionAuthority(next.source, evidence.GrantID, evidence.RuntimeGeneration)
		if err != nil {
			return err
		}
		if err := next.store.ActivateDeliveryAuthority(next.ctx, next.authority); err != nil {
			return err
		}
		assertAgentRecoveryClaimRefused(t, next, pending, next.authority, deliverylifecycle.ClaimDeferred)
		if _, err := nextGrant.MarkProbesSettled(next.ctx, nil); err != nil {
			return err
		}
		assertAgentRecoveryClaimRefused(t, next, pending, next.authority, deliverylifecycle.ClaimDeferred)
		if _, err := nextGrant.AdmitExecution(next.ctx); err != nil {
			return err
		}
		assertAgentRecoveryCandidateOwner(t, readNativeAgentRecoveryCandidate(t, next), nextProcess, nextGrant)
		result, err := next.store.ClaimDelivery(next.ctx, next.authority, pending, next.route)
		claimed, acquired := result.Acquired()
		if err != nil || !acquired || !result.Acknowledged || claimed.Claim.Version() != 1 {
			t.Fatalf("exact successor's admitted first claim: %+v err=%v", result, err)
		}
		assertNativeAgentRecoveryOccupancy(t, next, map[string]int{profile.ProfileID: 2})
		actual, err := next.store.Snapshot(next.ctx, first.Snapshot.DeliveryID)
		if err != nil {
			return err
		}
		assertAgentProfileRecoveryCapture(t, next, actual, profile)
		written = profileFixtureSnapshot(t, next)
		if got := readNativeAgentRecoveryCandidate(t, read); !reflect.DeepEqual(original, got) {
			t.Fatal("outside replacement/admission crossed the held complete candidate")
		}
		assertConnectedAgentRecoveryContextRefusal(t, ctx, next.store, "different selected store")
		assertAgentRecoveryRetiredGrant(t, f.ctx, grant, first.Snapshot.RunID)
		if after := profileFixtureSnapshot(t, read); !reflect.DeepEqual(before, after) {
			t.Fatal("held recovery image mixed in outside process/grant/claim writes")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	assertConnectedAgentRecoveryContextRefusal(t, held, f.store, "inspection snapshot is closed")
	assertAgentProfileProcessNoAttempts(t, next)
	if after := profileFixtureSnapshot(t, next); !reflect.DeepEqual(written, after) {
		t.Fatal("closed/foreign inspection or old-owner refusals changed successor storage")
	}
	t.Log("held candidate remains old process/count=1 while native reopened successor admits count=2; foreign/closed scopes expose zero candidate, never partial occupancy; no slot, launch or boot wiring")
}

func agentRecoveryFixtureOwners(t *testing.T, f agentClaimProfileFixture) (startupownership.ProcessCapability, startupownership.LiveGenerationGrant) {
	t.Helper()
	process, err := agentfixture.ProcessCapability(t, f.ctx, f.store)
	if err != nil {
		t.Fatal(err)
	}
	plan, found, err := process.CurrentSourceSet(f.ctx)
	if err != nil || !found {
		t.Fatalf("native process source plan: found=%t err=%v", found, err)
	}
	grant, err := agentfixture.AdmitGeneration(t, f.ctx, f.store, plan, agenttopology.SourceCoordinate{BundleHash: f.source.BundleHash()})
	if err != nil {
		t.Fatal(err)
	}
	return process, grant
}

func readNativeAgentRecoveryCandidate(t *testing.T, f agentClaimProfileFixture) startuprecovery.AgentRecoveryOccupancyCandidate {
	t.Helper()
	got, err := startuprecovery.InspectAgentRecoveryOccupancyCandidate(f.ctx, f.store.(startuprecovery.AgentRecoveryReader))
	if err != nil || got.Image.Process.Possession != nil || got.Image.Process.Authority == nil ||
		got.Image.Process.Validate() != nil || got.InProgressByProfile == nil || len(got.Image.Members) != 2 {
		t.Fatalf("complete native candidate without possession: %+v err=%v", got, err)
	}
	return got
}

func assertAgentRecoveryCandidateOwner(t *testing.T, candidate startuprecovery.AgentRecoveryOccupancyCandidate,
	process startupownership.ProcessCapability, grant startupownership.LiveGenerationGrant) {
	t.Helper()
	if err := process.ProveCurrent(t.Context()); err != nil {
		t.Fatal(err)
	}
	stamp, err := process.Evidence()
	if err != nil || candidate.Image.Process.Authority == nil || stamp != *candidate.Image.Process.Authority {
		t.Fatalf("candidate is not the exact possessed process: %+v err=%v", stamp, err)
	}
	evidence, err := grant.Evidence()
	if err != nil || evidence.State != startupownership.GrantAdmitted || evidence.ProcessCoordinates() != stamp.LeaseStamp().Coordinates() ||
		evidence.RuntimeInstanceID != stamp.RuntimeInstanceID || evidence.SourceSetRevision != candidate.Image.SourceHead.Revision {
		t.Fatalf("exact process lacks its admitted candidate generation: %+v err=%v", evidence, err)
	}
	if err := grant.ProveCurrent(t.Context()); err != nil {
		t.Fatal(err)
	}
	binding, err := grant.ProcessExecutionBinding()
	if err != nil {
		t.Fatal(err)
	}
	for _, member := range candidate.Image.Members {
		if !member.Lifecycle.ProcessBinding.Equal(binding) {
			t.Fatalf("complete candidate retains a lifecycle not bound to this admitted generation: %+v binding=%+v", member.Lifecycle.ProcessBinding, binding)
		}
	}
}

func assertAgentRecoveryRetiredGrant(t *testing.T, ctx context.Context, grant startupownership.LiveGenerationGrant, runID string) {
	t.Helper()
	if err := grant.ProveCurrent(ctx); err == nil {
		t.Fatal("old/retired generation proved current from a durable candidate")
	}
	if evidence, err := grant.Evidence(); err == nil || !reflect.DeepEqual(evidence, startupownership.GrantEvidence{}) {
		t.Fatalf("retired generation returned usable evidence: %+v err=%v", evidence, err)
	}
	if owned, err := grant.InspectRunExecutionOwnership(ctx, runID); err == nil || owned != 0 {
		t.Fatalf("retired generation inspected current ownership: %v err=%v", owned, err)
	}
}

func assertAgentRecoveryClaimRefused(t *testing.T, f agentClaimProfileFixture, event events.Event,
	authority deliverylifecycle.ExecutionAuthority, want deliverylifecycle.ClaimDisposition) {
	t.Helper()
	before := profileFixtureSnapshot(t, f)
	result, err := f.store.ClaimDelivery(f.ctx, authority, event, f.route)
	if _, acquired := result.Acquired(); err != nil || acquired || result.Disposition != want {
		t.Fatalf("unadmitted exact pending claim: %+v want=%s err=%v", result, want, err)
	}
	if after := profileFixtureSnapshot(t, f); !reflect.DeepEqual(before, after) {
		t.Fatal("refused claim changed complete storage or manufactured first capture")
	}
}
