package runtimepersistence

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/core/eventreceiver"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/store/testutil/agentfixture"
	"github.com/division-sh/swarm/internal/testutil"
)

type agentProfileProcessEvidence struct {
	Profile  deliverylifecycle.AgentClaimProfile
	Process  startupownership.Authority
	State    manager.AgentLifecycleState
	Attempts SelectedForkStorageTableSnapshot
	Standing *runlifecycle.StandingRestartDisposition
}

func TestAgentFirstClaimProfileProcessTakeoverBeforeAttemptBothStores(t *testing.T) {
	if backend := os.Getenv("SWARM_S6_PROFILE_PROCESS_BACKEND"); backend != "" {
		runAgentProfileProcessPredecessor(t, backend, os.Getenv("SWARM_S6_PROFILE_PROCESS_LOCATION"))
		return
	}
	if runtime.GOOS == "windows" {
		t.Skip("process-death evidence pipe requires inherited file descriptors")
	}
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			location := filepath.Join(t.TempDir(), "profile-process.db")
			if backend == "postgres" {
				location = testutil.StartPostgresDSN(t)
			}
			previous := killAgentProfileProcessAtClaim(t, backend, location)
			proveAgentProfileProcessRecovery(t, openAgentProfileProcessStore(t, backend, location), previous)
		})
	}
}

func openAgentProfileProcessStore(t *testing.T, backend, location string) agentFixtureFlowStore {
	t.Helper()
	if backend == "sqlite" {
		return newBootstrappedSQLiteRuntimeStoreForPath(t, location)
	}
	if backend != "postgres" {
		t.Fatalf("unsupported profile process backend %q", backend)
	}
	selected, err := NewPostgresStore(location)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := selected.Close(); err != nil {
			t.Error(err)
		}
	})
	bootstrapTestPostgresStore(t, selected)
	return selected
}

func runAgentProfileProcessPredecessor(t *testing.T, backend, location string) {
	selected := openAgentProfileProcessStore(t, backend, location)
	var f agentClaimProfileFixture
	if os.Getenv("SWARM_S6_PROFILE_PROCESS_STANDING") == "1" {
		standing := newStandingAgentProfileProcessFixture(t, selected)
		f = startStandingAgentProfileProcessFixture(t, standing)
	} else {
		f = admitAgentClaimProfileFixture(t, testAuthorActivityContext(), selected)
	}
	claimed := claimAgentProfileRecoveryEvent(t, f)
	profile, err := ReadFirstAgentClaimProfileForTest(f.ctx, f.store, claimed.Snapshot)
	if err != nil {
		t.Fatal(err)
	}
	process, err := agentfixture.ProcessCapability(t, f.ctx, f.store)
	if err != nil {
		t.Fatal(err)
	}
	authority, err := process.Evidence()
	if err != nil {
		t.Fatal(err)
	}
	state, found, err := f.store.LoadAgentLifecycleState(f.ctx, f.route.AgentIdentity)
	if err != nil || !found {
		t.Fatalf("predecessor lifecycle: found=%t err=%v", found, err)
	}
	var standing *runlifecycle.StandingRestartDisposition
	if os.Getenv("SWARM_S6_PROFILE_PROCESS_STANDING") == "1" {
		disposition, err := selected.(runlifecycle.StandingRestartDispositionReader).StandingRunRestartDisposition(f.ctx, profile.Identity.RunID)
		if err != nil || disposition.Validate() != nil || !disposition.Executable() || disposition.RunID != profile.Identity.RunID {
			t.Fatalf("predecessor standing generation: %+v err=%v", disposition, err)
		}
		standing = &disposition
	}
	assertAgentProfileProcessNoAttempts(t, f)
	output := os.NewFile(3, "agent-profile-claim-evidence")
	if err := json.NewEncoder(output).Encode(agentProfileProcessEvidence{
		Profile: profile, Process: authority, State: state,
		Attempts: profileFixtureSnapshot(t, f)["event_delivery_attempts"],
		Standing: standing,
	}); err != nil {
		t.Fatal(err)
	}
	if err := output.Close(); err != nil {
		t.Fatal(err)
	}
	<-t.Context().Done()
}

func killAgentProfileProcessAtClaim(t *testing.T, backend, location string, environment ...string) agentProfileProcessEvidence {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 40*time.Second)
	defer cancel()
	read, write, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer read.Close()
	defer write.Close()
	var output bytes.Buffer
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestAgentFirstClaimProfileProcessTakeoverBeforeAttemptBothStores$", "-test.timeout=40s")
	cmd.Env = append(os.Environ(), "SWARM_S6_PROFILE_PROCESS_BACKEND="+backend, "SWARM_S6_PROFILE_PROCESS_LOCATION="+location)
	cmd.Env = append(cmd.Env, environment...)
	cmd.ExtraFiles, cmd.Stdout, cmd.Stderr = []*os.File{write}, &output, &output
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	_ = write.Close()
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	joined := false
	defer func() {
		if !joined {
			_ = cmd.Process.Kill()
			<-done
		}
	}()
	var evidence agentProfileProcessEvidence
	decoded := make(chan error, 1)
	go func() { decoded <- json.NewDecoder(read).Decode(&evidence) }()
	select {
	case err := <-decoded:
		if err != nil {
			_ = cmd.Process.Kill()
			<-done
			joined = true
			t.Fatalf("predecessor claim evidence: %v\n%s", err, output.String())
		}
	case <-ctx.Done():
		_ = read.Close()
		<-decoded
		t.Fatal("predecessor did not acknowledge its pre-attempt claim within the process proof budget")
	}
	if err := cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	err = <-done
	joined = true
	if err == nil || cmd.ProcessState.Success() {
		t.Fatal("predecessor completed normally instead of losing process possession")
	}
	return evidence
}

func proveAgentProfileProcessRecovery(t *testing.T, selected agentFixtureFlowStore, previous agentProfileProcessEvidence) {
	t.Helper()
	f := agentClaimProfileFixture{ctx: testAuthorActivityContext(), store: selected,
		source: mustStoreTestSourceArtifactFact(previous.Profile.BundleHash)}
	f.ctx = correlation.WithRunID(correlation.WithSourceArtifactFact(f.ctx, f.source), previous.Profile.Identity.RunID)
	snapshot, err := selected.Snapshot(f.ctx, previous.Profile.DeliveryID)
	if err != nil || !previous.Profile.MatchesDelivery(snapshot) || snapshot.Status != deliverylifecycle.StatusInProgress || snapshot.ClaimVersion != 1 {
		t.Fatalf("canonical pre-takeover claim/profile join: %+v err=%v", snapshot, err)
	}
	f.route = snapshot.Route
	process, err := agentfixture.ProcessCapability(t, f.ctx, selected)
	if err != nil {
		t.Fatal(err)
	}
	current, err := process.Evidence()
	if err != nil || previous.Process.Validate() != nil || current.Validate() != nil || current.AuthorityID == previous.Process.AuthorityID ||
		current.AuthorityGeneration != previous.Process.AuthorityGeneration+1 || current.Backend != previous.Process.Backend || current.BootID == previous.Process.BootID {
		t.Fatalf("exact native process takeover: old=%+v current=%+v err=%v", previous.Process, current, err)
	}
	f.authority = admitAgentClaimProfileAuthority(t, f.ctx, selected, f.source)
	plan, found, err := process.CurrentSourceSet(f.ctx)
	if err != nil || !found || len(plan.Sources) != 1 {
		t.Fatalf("retained source plan: %+v found=%t err=%v", plan, found, err)
	}
	grant, err := agentfixture.AdmitGeneration(t, f.ctx, selected, plan, plan.Sources[0])
	if err != nil {
		t.Fatal(err)
	}
	rebindAgentProfileProcessLifecycle(t, f, grant, previous)
	if err := selected.ActivateDeliveryAuthority(f.ctx, f.authority); err != nil {
		t.Fatal(err)
	}
	assertAgentProfileProcessRecoveredClaim(t, f, previous)
}

func rebindAgentProfileProcessLifecycle(t *testing.T, f agentClaimProfileFixture, grant startupownership.LiveGenerationGrant, previous agentProfileProcessEvidence) {
	t.Helper()
	am := manager.NewAgentManagerWithOptions(nil, nil, manager.AgentManagerOptions{
		LifecycleStore: grant, ReceiverExecution: eventreceiver.NormalExecution(),
		PersistenceRoles: manager.PersistenceRoles{LifecycleCensus: f.store, LifecycleState: f.store, LifecycleOutcome: f.store},
	}, f.store)
	if err := am.RebindLifecycleExecutionForStartup(f.ctx); err != nil {
		t.Fatal(err)
	}
	binding, err := grant.ProcessExecutionBinding()
	if err != nil {
		t.Fatal(err)
	}
	state, found, err := f.store.LoadAgentLifecycleState(f.ctx, f.route.AgentIdentity)
	if err != nil || !found || state.Generation != previous.State.Generation+1 || state.RuntimeEpoch != previous.State.RuntimeEpoch+1 || state.ConfigRevision != previous.State.ConfigRevision ||
		!state.ProcessBinding.Equal(binding) || state.Phase != previous.State.Phase || state.RunMode != previous.State.RunMode || !state.Topology.Equal(previous.State.Topology) {
		t.Fatalf("real startup lifecycle rebind: before=%+v after=%+v found=%t err=%v", previous.State, state, found, err)
	}
	before := profileFixtureSnapshot(t, f)
	if !reflect.DeepEqual(previous.Attempts, before["event_delivery_attempts"]) {
		t.Fatal("process takeover rewrote the first attempt before authority activation")
	}
	if err := am.RebindLifecycleExecutionForStartup(f.ctx); err != nil {
		t.Fatal(err)
	}
	if after := profileFixtureSnapshot(t, f); !reflect.DeepEqual(before, after) {
		t.Fatal("exact startup takeover retry changed complete application storage")
	}
}

func assertAgentProfileProcessRecoveredClaim(t *testing.T, f agentClaimProfileFixture, previous agentProfileProcessEvidence) {
	t.Helper()
	snapshot, err := f.store.Snapshot(f.ctx, previous.Profile.DeliveryID)
	if err != nil || snapshot.Status != deliverylifecycle.StatusInProgress || snapshot.ClaimVersion != 1 {
		t.Fatalf("pre-attempt claim did not survive process death: %+v err=%v", snapshot, err)
	}
	before := profileFixtureSnapshot(t, f)
	assertAgentProfileProcessLeaseHandoff(t, previous.Attempts, before["event_delivery_attempts"])
	assertAgentProfileRecoveryCapture(t, f, snapshot, previous.Profile)
	continuation, err := f.store.ObserveDeliveryContinuation(f.ctx, f.authority, snapshot.DeliveryID)
	if err != nil || continuation.Disposition != deliverylifecycle.ClaimReclaimable {
		t.Fatalf("successor did not observe exact reclaimable obligation: %+v err=%v", continuation, err)
	}
	assertAgentProfileProcessNoAttempts(t, f)
	if after := profileFixtureSnapshot(t, f); !reflect.DeepEqual(before, after) {
		t.Fatal("pre-attempt process recovery reads changed complete application storage")
	}
	persisted, found, err := f.store.(bus.PreparedPublishEventReader).LoadPreparedPublishEvent(f.ctx, snapshot.EventID)
	if err != nil || !found || persisted.Event.ID() != snapshot.EventID {
		t.Fatalf("exact canonical event readback: found=%t err=%v", found, err)
	}
	predecessor, err := deliverylifecycle.NewNormalExecutionAuthority(f.source, previous.Profile.ExecutionID, previous.Profile.ExecutionGeneration)
	if err != nil || predecessor.Equal(f.authority) {
		t.Fatalf("original captured execution differs from its actual successor: %v", err)
	}
	result, err := f.store.ClaimDelivery(f.ctx, predecessor, persisted.Event.Event(), f.route)
	if err != nil || result.Disposition != deliverylifecycle.ClaimWrongAuthority || !result.Acknowledged {
		t.Fatalf("predecessor retained live claim permission after process death: %+v err=%v", result, err)
	}
	if after := profileFixtureSnapshot(t, f); !reflect.DeepEqual(before, after) {
		t.Fatal("fenced predecessor refusal changed complete application storage")
	}
	result, err = f.store.ClaimDelivery(f.ctx, f.authority, persisted.Event.Event(), f.route)
	claimed, acquired := result.Acquired()
	if err != nil || !acquired || !result.Acknowledged || claimed.Snapshot.ClaimVersion != 2 {
		t.Fatalf("successor exact reclaim: %+v err=%v", result, err)
	}
	assertAgentProfileRecoveryCapture(t, f, claimed.Snapshot, previous.Profile)
	assertAgentProfileProcessNoAttempts(t, f)
	t.Logf("native process death before attempt: exact successor lifecycle rebind and claim v2 retain first profile/config/agent+execution generations; no provider launch")
}

func assertAgentProfileProcessLeaseHandoff(t *testing.T, previous, current SelectedForkStorageTableSnapshot) {
	t.Helper()
	before, err := DecodeSelectedForkSnapshotRowsForTest(previous)
	if err != nil || len(before) != 1 {
		t.Fatalf("predecessor exact attempt: rows=%d err=%v", len(before), err)
	}
	after, err := DecodeSelectedForkSnapshotRowsForTest(current)
	if err != nil || len(after) != 1 {
		t.Fatalf("successor exact attempt: rows=%d err=%v", len(after), err)
	}
	if reflect.DeepEqual(before[0]["lease_expires_at"], after[0]["lease_expires_at"]) {
		t.Fatal("authority activation did not expire the exact predecessor lease")
	}
	// ActivateNormalAuthority shortens the old lease; all identity, capture and
	// outcome bytes must remain unchanged until the ordinary reclaim owns v2.
	delete(before[0], "lease_expires_at")
	delete(after[0], "lease_expires_at")
	if !reflect.DeepEqual(previous.Columns, current.Columns) || !reflect.DeepEqual(before, after) {
		t.Fatal("authority activation changed first-attempt evidence beyond its owned lease expiry")
	}
}

func assertAgentProfileProcessNoAttempts(t *testing.T, f agentClaimProfileFixture) {
	t.Helper()
	attempts, err := ReadExternalAttemptStorageForTest(f.ctx, f.store)
	if err != nil || len(attempts) != 0 {
		t.Fatalf("pre-attempt process proof launched work: attempts=%d err=%v", len(attempts), err)
	}
}
