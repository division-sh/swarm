package runtimepersistence

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/agenttopology"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/store/testutil/agentfixture"
	"github.com/google/uuid"
)

type agentRecoveryReadOwners interface {
	deliverylifecycle.ActiveSnapshotReader
	agenttopology.SourceSetHeadReader
	InspectSnapshot(context.Context, func(context.Context) error) error
}

func TestAgentRecoveryReadOwnersRetainNativeCutBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			ctx, selected := newAgentFixtureAuthorityStore(t, backend)
			reader := selected.(agentRecoveryReadOwners)
			initial := agentClaimProfileFixture{ctx: ctx, store: selected}
			before := profileFixtureSnapshot(t, initial)
			absent, found, err := reader.InspectSourceSetHead(ctx)
			if err != nil || found || !reflect.DeepEqual(absent, agenttopology.SourceSetPlan{}) {
				t.Fatalf("absent native head invented a plan: %+v found=%t err=%v", absent, found, err)
			}
			assertRecoveryReadImage(t, ctx, reader, absent, false, nil)
			if after := profileFixtureSnapshot(t, initial); !reflect.DeepEqual(before, after) {
				t.Fatal("absent/empty read created application evidence")
			}
			capability, err := agentfixture.ProcessCapability(t, ctx, selected)
			if err != nil {
				t.Fatal(err)
			}
			empty, err := agenttopology.EmptySourceSetPlan()
			if err != nil {
				t.Fatal(err)
			}
			if _, err := capability.InstallCompleteSourceSet(ctx, agenttopology.SourceSetCommitRequest{OperationID: uuid.NewString(), Plan: empty}); err != nil {
				t.Fatal(err)
			}
			assertRecoveryReadImage(t, ctx, reader, empty, true, nil)
			f := admitAgentClaimProfileFixture(t, ctx, selected)
			claim := claimAgentProfileRecoveryEvent(t, f)
			plan, found, err := capability.CurrentSourceSet(f.ctx)
			if err != nil || !found || plan.Revision == empty.Revision {
				t.Fatalf("native fixture lacks its actual complete head: %+v found=%t err=%v", plan, found, err)
			}
			foreign := newAgentClaimProfileFixture(t, backend)
			foreignReader := foreign.store.(agentRecoveryReadOwners)
			var held context.Context
			var written map[string]SelectedForkStorageTableSnapshot
			if err := reader.InspectSnapshot(f.ctx, func(ctx context.Context) error {
				held = ctx
				read := f
				read.ctx = ctx
				before := profileFixtureSnapshot(t, read)
				assertRecoveryReadImage(t, ctx, reader, plan, true, []deliverylifecycle.Snapshot{claim.Snapshot})
				profile, err := ReadFirstAgentClaimProfileForTest(ctx, f.store, claim.Snapshot)
				if err != nil || !profile.MatchesDelivery(claim.Snapshot) {
					t.Fatalf("same-cut first profile lost: %+v err=%v", profile, err)
				}
				state, found, err := f.store.LoadAgentLifecycleState(ctx, claim.Snapshot.Route.AgentIdentity)
				if err != nil || !found || state.Generation != profile.AgentGeneration || state.RuntimeEpoch != profile.AgentRuntimeEpoch ||
					state.ConfigRevision != profile.ConfigurationRevision || state.ProcessBinding.BundleHash != profile.BundleHash {
					t.Fatalf("same-cut lifecycle/profile join changed: %+v profile=%+v found=%t err=%v", state, profile, found, err)
				}
				assertRecoveryReadOwnerContextRefusal(t, ctx, foreignReader, "different selected store")
				writer, cancel := context.WithTimeout(f.ctx, 10*time.Second)
				defer cancel()
				if _, err := f.store.SettleSuccess(writer, claim.Claim, nil, 0, deliverylifecycle.NotApplicableHandlerRuleSelection()); err != nil {
					t.Fatal(err)
				}
				if _, err := capability.RestoreSourceSet(writer, agenttopology.SourceSetCommitRequest{
					OperationID: uuid.NewString(), ExpectedRevision: plan.Revision, Plan: empty,
				}); err != nil {
					t.Fatal(err)
				}
				written = profileFixtureSnapshot(t, f)
				if reflect.DeepEqual(before, written) {
					t.Fatal("native outside source-head/membership commits were not observable")
				}
				assertRecoveryReadImage(t, f.ctx, reader, empty, true, nil)
				assertRecoveryReadImage(t, ctx, reader, plan, true, []deliverylifecycle.Snapshot{claim.Snapshot})
				if after := profileFixtureSnapshot(t, read); !reflect.DeepEqual(before, after) {
					t.Fatal("held read roles crossed their original native transaction")
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			assertRecoveryReadOwnerContextRefusal(t, held, reader, "inspection snapshot is closed")
			canceled, cancel := context.WithCancel(f.ctx)
			cancel()
			assertRecoveryReadOwnerContextRefusal(t, canceled, reader, "context canceled")
			assertRecoveryReadImage(t, f.ctx, reader, empty, true, nil)
			assertAgentProfileProcessNoAttempts(t, f)
			if after := profileFixtureSnapshot(t, f); !reflect.DeepEqual(written, after) {
				t.Fatal("read roles or refused contexts changed the writer's complete storage")
			}
		})
	}
}

func assertRecoveryReadImage(t *testing.T, ctx context.Context, reader agentRecoveryReadOwners, want agenttopology.SourceSetPlan, found bool, snapshots []deliverylifecycle.Snapshot) {
	t.Helper()
	actual, exists, err := reader.InspectSourceSetHead(ctx)
	if err != nil || exists != found || !reflect.DeepEqual(actual, want) {
		t.Fatalf("native source-head image: %+v exists=%t err=%v want=%+v found=%t", actual, exists, err, want, found)
	}
	active, err := reader.ReadActiveDeliverySnapshots(ctx)
	if err != nil || active == nil || len(active) != len(snapshots) {
		t.Fatalf("native complete delivery membership: %+v err=%v want=%+v", active, err, snapshots)
	}
	for i := range active {
		if !reflect.DeepEqual(active[i], snapshots[i]) {
			t.Fatalf("native exact membership changed: %+v want=%+v", active, snapshots)
		}
	}
}

func assertRecoveryReadOwnerContextRefusal(t *testing.T, ctx context.Context, reader agentRecoveryReadOwners, reason string) {
	t.Helper()
	active, err := reader.ReadActiveDeliverySnapshots(ctx)
	if err == nil || !strings.Contains(err.Error(), reason) || active != nil {
		t.Fatalf("read membership exposed partial evidence on %s: %+v err=%v", reason, active, err)
	}
	plan, found, err := reader.InspectSourceSetHead(ctx)
	if err == nil || !strings.Contains(err.Error(), reason) || found || !reflect.DeepEqual(plan, agenttopology.SourceSetPlan{}) {
		t.Fatalf("read source head exposed partial evidence on %s: %+v found=%t err=%v", reason, plan, found, err)
	}
}

func TestSourceSetHeadNativeFaultRefusesBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newAgentClaimProfileFixture(t, backend)
			foreign := newAgentClaimProfileFixture(t, backend)
			capability, err := agentfixture.ProcessCapability(t, f.ctx, f.store)
			if err != nil {
				t.Fatal(err)
			}
			plan, found, err := capability.CurrentSourceSet(f.ctx)
			if err != nil || !found {
				t.Fatalf("fault requires its native source head: %+v found=%t err=%v", plan, found, err)
			}
			for _, fault := range []SourceSetHeadFault{SourceSetHeadCorruptPlan, SourceSetHeadRevisionMismatch} {
				t.Run(string(fault), func(t *testing.T) {
					proveSourceSetHeadNativeFault(t, f, foreign, plan, capability.CurrentSourceSet, fault)
				})
			}
		})
	}
}

func proveSourceSetHeadNativeFault(t *testing.T, f, foreign agentClaimProfileFixture, plan agenttopology.SourceSetPlan,
	locked func(context.Context) (agenttopology.SourceSetPlan, bool, error), fault SourceSetHeadFault) {
	t.Helper()
	before := profileFixtureSnapshot(t, f)
	for _, refusal := range []string{"unknown", "stale_plan", "canceled"} {
		ctx, expected, variant := f.ctx, plan, fault
		switch refusal {
		case "unknown":
			variant = SourceSetHeadFault("not-a-source-head-fault")
		case "stale_plan":
			var err error
			expected, err = agenttopology.EmptySourceSetPlan()
			if err != nil {
				t.Fatal(err)
			}
		case "canceled":
			var cancel context.CancelFunc
			ctx, cancel = context.WithCancel(ctx)
			cancel()
		}
		if receipt, err := ApplySourceSetHeadFaultForTest(ctx, f.store, expected, variant); err == nil || receipt != nil {
			t.Fatalf("source-head fault accepted %s: %+v err=%v", refusal, receipt, err)
		}
		if after := profileFixtureSnapshot(t, f); !reflect.DeepEqual(before, after) {
			t.Fatalf("refused %s source-head fault changed complete storage", refusal)
		}
	}
	receipt, err := ApplySourceSetHeadFaultForTest(f.ctx, f.store, plan, fault)
	if err != nil {
		t.Fatal(err)
	}
	restored := false
	defer func() {
		if !restored {
			if err := RestoreSourceSetHeadFaultForTest(f.ctx, f.store, receipt); err != nil {
				t.Error(err)
			}
		}
	}()
	faulted := profileFixtureSnapshot(t, f)
	column := "plan"
	if fault == SourceSetHeadRevisionMismatch {
		column = "revision"
	}
	assertSourceHeadFaultOnlyColumn(t, before, faulted, column)
	reader := f.store.(agentRecoveryReadOwners)
	for _, read := range []func(context.Context) (agenttopology.SourceSetPlan, bool, error){reader.InspectSourceSetHead, locked} {
		actual, found, err := read(f.ctx)
		if err == nil || found || !reflect.DeepEqual(actual, agenttopology.SourceSetPlan{}) {
			t.Fatalf("native %s head exposed evidence: %+v found=%t err=%v", fault, actual, found, err)
		}
		if fault == SourceSetHeadRevisionMismatch && !strings.Contains(err.Error(), "stored revision differs from decoded plan") {
			t.Fatalf("native mismatch lost canonical decoder reason: %v", err)
		}
	}
	foreignBefore := profileFixtureSnapshot(t, foreign)
	if err := RestoreSourceSetHeadFaultForTest(foreign.ctx, foreign.store, receipt); err == nil {
		t.Fatal("foreign native owner restored the source head")
	}
	canceled, cancel := context.WithCancel(f.ctx)
	cancel()
	if err := RestoreSourceSetHeadFaultForTest(canceled, f.store, receipt); err == nil {
		t.Fatal("canceled native restoration succeeded")
	}
	if repeated, err := ApplySourceSetHeadFaultForTest(f.ctx, f.store, plan, fault); err == nil || repeated != nil {
		t.Fatalf("faulted source head accepted a second fault: %+v err=%v", repeated, err)
	}
	if after := profileFixtureSnapshot(t, f); !reflect.DeepEqual(faulted, after) {
		t.Fatal("native decoder or refused restoration changed the faulted application")
	}
	if after := profileFixtureSnapshot(t, foreign); !reflect.DeepEqual(foreignBefore, after) {
		t.Fatal("foreign source-head restoration changed foreign storage")
	}
	if err := RestoreSourceSetHeadFaultForTest(f.ctx, f.store, receipt); err != nil {
		t.Fatal(err)
	}
	restored = true
	assertRecoveryReadImage(t, f.ctx, reader, plan, true, nil)
	if err := RestoreSourceSetHeadFaultForTest(f.ctx, f.store, receipt); err == nil {
		t.Fatal("source-head receipt restored twice")
	}
	if after := profileFixtureSnapshot(t, f); !reflect.DeepEqual(before, after) {
		t.Fatal("native source-head restoration did not conserve complete storage")
	}
}

func assertSourceHeadFaultOnlyColumn(t *testing.T, before, after map[string]SelectedForkStorageTableSnapshot, column string) {
	t.Helper()
	const head = "agent_topology_source_set_head"
	if len(before) != len(after) {
		t.Fatal("source-head fault changed the table census")
	}
	for table, original := range before {
		if table != head && !reflect.DeepEqual(original, after[table]) {
			t.Fatalf("source-head fault changed foreign table %s", table)
		}
	}
	a, b := before[head], after[head]
	if !reflect.DeepEqual(a.Columns, b.Columns) || len(a.Rows) != 1 || len(b.Rows) != 1 {
		t.Fatalf("source-head fault changed its singleton shape: %+v %+v", a, b)
	}
	type cell struct {
		Column, Type string
		Value        json.RawMessage
	}
	var original, faulted []cell
	if err := json.Unmarshal([]byte(a.Rows[0]), &original); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal([]byte(b.Rows[0]), &faulted); err != nil {
		t.Fatal(err)
	}
	if len(original) != len(faulted) {
		t.Fatal("source-head fault changed its physical column count")
	}
	changed := 0
	for i, prior := range original {
		if !reflect.DeepEqual(prior, faulted[i]) {
			if prior.Column != column || faulted[i].Column != column || prior.Type != faulted[i].Type {
				t.Fatalf("source-head fault changed a non-%s cell: %+v %+v", column, prior, faulted[i])
			}
			changed++
		}
	}
	if changed != 1 {
		t.Fatalf("source-head fault changed %d physical cells, want exactly %s", changed, column)
	}
}
