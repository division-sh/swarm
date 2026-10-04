package runtime

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/timerobligation"
)

type recoveryAdmissionReads struct {
	delivery    deliverylifecycle.RecoveryInventory
	timers      timerobligation.Snapshot
	manager     manager.RecoverableStateSnapshot
	deliveryErr error
	timerErr    error
	managerErr  error
	seenSource  correlation.SourceArtifactFact
	seenAt      time.Time
	reads       []string
	cancel      context.CancelFunc
}

func (r *recoveryAdmissionReads) InspectDeliveryRecovery(_ context.Context, source correlation.SourceArtifactFact) (deliverylifecycle.RecoveryInventory, error) {
	r.reads = append(r.reads, "delivery")
	r.seenSource = source
	return r.delivery, r.deliveryErr
}

func (r *recoveryAdmissionReads) ReadTimerObligations(_ context.Context, scope timerobligation.Scope, at time.Time) (timerobligation.Snapshot, error) {
	r.reads = append(r.reads, "timers:"+scope.RunID())
	r.seenAt = at
	return r.timers, r.timerErr
}

func (r *recoveryAdmissionReads) readManager(context.Context) (manager.RecoverableStateSnapshot, error) {
	r.reads = append(r.reads, "manager")
	if r.cancel != nil {
		r.cancel()
	}
	return r.manager, r.managerErr
}

func recoveryAdmissionRequest(t *testing.T, reads *recoveryAdmissionReads) StartupRecoveryReadRequest {
	t.Helper()
	source, err := correlation.NewSourceArtifactFact("bundle-v2:sha256:" + strings.Repeat("a", 64))
	if err != nil {
		t.Fatal(err)
	}
	return StartupRecoveryReadRequest{
		SourceArtifact: source, ObservedAt: time.Date(2026, 10, 3, 0, 0, 0, 0, time.UTC),
		Delivery: reads, Timers: reads, ReadManagerState: reads.readManager,
		StandingRestarts: startupRecoveryDispositionMap{
			"ordinary":          runlifecycle.StandingRestartOrdinary,
			"active":            runlifecycle.StandingRestartActiveIntrinsic,
			"suspended":         runlifecycle.StandingRestartSuspended,
			"orphaned":          runlifecycle.StandingRestartOrphaned,
			"terminal-declared": runlifecycle.StandingRestartTerminalDeclared,
			"terminal-orphaned": runlifecycle.StandingRestartTerminalOrphaned,
			"invalid":           runlifecycle.StandingRestartInvalidCurrent,
		},
	}
}

func TestSharedStartupRecoveryAdmissionPartitionsAndDecides(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		for _, test := range []struct {
			name                                                   string
			reads                                                  recoveryAdmissionReads
			blocking, workflow, standingTimers, standingDeliveries int
			denied                                                 bool
			reason                                                 startupRecoveryReasonCode
		}{
			{name: "empty", reason: startupRecoveryReasonDisabledNoWork},
			{name: "global", reads: recoveryAdmissionReads{timers: timerobligation.Snapshot{GlobalFamilies: []timerobligation.FamilyObligation{{Family: timerobligation.FamilyGlobalRecurring, RecoverableCount: 1}}}}, blocking: 1, denied: true, reason: startupRecoveryReasonDisabledWithWork},
			{name: "ordinary", reads: recoveryAdmissionReads{delivery: deliverylifecycle.RecoveryInventory{Runs: []deliverylifecycle.RecoveryRunInventory{{RunID: "ordinary", Pending: 1}}}}, denied: true, reason: startupRecoveryReasonDisabledWithDelivery},
			{name: "standing", reads: recoveryAdmissionReads{delivery: deliverylifecycle.RecoveryInventory{Runs: []deliverylifecycle.RecoveryRunInventory{{RunID: "active", Failed: 2}}}, timers: timerobligation.Snapshot{Runs: []timerobligation.RunObligations{{RunID: "active", Families: []timerobligation.FamilyObligation{{Family: timerobligation.FamilyWorkflowTimer, RecoverableCount: 3}}}}}}, standingTimers: 3, standingDeliveries: 2, reason: startupRecoveryReasonDisabledWithIntrinsic},
			{name: "manager", reads: recoveryAdmissionReads{manager: manager.RecoverableStateSnapshot{PersistedAgentCount: 1}}, reason: startupRecoveryReasonDisabledWithManagerWork},
			{name: "readiness", reads: recoveryAdmissionReads{manager: manager.RecoverableStateSnapshot{PendingDynamicFlowRuntimeReadinessCount: 1}}, reason: startupRecoveryReasonDisabledWithManagerWork},
			{name: "workflow", reads: recoveryAdmissionReads{timers: timerobligation.Snapshot{Runs: []timerobligation.RunObligations{{RunID: "ordinary", Families: []timerobligation.FamilyObligation{{Family: timerobligation.FamilyWorkflowTimer, RecoverableCount: 3}}}}}}, blocking: 3, workflow: 3, denied: true, reason: startupRecoveryReasonDisabledWithWork},
		} {
			name := test.name + "/disabled"
			if enabled {
				name = test.name + "/enabled"
			}
			t.Run(name, func(t *testing.T) {
				request := recoveryAdmissionRequest(t, &test.reads)
				request.RecoveryOnStartup = enabled
				snapshot, err := inspectStartupRecoverySnapshot(context.Background(), request)
				if err != nil {
					t.Fatal(err)
				}
				if !snapshot.InspectionComplete || snapshot.StartupBlockingTimers != test.blocking || snapshot.StartupBlockingWorkflowTimers != test.workflow || snapshot.StandingTimerObligations != test.standingTimers || snapshot.StandingDeliveryObligations != test.standingDeliveries || snapshot.Manager != test.reads.manager {
					t.Fatalf("wrong partition: %+v", snapshot)
				}
				if !test.reads.seenSource.Matches(request.SourceArtifact) || test.reads.seenAt != request.ObservedAt || !reflect.DeepEqual(test.reads.reads, []string{"delivery", "timers:", "manager"}) {
					t.Fatalf("wrong read context/order: %+v", test.reads)
				}
				decision := newStartupRecoveryDecisionReport(snapshot)
				wantReason := test.reason
				if enabled {
					wantReason = startupRecoveryReasonEnabledNoWork
					if snapshot.HasRecoverableWork() {
						wantReason = startupRecoveryReasonEnabledWithWork
					}
				}
				if decision.ReasonCode != wantReason || (decision.denialError() != nil) != (test.denied && !enabled) {
					t.Fatalf("decision: %+v", decision)
				}
				detail, err := InspectStartupRecoveryAdmission(context.Background(), request)
				if (err != nil) != (test.denied && !enabled) || !reflect.DeepEqual(detail, decision.detail()) {
					t.Fatalf("public observation differs from boot decision: %+v, %v", detail, err)
				}
			})
		}
	}
	for _, kind := range []string{"suspended", "orphaned", "terminal-declared", "terminal-orphaned", "invalid"} {
		t.Run(kind, func(t *testing.T) {
			reads := &recoveryAdmissionReads{
				delivery: deliverylifecycle.RecoveryInventory{Runs: []deliverylifecycle.RecoveryRunInventory{{RunID: kind, Pending: 3}}},
				timers:   timerobligation.Snapshot{Runs: []timerobligation.RunObligations{{RunID: kind, Families: []timerobligation.FamilyObligation{{Family: timerobligation.FamilyWorkflowTimer, RecoverableCount: 2}}}}},
			}
			snapshot, err := inspectStartupRecoverySnapshot(context.Background(), recoveryAdmissionRequest(t, reads))
			if err != nil || !snapshot.InspectionComplete || snapshot.HasRecoverableWork() {
				t.Fatalf("suppressed standing work became generic recovery: %+v, %v", snapshot, err)
			}
		})
	}
}

func TestSharedStartupRecoveryAdmissionMissingReadsNeverMeanEmpty(t *testing.T) {
	for _, test := range []struct {
		name   string
		remove func(*StartupRecoveryReadRequest)
	}{
		{"delivery", func(r *StartupRecoveryReadRequest) { r.Delivery = nil }},
		{"timers", func(r *StartupRecoveryReadRequest) { r.Timers = nil }},
		{"standing", func(r *StartupRecoveryReadRequest) { r.StandingRestarts = nil }},
		{"manager", func(r *StartupRecoveryReadRequest) { r.ReadManagerState = nil }},
		{"time", func(r *StartupRecoveryReadRequest) { r.ObservedAt = time.Time{} }},
		{"source", func(r *StartupRecoveryReadRequest) { r.SourceArtifact = correlation.SourceArtifactFact{} }},
	} {
		t.Run(test.name, func(t *testing.T) {
			reads := &recoveryAdmissionReads{}
			request := recoveryAdmissionRequest(t, reads)
			test.remove(&request)
			detail, err := InspectStartupRecoveryAdmission(context.Background(), request)
			if err == nil || detail["recovery_inspection_complete"] != false || len(reads.reads) != 0 {
				t.Fatalf("partial inspection passed: %v, %v, %v", detail, err, reads.reads)
			}
			if _, ok := detail["recoverable_work_present"]; ok {
				t.Fatal("unknown inventory reported as empty")
			}
		})
	}
}

func TestSharedStartupRecoveryAdmissionErrorsAndCancellation(t *testing.T) {
	witness := errors.New("read failed")
	for _, phase := range []string{"delivery", "timers", "standing", "manager", "cancel-before", "cancel-after"} {
		t.Run(phase, func(t *testing.T) {
			reads := &recoveryAdmissionReads{}
			request := recoveryAdmissionRequest(t, reads)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			want := witness
			switch phase {
			case "delivery":
				reads.deliveryErr = witness
			case "timers":
				reads.timerErr = witness
			case "standing":
				request.StandingRestarts = startupRecoveryDispositionMap{}
				reads.delivery.Runs = []deliverylifecycle.RecoveryRunInventory{{RunID: "missing", Pending: 1}}
				want = nil
			case "manager":
				reads.managerErr = witness
			case "cancel-before":
				cancel()
				want = context.Canceled
			case "cancel-after":
				reads.cancel = cancel
				want = context.Canceled
			}
			detail, err := InspectStartupRecoveryAdmission(ctx, request)
			if err == nil || (want != nil && !errors.Is(err, want)) || detail["recovery_inspection_complete"] != false {
				t.Fatalf("failed read looked complete: %v, %v", detail, err)
			}
			if _, ok := detail["recoverable_work_present"]; ok {
				t.Fatal("failed inventory reported no work")
			}
		})
	}
}
