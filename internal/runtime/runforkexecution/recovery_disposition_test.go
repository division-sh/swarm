package runforkexecution

import (
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/durabledata"
	"github.com/division-sh/swarm/internal/runtime/agentcontrol"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/google/uuid"
)

func selectedRecoveryEvidence() (runfork.SelectedForkRecoveryResult, runfork.SelectedForkRecoveryEntry) {
	point := runfork.RunForkPoint{Kind: runfork.RunForkPointDeploymentRevision, Revision: 3}
	selection := runfork.RunForkContractSelection{Mode: runfork.RunForkContractSelectionModeSelectedContracts}
	bundle := "bundle-v2:sha256:" + strings.Repeat("a", 64)
	entry := runfork.SelectedForkRecoveryEntry{
		BundleHash: bundle,
		Binding: runfork.RunForkSelectedContractBinding{
			BindingID: uuid.NewString(), ForkRunID: uuid.NewString(), SourceRunID: uuid.NewString(),
			ForkPoint: point, ContractSelection: selection,
		},
	}
	pin := durabledata.Pin{
		RunID: entry.Binding.ForkRunID, RunState: "paused", Declaration: durabledata.DeclarationRef{FlowPath: ".", EventName: "root.ready"},
		SchemaDigest: durabledata.SchemaDigest("resource-schema-v1:sha256:" + strings.Repeat("b", 64)),
		VersionID:    durabledata.VersionID("resource-version-v1:sha256:" + strings.Repeat("c", 64)), Selection: "fork_override",
	}
	operation := runfork.ForkOperationRequest{
		OperationID: uuid.NewString(), Actor: "operator", TransportHash: "transport-hash",
		SourceRunID: entry.Binding.SourceRunID, ResolvedPoint: &point,
		TargetBundleHash: bundle, ContractSelection: selection,
		DataPinOverrides: []durabledata.ExplicitPin{{Declaration: pin.Declaration, VersionID: pin.VersionID}},
	}
	operation, hash, err := operation.Canonical()
	if err != nil {
		panic(err)
	}
	return runfork.SelectedForkRecoveryResult{
		RunID: entry.Binding.ForkRunID, ExecutionID: uuid.NewString(),
		Disposition: runfork.SelectedForkRecoveryResume,
		Operation: &runfork.ForkOperationRecord{Request: operation, SemanticHash: hash, ForkRunID: entry.Binding.ForkRunID,
			BindingID: entry.Binding.BindingID, Status: runfork.ForkOperationMaterialized},
		Continuation: &runfork.SelectedForkContinuation{ForkRunStatus: runfork.RunForkMaterializedStatus, Pins: []durabledata.Pin{pin}},
	}, entry
}

func TestSelectedRecoveryBootActionsAreClosed(t *testing.T) {
	_, entry := selectedRecoveryEvidence()
	for _, tc := range []struct {
		name        string
		disposition runfork.SelectedForkRecoveryDisposition
		want        selectedRecoveryAction
	}{
		{"terminal", runfork.SelectedForkRecoveryTerminal, selectedRecoveryTerminal},
		{"staged", runfork.SelectedForkRecoveryStaged, selectedRecoveryStaged},
		{"control_only", runfork.SelectedForkRecoveryControlOnly, selectedRecoveryControlOnly},
		{"failed", runfork.SelectedForkRecoveryFailed, selectedRecoveryFailed},
		{"current_process_refusal", runfork.SelectedForkRecoveryCurrent, selectedRecoveryRejectCurrent},
	} {
		t.Run(tc.name, func(t *testing.T) {
			action, err := selectedRecoveryActionFor(runfork.SelectedForkRecoveryResult{RunID: entry.Binding.ForkRunID, Disposition: tc.disposition}, entry)
			if err != nil || action != tc.want {
				t.Fatalf("recovery action = %d, %v; want %d", action, err, tc.want)
			}
		})
	}
	for _, disposition := range []runfork.SelectedForkRecoveryDisposition{"", "resumable", "normal_runtime"} {
		if action, err := selectedRecoveryActionFor(runfork.SelectedForkRecoveryResult{RunID: entry.Binding.ForkRunID, Disposition: disposition}, entry); err == nil || action != 0 {
			t.Fatalf("unadmitted disposition %q became boot action %d: %v", disposition, action, err)
		}
	}
}

func TestSelectedCancellationRecoveryPreservesPermanentOperationBinding(t *testing.T) {
	result, entry := selectedRecoveryEvidence()
	origin, err := effects.DirectiveCompletionOrigin(agentcontrol.DirectiveExecutionOrigin{
		OperationID: uuid.NewString(), ExecutionOwnerID: uuid.NewString(),
	})
	if err != nil {
		t.Fatal(err)
	}
	result.PendingCancellations = []effects.TurnExecutionResult{{
		Attempt: effects.Attempt{Origin: origin},
		Cancellation: effects.TurnCancellation{Committed: true, Requested: true, Origin: origin,
			Reason: deliverylifecycle.CancellationTerminate, CauseEvent: uuid.NewString(), RequestedAt: time.Now().UTC()},
	}}
	if action, err := selectedRecoveryActionFor(result, entry); err != nil || action != selectedRecoverySettleCancellations {
		t.Fatalf("exact prelaunch cancellation lost settlement recovery: action=%d err=%v", action, err)
	}
	result.Operation.ForkRunID = uuid.NewString()
	if action, err := selectedRecoveryActionFor(result, entry); err == nil || action != 0 {
		t.Fatalf("cancellation bypassed permanent operation binding: action=%d err=%v", action, err)
	}
}

func TestSelectedRecoveryUsesPersistedCutIdentityNotDiagnosticMetadata(t *testing.T) {
	result, entry := selectedRecoveryEvidence()
	point := runfork.RunForkPoint{Kind: runfork.RunForkPointEvent, Revision: 3, EventID: uuid.NewString()}
	entry.Binding.ForkPoint, entry.Binding.ForkEventID = point, point.EventID
	point.Input, point.EventName, point.ProducedBy, point.Timestamp = "event selector", "item.received", "historical producer", time.Unix(1700002200, 0).UTC()
	result.Operation.Request.ResolvedPoint, result.Operation.Request.ForkEventID = &point, point.EventID
	request, hash, err := result.Operation.Request.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	result.Operation.Request, result.Operation.SemanticHash = request, hash
	if action, err := selectedRecoveryActionFor(result, entry); err != nil || action != selectedRecoveryResume {
		t.Fatalf("exact recorded event cut was rejected for diagnostic metadata: %d %v", action, err)
	}
	for _, changed := range []runfork.RunForkPoint{
		{Kind: runfork.RunForkPointEvent, Revision: 4, EventID: point.EventID},
		{Kind: runfork.RunForkPointEvent, Revision: 3, EventID: uuid.NewString()},
		{Kind: runfork.RunForkPointRunStart, Revision: 3},
		{Kind: runfork.RunForkPointEvent, Revision: 0, EventID: point.EventID},
	} {
		altered := entry
		altered.Binding.ForkPoint = changed
		if action, err := selectedRecoveryActionFor(result, altered); err == nil || action != 0 {
			t.Fatalf("changed or invalid cut became recovery authority: %+v %d %v", changed, action, err)
		}
	}
}

func TestSelectedFiniteFeedRecoveryRequiresExactTypedOperation(t *testing.T) {
	valid, entry := selectedRecoveryEvidence()
	for _, tc := range []struct {
		disposition runfork.SelectedForkRecoveryDisposition
		want        selectedRecoveryAction
	}{
		{runfork.SelectedForkRecoveryResume, selectedRecoveryResume},
		{runfork.SelectedForkRecoveryActivate, selectedRecoveryActivate},
	} {
		result := valid
		result.Disposition = tc.disposition
		if action, err := selectedRecoveryActionFor(result, entry); err != nil || action != tc.want {
			t.Fatalf("exact %s finite-feed recovery rejected: action=%d err=%v", tc.disposition, action, err)
		}
	}
	for _, tc := range []struct {
		name   string
		mutate func(*runfork.SelectedForkRecoveryResult)
	}{
		{"missing_resume", func(r *runfork.SelectedForkRecoveryResult) { r.Continuation = nil }},
		{"wrong_child_status", func(r *runfork.SelectedForkRecoveryResult) { r.Continuation.ForkRunStatus = "running" }},
		{"foreign_run", func(r *runfork.SelectedForkRecoveryResult) { r.RunID = uuid.NewString() }},
		{"invalid_execution", func(r *runfork.SelectedForkRecoveryResult) { r.ExecutionID = "not-a-predecessor" }},
		{"foreign_source", func(r *runfork.SelectedForkRecoveryResult) { r.Operation.Request.SourceRunID = uuid.NewString() }},
		{"wrong_revision", func(r *runfork.SelectedForkRecoveryResult) { r.Operation.Request.ResolvedPoint.Revision++ }},
		{"missing_point", func(r *runfork.SelectedForkRecoveryResult) { r.Operation.Request.ResolvedPoint = nil }},
		{"wrong_bundle", func(r *runfork.SelectedForkRecoveryResult) { r.Operation.Request.TargetBundleHash = "other" }},
		{"wrong_selection", func(r *runfork.SelectedForkRecoveryResult) {
			r.Operation.Request.ContractSelection.Mode = runfork.RunForkContractSelectionModeBundleHash
		}},
		{"invalid_operation", func(r *runfork.SelectedForkRecoveryResult) { r.Operation.Request.Actor = "" }},
		{"missing_pins", func(r *runfork.SelectedForkRecoveryResult) { r.Continuation.Pins = nil }},
		{"foreign_pin", func(r *runfork.SelectedForkRecoveryResult) { r.Continuation.Pins[0].RunID = uuid.NewString() }},
		{"duplicate_pin", func(r *runfork.SelectedForkRecoveryResult) {
			r.Continuation.Pins = append(r.Continuation.Pins, r.Continuation.Pins[0])
		}},
		{"payload_on_terminal", func(r *runfork.SelectedForkRecoveryResult) { r.Disposition = runfork.SelectedForkRecoveryTerminal }},
	} {
		for _, disposition := range []runfork.SelectedForkRecoveryDisposition{runfork.SelectedForkRecoveryResume, runfork.SelectedForkRecoveryActivate} {
			t.Run(string(disposition)+"/"+tc.name, func(t *testing.T) {
				result := valid
				result.Disposition = disposition
				resume := *valid.Continuation
				resume.Pins = append([]durabledata.Pin(nil), valid.Continuation.Pins...)
				operation := *valid.Operation
				point := *operation.Request.ResolvedPoint
				operation.Request.ResolvedPoint = &point
				result.Operation = &operation
				result.Continuation = &resume
				tc.mutate(&result)
				if action, err := selectedRecoveryActionFor(result, entry); err == nil || action != 0 {
					t.Fatalf("contradictory recovery action=%d err=%v", action, err)
				}
			})
		}
	}
	valid.ExecutionID = ""
	if action, err := selectedRecoveryActionFor(valid, entry); err != nil || action != selectedRecoveryResume {
		t.Fatalf("first attachment without predecessor rejected: action=%d err=%v", action, err)
	}
	valid.Disposition = runfork.SelectedForkRecoveryActivate
	if _, err := selectedRecoveryActionFor(valid, entry); err == nil {
		t.Fatal("quiesced activation without predecessor was admitted")
	}
}

func TestSelectedRecoveryAllTypedCutsAndActivatedAcknowledgment(t *testing.T) {
	for _, kind := range []runfork.RunForkPointKind{runfork.RunForkPointRunStart, runfork.RunForkPointEvent, runfork.RunForkPointDeploymentRevision} {
		t.Run(string(kind), func(t *testing.T) {
			recovered, entry := selectedRecoveryEvidence()
			point := runfork.RunForkPoint{Kind: kind, Revision: 4}
			if kind == runfork.RunForkPointEvent {
				point.EventID = uuid.NewString()
			}
			entry.Binding.ForkPoint, entry.Binding.ForkEventID = point, point.EventID
			recovered.Operation.Request.ResolvedPoint = &point
			recovered.Operation.Request.AtStart = kind == runfork.RunForkPointRunStart
			recovered.Operation.Request.ForkEventID = point.EventID
			recovered.Operation.Request.DataPinOverrides = nil
			operation, hash, err := recovered.Operation.Request.Canonical()
			if err != nil {
				t.Fatal(err)
			}
			recovered.Operation.Request, recovered.Operation.SemanticHash = operation, hash
			recovered.Continuation.Pins = nil
			recovered.ExecutionID = ""
			if action, err := selectedRecoveryActionFor(recovered, entry); err != nil || action != selectedRecoveryResume {
				t.Fatalf("eventless first attachment requires no invented feed: action=%d err=%v", action, err)
			}
			recovered.ExecutionID = uuid.NewString()
			recovered.Operation.Status = runfork.ForkOperationActivated
			recovered.Operation.Result = &runfork.ForkOperationResult{
				SourceRunID: operation.SourceRunID, SourceRunStatus: runfork.RunForkSourceFrozenStatus, SourceFrozen: true,
				ForkRunID: recovered.RunID, ForkRunStatus: runfork.RunForkActivatedStatus,
				ForkPoint: point, ForkEventID: point.EventID, BundleHash: entry.BundleHash, ExecutedEventCount: 2,
			}
			recovered.Continuation.ForkRunStatus = runfork.RunForkActivatedStatus
			if action, err := selectedRecoveryActionFor(recovered, entry); err != nil || action != selectedRecoveryResume {
				t.Fatalf("activated running child rejected: action=%d err=%v", action, err)
			}
			recovered.Continuation.ForkRunStatus = runfork.RunForkMaterializedStatus
			if action, err := selectedRecoveryActionFor(recovered, entry); err != nil || action != selectedRecoveryResume {
				t.Fatalf("paused-after-activation child rejected: action=%d err=%v", action, err)
			}
			ack, present, err := recovered.ActivatedResult()
			if err != nil || !present || ack.ExecutedEventCount != 2 || ack.ForkPoint != point || ack.ForkRunStatus != runfork.RunForkActivatedStatus {
				t.Fatalf("activated acknowledgment was reconstructed: result=%+v present=%t err=%v", ack, present, err)
			}
			recovered.ExecutionID = ""
			if _, err := selectedRecoveryActionFor(recovered, entry); err == nil {
				t.Fatal("activated child resumed without predecessor")
			}
			recovered.ExecutionID = uuid.NewString()
			recovered.Disposition = runfork.SelectedForkRecoveryActivate
			if _, err := selectedRecoveryActionFor(recovered, entry); err == nil {
				t.Fatal("activated child was readmitted to activation")
			}
			for _, disposition := range []runfork.SelectedForkRecoveryDisposition{runfork.SelectedForkRecoveryTerminal, runfork.SelectedForkRecoveryControlOnly} {
				recovered.Disposition, recovered.Continuation = disposition, nil
				if _, err := selectedRecoveryActionFor(recovered, entry); err != nil {
					t.Fatalf("read/control-only recovery rejected: %v", err)
				}
				if _, present, err := recovered.ActivatedResult(); err != nil || !present {
					t.Fatalf("read/control-only recovery lost acknowledgment: present=%t err=%v", present, err)
				}
			}
		})
	}
}
