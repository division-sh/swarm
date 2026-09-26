package runforkexecution

import (
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/durabledata"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/google/uuid"
)

func selectedFiniteFeedRecoveryEvidence() (runfork.SelectedForkRecoveryResult, runfork.SelectedForkRecoveryEntry) {
	point := runfork.RunForkPoint{Kind: runfork.RunForkPointDeploymentRevision, Revision: 3}
	selection := runfork.RunForkContractSelection{Mode: runfork.RunForkContractSelectionModeSelectedContracts}
	bundle := "bundle-v2:sha256:" + strings.Repeat("a", 64)
	entry := runfork.SelectedForkRecoveryEntry{
		BundleHash: bundle,
		Binding: runfork.RunForkSelectedContractBinding{
			ForkRunID: uuid.NewString(), SourceRunID: uuid.NewString(),
			ForkPoint: point, ContractSelection: selection,
		},
	}
	return runfork.SelectedForkRecoveryResult{
		RunID: entry.Binding.ForkRunID, ExecutionID: uuid.NewString(),
		Disposition: runfork.SelectedForkRecoveryResumeFiniteFeed,
		Resume: &runfork.SelectedForkFiniteFeedResume{ForkRunStatus: runfork.RunForkMaterializedStatus, Pins: []durabledata.Pin{{
			RunID: entry.Binding.ForkRunID, RunState: "paused", Declaration: durabledata.DeclarationRef{FlowPath: ".", EventName: "root.ready"},
			SchemaDigest: durabledata.SchemaDigest("resource-schema-v1:sha256:" + strings.Repeat("b", 64)),
			VersionID:    durabledata.VersionID("resource-version-v1:sha256:" + strings.Repeat("c", 64)), Selection: "fork_override",
		}}, Operation: runfork.ForkOperationRequest{
			OperationID: uuid.NewString(), Actor: "operator", TransportHash: "transport-hash",
			SourceRunID: entry.Binding.SourceRunID, ResolvedPoint: &point,
			TargetBundleHash: bundle, ContractSelection: selection,
		}},
	}, entry
}

func TestSelectedRecoveryBootActionsAreClosed(t *testing.T) {
	_, entry := selectedFiniteFeedRecoveryEvidence()
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

func TestSelectedFiniteFeedRecoveryRequiresExactTypedOperation(t *testing.T) {
	valid, entry := selectedFiniteFeedRecoveryEvidence()
	for _, tc := range []struct {
		disposition runfork.SelectedForkRecoveryDisposition
		want        selectedRecoveryAction
	}{
		{runfork.SelectedForkRecoveryResumeFiniteFeed, selectedRecoveryResumeFiniteFeed},
		{runfork.SelectedForkRecoveryActivateFiniteFeed, selectedRecoveryActivateFiniteFeed},
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
		{"missing_resume", func(r *runfork.SelectedForkRecoveryResult) { r.Resume = nil }},
		{"wrong_child_status", func(r *runfork.SelectedForkRecoveryResult) { r.Resume.ForkRunStatus = "running" }},
		{"foreign_run", func(r *runfork.SelectedForkRecoveryResult) { r.RunID = uuid.NewString() }},
		{"missing_execution", func(r *runfork.SelectedForkRecoveryResult) { r.ExecutionID = "" }},
		{"foreign_source", func(r *runfork.SelectedForkRecoveryResult) { r.Resume.Operation.SourceRunID = uuid.NewString() }},
		{"wrong_revision", func(r *runfork.SelectedForkRecoveryResult) { r.Resume.Operation.ResolvedPoint.Revision++ }},
		{"missing_point", func(r *runfork.SelectedForkRecoveryResult) { r.Resume.Operation.ResolvedPoint = nil }},
		{"wrong_bundle", func(r *runfork.SelectedForkRecoveryResult) { r.Resume.Operation.TargetBundleHash = "other" }},
		{"wrong_selection", func(r *runfork.SelectedForkRecoveryResult) {
			r.Resume.Operation.ContractSelection.Mode = runfork.RunForkContractSelectionModeBundleHash
		}},
		{"invalid_operation", func(r *runfork.SelectedForkRecoveryResult) { r.Resume.Operation.Actor = "" }},
		{"missing_pins", func(r *runfork.SelectedForkRecoveryResult) { r.Resume.Pins = nil }},
		{"foreign_pin", func(r *runfork.SelectedForkRecoveryResult) { r.Resume.Pins[0].RunID = uuid.NewString() }},
		{"duplicate_pin", func(r *runfork.SelectedForkRecoveryResult) { r.Resume.Pins = append(r.Resume.Pins, r.Resume.Pins[0]) }},
		{"payload_on_terminal", func(r *runfork.SelectedForkRecoveryResult) { r.Disposition = runfork.SelectedForkRecoveryTerminal }},
	} {
		for _, disposition := range []runfork.SelectedForkRecoveryDisposition{runfork.SelectedForkRecoveryResumeFiniteFeed, runfork.SelectedForkRecoveryActivateFiniteFeed} {
			t.Run(string(disposition)+"/"+tc.name, func(t *testing.T) {
				result := valid
				result.Disposition = disposition
				resume := *valid.Resume
				resume.Pins = append([]durabledata.Pin(nil), valid.Resume.Pins...)
				point := *valid.Resume.Operation.ResolvedPoint
				resume.Operation.ResolvedPoint = &point
				result.Resume = &resume
				tc.mutate(&result)
				if action, err := selectedRecoveryActionFor(result, entry); err == nil || action != 0 {
					t.Fatalf("contradictory recovery action=%d err=%v", action, err)
				}
			})
		}
	}
}
