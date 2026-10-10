package runforkexecution

import (
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/runfork"
)

func stagedTimerBlockedAdmission() runfork.RunForkReplayResumeAdmission {
	return runfork.RunForkReplayResumeAdmission{
		Owner: runfork.RunForkReplayResumeAdmissionOwner, ReplayResumeFactsPresent: true,
		Dispositions: []runfork.RunForkReplayResumeDisposition{{
			Fact:        runfork.RunForkReplayResumeFactTimerHistory,
			Disposition: runfork.RunForkReplayResumeDispositionFailClosedBlocker,
			BlockerCode: runfork.RunForkBlockerTimerHistoryUnproven,
		}},
		UnsupportedBlockers: []runfork.RunForkUnsupportedBlocker{{Code: runfork.RunForkBlockerTimerHistoryUnproven}},
	}
}

func TestStagedWorkflowTimerGateDefersOnlyNativeReadbackWithoutGrantingReadiness(t *testing.T) {
	admission := stagedTimerBlockedAdmission()
	if !selectedContractStagedTimerReadbackPending(admission) {
		t.Fatal("timer-only source blocker cannot reach its canonical native proof")
	}
	if !reflect.DeepEqual(admission, stagedTimerBlockedAdmission()) || admission.StateOnlyExecutionReady ||
		admission.DeliveryEventReplayReady || admission.BoundedReplaySupported {
		t.Fatal("attempting native readback mutated the source blocker or granted execution")
	}
	// Source inventory certification and generic-timer refusal belong to the
	// native owner, not this pre-proof routing predicate.
	if admission.Dispositions[0].Owner != "" || admission.Dispositions[0].Classification != "" {
		t.Fatal("gate manufactured source inventory authority")
	}
}

func TestStagedWorkflowTimerGatePreservesOtherAndIncompleteBlockers(t *testing.T) {
	for _, name := range []string{"owner", "missing_facts", "missing_blocker", "duplicate_blocker", "missing_disposition", "duplicate_disposition", "stale_reconstruct", "foreign_fact", "foreign_fact_code", "other_blocker"} {
		t.Run(name, func(t *testing.T) {
			admission := stagedTimerBlockedAdmission()
			switch name {
			case "owner":
				admission.Owner = "foreign-owner"
			case "missing_facts":
				admission.ReplayResumeFactsPresent = false
			case "missing_blocker":
				admission.UnsupportedBlockers = nil
			case "duplicate_blocker":
				admission.UnsupportedBlockers = append(admission.UnsupportedBlockers, admission.UnsupportedBlockers[0])
			case "missing_disposition":
				admission.Dispositions = nil
			case "duplicate_disposition":
				admission.Dispositions = append(admission.Dispositions, admission.Dispositions[0])
			case "stale_reconstruct":
				admission.Dispositions[0].Disposition = runfork.RunForkReplayResumeDispositionReconstruct
			case "foreign_fact":
				admission.Dispositions = append(admission.Dispositions, runfork.RunForkReplayResumeDisposition{
					Fact: runfork.RunForkReplayResumeFactOpenReplyContext, Disposition: runfork.RunForkReplayResumeDispositionFailClosedBlocker,
				})
			case "foreign_fact_code":
				admission.Dispositions = append(admission.Dispositions, runfork.RunForkReplayResumeDisposition{
					Fact: runfork.RunForkReplayResumeFactSessionHistory, Disposition: runfork.RunForkReplayResumeDispositionReconstruct,
					BlockerCode: runfork.RunForkBlockerSessionHistoryUnproven,
				})
			case "other_blocker":
				admission.UnsupportedBlockers = append(admission.UnsupportedBlockers, runfork.RunForkUnsupportedBlocker{
					Code: runfork.RunForkBlockerOpenReplyContextUnsupported,
				})
			}
			if selectedContractStagedTimerReadbackPending(admission) {
				t.Fatal("incomplete timer evidence or unrelated blocker reached the retained mutation path")
			}
		})
	}
}

func stagedTimerMaterializationProof() runfork.RunForkMaterialization {
	return runfork.RunForkMaterialization{
		ForkRunID: "11111111-1111-4111-8111-111111111111", SourceRunID: "22222222-2222-4222-8222-222222222222",
		ForkPoint: runfork.RunForkPoint{Kind: runfork.RunForkPointRunStart, Revision: 7},
		ReplayResumeAdmission: runfork.RunForkReplayResumeAdmission{
			Owner: runfork.RunForkReplayResumeAdmissionOwner, ReplayResumeFactsPresent: true,
			Dispositions: []runfork.RunForkReplayResumeDisposition{{
				Fact: runfork.RunForkReplayResumeFactTimerHistory, Disposition: runfork.RunForkReplayResumeDispositionReconstruct,
			}},
		},
	}
}

func TestStagedWorkflowTimerGateConsumesMaterializationAdmissionNotExecutionFlag(t *testing.T) {
	proof := stagedTimerMaterializationProof()
	got, err := selectedContractStagedMaterializationAdmission(proof, proof.ForkRunID, proof.SourceRunID, proof.ForkPoint)
	if err != nil || !reflect.DeepEqual(got, proof.ReplayResumeAdmission) || proof.ExecutionReady || got.StateOnlyExecutionReady || got.DeliveryEventReplayReady {
		t.Fatalf("exact materialized timer readback was mistaken for execution: admission=%+v proof=%+v err=%v", got, proof, err)
	}
}

func TestStagedWorkflowTimerGateRejectsMaterializationMismatchAndRemainingBlockers(t *testing.T) {
	for _, name := range []string{"missing_child", "foreign_child", "foreign_source", "wrong_point", "foreign_owner", "materialization_blocker", "admission_blocker"} {
		t.Run(name, func(t *testing.T) {
			proof := stagedTimerMaterializationProof()
			child, source, point := proof.ForkRunID, proof.SourceRunID, proof.ForkPoint
			switch name {
			case "missing_child":
				proof.ForkRunID = ""
			case "foreign_child":
				proof.ForkRunID = source
			case "foreign_source":
				proof.SourceRunID = child
			case "wrong_point":
				proof.ForkPoint.Revision++
			case "foreign_owner":
				proof.ReplayResumeAdmission.Owner = "foreign-owner"
			case "materialization_blocker":
				proof.UnsupportedBlockers = []runfork.RunForkUnsupportedBlocker{{Code: runfork.RunForkBlockerTimerHistoryUnproven}}
			case "admission_blocker":
				proof.ReplayResumeAdmission.UnsupportedBlockers = []runfork.RunForkUnsupportedBlocker{{Code: runfork.RunForkBlockerOpenReplyContextUnsupported}}
			}
			proof.ExecutionReady = true
			if _, err := selectedContractStagedMaterializationAdmission(proof, child, source, point); err == nil {
				t.Fatal("execution-ready flag hid an inexact readback or remaining blocker")
			}
		})
	}
}
