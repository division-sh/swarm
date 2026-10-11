package runforkexecution

import (
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/runfork"
)

func stagedReplyBlockedAdmission(withTimer bool) runfork.RunForkReplayResumeAdmission {
	admission := runfork.RunForkReplayResumeAdmission{
		Owner: runfork.RunForkReplayResumeAdmissionOwner, ReplayResumeFactsPresent: true,
		Dispositions: []runfork.RunForkReplayResumeDisposition{{
			Fact:        runfork.RunForkReplayResumeFactOpenReplyContext,
			Disposition: runfork.RunForkReplayResumeDispositionFailClosedBlocker,
			BlockerCode: runfork.RunForkBlockerOpenReplyContextUnsupported,
		}},
		UnsupportedBlockers: []runfork.RunForkUnsupportedBlocker{{Code: runfork.RunForkBlockerOpenReplyContextUnsupported}},
	}
	if withTimer {
		timer := stagedTimerBlockedAdmission()
		admission.Dispositions = append(admission.Dispositions, timer.Dispositions...)
		admission.UnsupportedBlockers = append(admission.UnsupportedBlockers, timer.UnsupportedBlockers...)
	}
	return admission
}

func TestStagedReplyGateReachesOnlyNativeReadback(t *testing.T) {
	for _, withTimer := range []bool{false, true} {
		admission := stagedReplyBlockedAdmission(withTimer)
		if !selectedContractStagedReadbackPending(admission) {
			t.Fatalf("exact pending reply readback is unreachable (timer=%t)", withTimer)
		}
		if !reflect.DeepEqual(admission, stagedReplyBlockedAdmission(withTimer)) || admission.StateOnlyExecutionReady ||
			admission.DeliveryEventReplayReady || admission.BoundedReplaySupported {
			t.Fatal("readback routing granted reply or execution authority")
		}
	}
}

func TestStagedReplyGatePreservesUnrelatedAndContradictoryEvidence(t *testing.T) {
	for _, name := range []string{"owner", "facts", "absent", "duplicate_blocker", "duplicate_fact", "missing_fact", "wrong_fact", "wrong_code", "reconstructed", "session"} {
		t.Run(name, func(t *testing.T) {
			admission := stagedReplyBlockedAdmission(true)
			switch name {
			case "owner":
				admission.Owner = "foreign"
			case "facts":
				admission.ReplayResumeFactsPresent = false
			case "absent":
				admission.UnsupportedBlockers = nil
			case "duplicate_blocker":
				admission.UnsupportedBlockers = append(admission.UnsupportedBlockers, admission.UnsupportedBlockers[0])
			case "duplicate_fact":
				admission.Dispositions = append(admission.Dispositions, admission.Dispositions[0])
			case "missing_fact":
				admission.Dispositions = admission.Dispositions[1:]
			case "wrong_fact":
				admission.Dispositions[0].Fact = runfork.RunForkReplayResumeFactSessionHistory
			case "wrong_code":
				admission.Dispositions[0].BlockerCode = runfork.RunForkBlockerTimerHistoryUnproven
			case "reconstructed":
				admission.Dispositions[0].Disposition = runfork.RunForkReplayResumeDispositionReconstruct
			case "session":
				admission.UnsupportedBlockers = append(admission.UnsupportedBlockers, runfork.RunForkUnsupportedBlocker{Code: runfork.RunForkBlockerSessionHistoryUnproven})
			}
			if selectedContractStagedReadbackPending(admission) {
				t.Fatal("incomplete or unrelated history reached reply readback")
			}
		})
	}
}
