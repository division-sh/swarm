package store

import (
	"fmt"
	"strings"
)

const RunForkSelectedContractCommittedReplayScopeMarkerPolicyOwner = "runtime.run_fork.historical_replay_execution.selected_contract_committed_replay_scope_marker_policy"

// RunForkSelectedContractReplayResumeAdmission applies the selected-contract
// replay/resume policies that are allowed to reinterpret source-run facts before
// any selected-contract fork mutation.
func RunForkSelectedContractReplayResumeAdmission(plan RunForkPlan) RunForkReplayResumeAdmission {
	admission := RunForkSelectedContractActiveSourceDeliveryConversationCouplingAdmission(plan, plan.ReplayResumeAdmission)
	admission = runForkSelectedContractSessionTurnAuditLineageAdmission(plan, admission)
	return RunForkSelectedContractCommittedReplayScopeMarkerAdmission(admission)
}

// RunForkSelectedContractCommittedReplayScopeMarkerAdmission treats source
// committed replay-scope marker rows present at the selected revision as
// lineage/no-action evidence for selected-contract forks. It does not authorize copying source marker
// rows or using them as fork-local recovery proof.
func RunForkSelectedContractCommittedReplayScopeMarkerAdmission(admission RunForkReplayResumeAdmission) RunForkReplayResumeAdmission {
	if strings.TrimSpace(admission.Owner) == "" {
		admission.Owner = RunForkReplayResumeAdmissionOwner
	}

	changed := false
	for i := range admission.Dispositions {
		disposition := &admission.Dispositions[i]
		if strings.TrimSpace(disposition.Fact) != RunForkReplayResumeFactCommittedReplayScope {
			continue
		}
		if strings.TrimSpace(disposition.Disposition) != RunForkReplayResumeDispositionFailClosedBlocker {
			continue
		}
		if code := strings.TrimSpace(disposition.BlockerCode); code != "" && code != RunForkBlockerCommittedReplayScopeReplayUnsupported {
			continue
		}
		disposition.Disposition = RunForkReplayResumeDispositionLineageOnly
		disposition.Owner = RunForkSelectedContractCommittedReplayScopeMarkerPolicyOwner
		disposition.BlockerCode = ""
		disposition.Classification = ""
		disposition.Message = fmt.Sprintf("%s classifies source committed replay-scope marker rows present at the selected revision as selected-contract lineage/no-action evidence only; fork-local recovery proof must be written under the fork run_id by runtime execution or fork-local replay owners", RunForkSelectedContractCommittedReplayScopeMarkerPolicyOwner)
		changed = true
	}
	if !changed {
		return admission
	}

	filtered := make([]RunForkUnsupportedBlocker, 0, len(admission.UnsupportedBlockers))
	for _, blocker := range admission.UnsupportedBlockers {
		if strings.TrimSpace(blocker.Code) == RunForkBlockerCommittedReplayScopeReplayUnsupported {
			continue
		}
		filtered = append(filtered, blocker)
	}
	admission.UnsupportedBlockers = filtered
	return runForkReplayResumeAdmissionRecalculateReadiness(admission)
}

func runForkSelectedContractCommittedReplayScopeMarkerAdmitted(admission RunForkReplayResumeAdmission) bool {
	for _, disposition := range admission.Dispositions {
		if strings.TrimSpace(disposition.Fact) != RunForkReplayResumeFactCommittedReplayScope {
			continue
		}
		if strings.TrimSpace(disposition.Disposition) != RunForkReplayResumeDispositionLineageOnly {
			continue
		}
		if strings.TrimSpace(disposition.Owner) == RunForkSelectedContractCommittedReplayScopeMarkerPolicyOwner {
			return true
		}
	}
	return false
}
