package runforkexecution

import (
	"errors"
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/runfork"
)

func selectedContractStagedMaterializationAdmission(materialization runfork.RunForkMaterialization, forkRunID, sourceRunID string, point runfork.RunForkPoint) (runfork.RunForkReplayResumeAdmission, error) {
	if materialization.ForkRunID != forkRunID || materialization.SourceRunID != sourceRunID ||
		!sameSelectedForkPointIdentity(materialization.ForkPoint, point) {
		return runfork.RunForkReplayResumeAdmission{}, errors.New("selected staged materialization readback differs from the exact child and cut")
	}
	admission := materialization.ReplayResumeAdmission
	if admission.Owner != runfork.RunForkReplayResumeAdmissionOwner {
		return runfork.RunForkReplayResumeAdmission{}, errors.New("selected staged materialization readback lacks canonical replay admission")
	}
	if len(materialization.UnsupportedBlockers) != 0 {
		return runfork.RunForkReplayResumeAdmission{}, fmt.Errorf("selected staged materialization remains blocked before issuance: %s", selectedContractBlockerCodes(materialization.UnsupportedBlockers))
	}
	if len(admission.UnsupportedBlockers) != 0 {
		return runfork.RunForkReplayResumeAdmission{}, fmt.Errorf("selected staged replay admission remains blocked before issuance: %s", selectedContractBlockerCodes(admission.UnsupportedBlockers))
	}
	return admission, nil
}

// This permits reaching the native proof, not executing or discharging history.
// Timer history and reply rows remain blocked until their canonical readback.
func selectedContractStagedReadbackPending(admission runfork.RunForkReplayResumeAdmission) bool {
	if admission.Owner != runfork.RunForkReplayResumeAdmissionOwner || !admission.ReplayResumeFactsPresent {
		return false
	}
	pending := make(map[string]string, 2)
	for _, blocker := range admission.UnsupportedBlockers {
		var fact string
		switch blocker.Code {
		case runfork.RunForkBlockerTimerHistoryUnproven:
			fact = runfork.RunForkReplayResumeFactTimerHistory
		case runfork.RunForkBlockerOpenReplyContextUnsupported:
			fact = runfork.RunForkReplayResumeFactOpenReplyContext
		default:
			return false
		}
		if _, duplicate := pending[fact]; duplicate {
			return false
		}
		pending[fact] = blocker.Code
	}
	if len(pending) == 0 {
		return false
	}
	seen := make(map[string]bool, len(pending))
	for _, disposition := range admission.Dispositions {
		if code, exists := pending[disposition.Fact]; exists {
			if seen[disposition.Fact] || disposition.Disposition != runfork.RunForkReplayResumeDispositionFailClosedBlocker || disposition.BlockerCode != code {
				return false
			}
			seen[disposition.Fact] = true
			continue
		}
		if disposition.Disposition == runfork.RunForkReplayResumeDispositionFailClosedBlocker || disposition.BlockerCode != "" {
			return false
		}
	}
	return len(seen) == len(pending)
}

func selectedContractBlockerCodes(blockers []runfork.RunForkUnsupportedBlocker) string {
	if len(blockers) == 0 {
		return "none"
	}
	codes := make([]string, 0, len(blockers))
	for _, blocker := range blockers {
		if code := strings.TrimSpace(blocker.Code); code != "" {
			codes = append(codes, code)
		}
	}
	if len(codes) == 0 {
		return "none"
	}
	return strings.Join(codes, ",")
}
