package pipeline_test

import (
	"testing"

	"github.com/division-sh/swarm/internal/runtime/pipeline"
)

func TestIssue2564M21GateFirstReadAndCommitUseEntityFenceBothStores(t *testing.T) {
	pipeline.VerifyIssue2564SiblingEntityFenceForTest(t, newTimerCauseReplayNativeFixture, "gate")
}

func TestIssue2564M22CardFirstReadAndCommitUseEntityFenceBothStores(t *testing.T) {
	pipeline.VerifyIssue2564SiblingEntityFenceForTest(t, newTimerCauseReplayNativeFixture, "card")
}

func TestIssue2564M22CompatibilityCardFirstReadAndCommitUseEntityFenceBothStores(t *testing.T) {
	pipeline.VerifyIssue2564SiblingEntityFenceForTest(t, newTimerCauseReplayNativeFixture, "compatibility_card")
}

func TestIssue2564M23TerminationFirstReadAndCommitUseEntityFenceBothStores(t *testing.T) {
	pipeline.VerifyIssue2564SiblingEntityFenceForTest(t, newTimerCauseReplayNativeFixture, "termination")
}
