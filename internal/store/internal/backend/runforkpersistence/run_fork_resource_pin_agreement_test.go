package runforkpersistence

import (
	"testing"

	"github.com/division-sh/swarm/internal/durabledata"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/runtime/runfork"
)

func TestForkResourceSourcePinAgreementUsesFixedRevisionOutstandingWork(t *testing.T) {
	ref := durabledata.DeclarationRef{FlowPath: "portfolio", EventName: "portfolio/company.lead"}
	other := durabledata.DeclarationRef{FlowPath: "portfolio", EventName: "portfolio/company.other"}
	oldVersion := durabledata.VersionID("old-version")
	newVersion := durabledata.VersionID("new-version")
	intent := fanoutobligation.Intent{
		Source:  fanoutobligation.SourceRef{Kind: fanoutobligation.SourceResourceVersion, Declaration: ref, VersionID: oldVersion},
		Request: fanoutobligation.IntentRequest{Cardinality: 2},
		Status:  fanoutobligation.StatusOpen,
	}
	pins := func(version durabledata.VersionID) []durabledata.Pin {
		return []durabledata.Pin{{Declaration: ref, VersionID: version}, {Declaration: other, VersionID: newVersion}}
	}
	for _, test := range []struct {
		name   string
		plan   runfork.RunForkPlan
		pins   []durabledata.Pin
		reject bool
	}{
		{"before-intent-alternate", runfork.RunForkPlan{}, pins(newVersion), false},
		{"unissued-alternate", runfork.RunForkPlan{FanOutObligations: []runfork.RunForkFanOutObligation{{Intent: intent}}}, pins(newVersion), true},
		{"unissued-same-version", runfork.RunForkPlan{FanOutObligations: []runfork.RunForkFanOutObligation{{Intent: intent}}}, pins(oldVersion), false},
		{"unissued-unrelated-only", runfork.RunForkPlan{FanOutObligations: []runfork.RunForkFanOutObligation{{Intent: intent}}}, []durabledata.Pin{{Declaration: other, VersionID: newVersion}}, true},
		{"settled-alternate", runfork.RunForkPlan{FanOutObligations: []runfork.RunForkFanOutObligation{{Intent: func() fanoutobligation.Intent {
			settled := intent
			settled.Cursor = 2
			settled.Status = fanoutobligation.StatusClosed
			return settled
		}()}}}, pins(newVersion), false},
		{"issued-unsettled-alternate", runfork.RunForkPlan{FanOutObligations: []runfork.RunForkFanOutObligation{{Intent: func() fanoutobligation.Intent { issued := intent; issued.Cursor = 2; return issued }(), PendingReplays: []runfork.RunForkFanOutPendingReplay{{Ordinal: 1}}}}}, pins(newVersion), true},
		{"duplicate-pin", runfork.RunForkPlan{}, append(pins(oldVersion), durabledata.Pin{Declaration: ref, VersionID: oldVersion}), true},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := requireForkResourceSourcePinAgreement(test.plan, test.pins)
			if (err != nil) != test.reject {
				t.Fatalf("fixed-revision agreement: reject=%t err=%v", test.reject, err)
			}
		})
	}
}
