package bootverify

import (
	"fmt"
	"sort"
	"strings"

	"github.com/division-sh/swarm/internal/events"
	c "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/pinrouting"
)

func checkFlowConstructorValidation(checker *checkerContext) []Finding {
	var findings []Finding
	edges := pinrouting.CompileConnectGraph(checker.source).Edges()
	var flows []string
	for id := range checker.source.FlowSchemaEntries() {
		flows = append(flows, id)
	}
	sort.Strings(flows)
	for _, flowID := range flows {
		constructors := checker.constructorContracts(flowID)
		eligible := 0
		var refusals []string
		for _, constructor := range constructors {
			if constructor.Eligible() {
				eligible++
				continue
			}
			refusal := fmt.Sprintf("%s cannot create flow %s: %s", constructor.Input(), flowID, strings.Join(constructor.Refusals(), "; "))
			refusals = append(refusals, refusal)
			for _, edge := range edges {
				mode := edge.ResolutionMode()
				if edge.Consumer().Matches(flowID, events.EventType(constructor.Input())) && (mode == c.FlowInputResolutionModeCreate || mode == c.FlowInputResolutionModeSelectOrCreate) {
					findings = append(findings, Finding{CheckID: "flow_constructor_validation", Severity: SeverityHardInvalidity, Location: flowID, Message: refusal})
					break
				}
			}
		}
		if eligible == 0 {
			findings = append(findings, Finding{CheckID: "flow_constructor_validation", Severity: SeverityHardInvalidity, Location: flowID, Message: fmt.Sprintf("flow %s has no eligible constructor: %s", flowID, strings.Join(refusals, "; "))})
		}
	}
	return findings
}
