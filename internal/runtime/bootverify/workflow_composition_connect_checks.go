package bootverify

import (
	"fmt"
	"strings"

	runtimepinrouting "github.com/division-sh/swarm/internal/runtime/core/pinrouting"
)

func checkCompositionConnectValidation(c *checkerContext) []Finding {
	if c == nil || c.source == nil {
		return nil
	}
	var findings []Finding
	graph := runtimepinrouting.CompileConnectGraph(c.source)
	for _, issue := range graph.Issues() {
		context := issue.DiagnosticContext()
		if context != "" {
			context += ": "
		}
		findings = append(findings, Finding{
			CheckID: "composition_connect_validation", Severity: "error",
			Location: strings.TrimSpace(issue.AuthoredLocation),
			Message:  fmt.Sprintf("%scompiled connect is invalid: %s: %s", context, issue.Failure.Code(), strings.TrimSpace(issue.Detail)),
			Evidence: []string{"classification: " + issue.Failure.Code()},
		})
	}
	for _, collision := range graph.ReceiverPinCollisions() {
		findings = append(findings, Finding{
			CheckID:     "composition_connect_validation",
			Severity:    "error",
			Location:    strings.TrimSpace(collision.AuthoredLocation()),
			Message:     collision.Message(),
			Remediation: "Route the source event to distinct subscribers or targets, or consolidate the receiver pins behind one handler. One event x subscriber cannot select multiple receiver-local handlers.",
			Evidence: []string{
				"classification: " + runtimepinrouting.ConnectReceiverPinCollisionFailure,
				"source: " + collision.SourceDiagnostic(),
				"subscriber: " + collision.SubscriberType() + ":" + collision.SubscriberID(),
			},
		})
	}
	return findings
}
