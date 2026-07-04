package bootverify

import (
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func (c *checkerContext) platformVersionCompatibility() []Finding {
	bundle, ok := semanticview.Bundle(c.source)
	if !ok {
		return []Finding{{
			CheckID:  "platform_version_compatibility",
			Severity: SeverityHardInvalidity,
			Message:  "platform version compatibility requires a bundle-backed semantic source",
			Location: "global",
		}}
	}
	findings := make([]Finding, 0)
	for _, violation := range runtimecontracts.BundlePlatformVersionCompatibilityViolations(bundle) {
		findings = append(findings, Finding{
			CheckID:  "platform_version_compatibility",
			Severity: SeverityHardInvalidity,
			Message:  violation.Message(),
			Location: violation.Location(),
		})
	}
	return findings
}
