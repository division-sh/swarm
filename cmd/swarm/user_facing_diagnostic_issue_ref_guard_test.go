package main

import (
	"regexp"
	"testing"
)

var commandUserFacingDiagnosticInternalRefPattern = regexp.MustCompile(`#\d{3,}|\bimplemented_[A-Za-z0-9_#-]+\b|\b(?:tracked by|split to|unsupported by|first-slice source authority)\b`)

func TestSwarmEnvUserFacingDiagnosticsDoNotLeakInternalIssueRefs(t *testing.T) {
	for _, entry := range swarmEnvCatalogEntries() {
		name := entry.Name
		if name == "" {
			name = entry.Prefix + "EXAMPLE"
		}
		finding := findingForSwarmEnvEntry(name, entry)
		assertNoUserFacingIssueRef(t, "env message "+name, finding.Message)
		assertNoUserFacingIssueRef(t, "env remediation "+name, finding.Remediation)
		assertNoUserFacingIssueRef(t, "env rendered "+name, formatSwarmEnvFinding(finding))
	}
}

func TestDoctorTargetUserFacingDiagnosticsDoNotLeakInternalIssueRefs(t *testing.T) {
	for _, class := range doctorTargetCommandClasses() {
		assertNoUserFacingIssueRef(t, "target command class "+class.Name+" status", class.Status)
		assertNoUserFacingIssueRef(t, "target command class "+class.Name+" fallthrough", class.Fallthrough)
	}
	for _, sibling := range doctorTargetSplitSiblings() {
		assertNoUserFacingIssueRef(t, "target split sibling", sibling)
	}

	entry := validateLocalContextEntry(nil, localContextEntry{Status: localContextStatusOK, Descriptor: localContextDescriptor{Transport: localContextTransportUnix}}, nil)
	assertNoUserFacingIssueRef(t, "local context detail", entry.Detail)
}

func assertNoUserFacingIssueRef(t *testing.T, label, text string) {
	t.Helper()
	if commandUserFacingDiagnosticInternalRefPattern.MatchString(text) {
		t.Fatalf("%s leaks internal issue/tracker ref: %q", label, text)
	}
}
