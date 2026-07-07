package apispec

import "testing"

func TestPlatformSpecVerifierFindingAuthoringGuidelines(t *testing.T) {
	root := loadPlatformSpecYAMLNode(t)
	engine := mustMappingValue(t, root, "engine")
	bootVerification := mustMappingValue(t, engine, "boot_verification")
	implementationNotes := mustMappingValue(t, bootVerification, "implementation_notes")
	guidelines := mustMappingValue(t, implementationNotes, "authoring_guidelines")

	for _, key := range []string{
		"rule",
		"problem_statement",
		"remediation",
		"evidence",
		"gold_standard",
		"good_example",
		"bad_example",
		"split_boundaries",
	} {
		if !hasMappingKey(guidelines, key) {
			t.Fatalf("engine.boot_verification.implementation_notes.authoring_guidelines.%s missing", key)
		}
	}

	assertScalarContains(t, mustMappingValue(t, guidelines, "rule"), "contract authors")
	assertScalarContains(t, mustMappingValue(t, guidelines, "rule"), "not Go functions")
	assertScalarContains(t, mustMappingValue(t, guidelines, "rule"), "internal GitHub issue/tracker numbers")
	assertScalarContains(t, mustMappingValue(t, guidelines, "rule"), "must not reference internal issue numbers or trackers")
	assertScalarContains(t, mustMappingValue(t, guidelines, "rule"), "public docs URL")
	assertScalarContains(t, mustMappingValue(t, guidelines, "problem_statement"), "`message` field")
	assertScalarContains(t, mustMappingValue(t, guidelines, "problem_statement"), "`remediation`")
	assertScalarContains(t, mustMappingValue(t, guidelines, "remediation"), "imperative")
	assertScalarContains(t, mustMappingValue(t, guidelines, "remediation"), "Fix one of:")
	assertScalarContains(t, mustMappingValue(t, guidelines, "evidence"), "what was checked")
	assertScalarContains(t, mustMappingValue(t, guidelines, "evidence"), "must not expose internal runtime bookkeeping")
	assertScalarContains(t, mustMappingValue(t, guidelines, "gold_standard"), "workflow_accumulator_safety_checks.go")
	assertScalarContains(t, mustMappingValue(t, guidelines, "good_example"), "Fix one of:")
	assertScalarContains(t, mustMappingValue(t, guidelines, "bad_example"), "no imperative remediation")
	assertScalarContains(t, mustMappingValue(t, guidelines, "split_boundaries"), "#1746")
	assertScalarContains(t, mustMappingValue(t, guidelines, "split_boundaries"), "#1786")
	assertScalarContains(t, mustMappingValue(t, guidelines, "split_boundaries"), "broader CLI output-conformance lane")
}
