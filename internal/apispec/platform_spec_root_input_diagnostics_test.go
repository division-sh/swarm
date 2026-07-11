package apispec

import "testing"

func TestPlatformSpecOwnsRootInputRejectionDiagnostics(t *testing.T) {
	root := loadPlatformSpecYAMLNode(t)
	outputContract := mustYAMLPath(t, root, "cli_specification", "foundations", "output_contract")
	convention := mustMappingValue(t, outputContract, "diagnostic_convention")
	contentRules := mustMappingValue(t, convention, "diagnostic_content_rules")
	rendering := mustMappingValue(t, contentRules, "root_input_rejections")
	for _, want := range []string{
		"not_declared_root_input",
		"declared_root_input_not_routable",
		"server-owned root-input facts",
		"pins.inputs.events",
		"none",
		"MUST NOT reload contracts",
	} {
		assertScalarContains(t, rendering, want)
	}

	contract := mustYAMLPath(t, root, "api_specification", "components", "error_catalog_metadata", "root_input_rejection_contract")
	assertScalarContains(t, mustMappingValue(t, contract, "canonical_owner"), "RootInputValidationError")
	reasons := mustMappingValue(t, contract, "reasons")
	assertScalarContains(t, mustMappingValue(t, reasons, "not_declared_root_input"), "absent from the root flow")
	assertScalarContains(t, mustMappingValue(t, reasons, "declared_root_input_not_routable"), "no derived runtime route")
	assertScalarContains(t, mustMappingValue(t, contract, "rule"), "deterministically sorted declared_events")
	assertScalarContains(t, mustMappingValue(t, contract, "rule"), "do not select the reason again")
	assertScalarContains(t, mustMappingValue(t, contract, "absent_root_schema_rule"), "empty declared and routable root-input domain")
	assertScalarContains(t, mustMappingValue(t, contract, "post_validation_rule"), "EVENT_PUBLISH_FAILED")
	assertScalarContains(t, mustMappingValue(t, contract, "post_validation_rule"), "non-retryable publish contradiction")
	assertScalarContains(t, mustMappingValue(t, contract, "post_validation_rule"), "separate event-catalog concept")
}
