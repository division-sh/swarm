package apispec

import (
	"reflect"
	"strconv"
	"testing"

	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"gopkg.in/yaml.v3"
)

func TestPlatformFailureTaxonomyMatchesExecutableRegistry(t *testing.T) {
	root := loadPlatformSpecYAMLNode(t)
	engine := mustMappingValue(t, root, "engine")
	errorModel := mustMappingValue(t, engine, "error_model")
	taxonomy := mustMappingValue(t, errorModel, "failure_taxonomy")

	assertScalarValue(t, mustMappingValue(t, taxonomy, "owner"), "platform-spec.yaml#engine.error_model.failure_taxonomy")
	assertScalarValue(t, mustMappingValue(t, taxonomy, "executable_owner"), "internal/runtime/failures")
	assertScalarValue(t, mustMappingValue(t, mustMappingValue(t, taxonomy, "envelope"), "schema_version"), runtimefailures.EnvelopeSchemaVersion)
	authoritySources := mustMappingValue(t, mustMappingValue(t, taxonomy, "authority"), "sources")
	for source, status := range map[string]string{
		"same_process":                  "authoritative",
		"persisted_or_replayed":         "authoritative_after_current_version_validation",
		"local_gateway_server":          "authoritative_at_server_boundary",
		"managed_startup_local_gateway": "authoritative_with_construction_provenance",
		"generic_external_mcp":          "untrusted_provider_evidence",
		"federated_swarm_peer":          "unsupported",
	} {
		assertScalarValue(t, mustMappingValue(t, mustMappingValue(t, authoritySources, source), "status"), status)
	}

	classes := mustMappingValue(t, taxonomy, "classes")
	gotClassNames := failureMappingKeys(classes)
	wantClassNames := make([]string, 0, len(runtimefailures.Classes()))
	for _, class := range runtimefailures.Classes() {
		wantClassNames = append(wantClassNames, string(class))
	}
	if !reflect.DeepEqual(gotClassNames, wantClassNames) {
		t.Fatalf("spec classes = %#v, want %#v", gotClassNames, wantClassNames)
	}

	for _, def := range runtimefailures.Registry() {
		node := mustMappingValue(t, classes, string(def.Class))
		assertScalarValue(t, mustMappingValue(t, node, "task_failure"), strconv.FormatBool(def.TaskFailure))
		assertScalarValue(t, mustMappingValue(t, node, "retryable"), strconv.FormatBool(def.Retryable))
		assertScalarValue(t, mustMappingValue(t, node, "deterministic"), strconv.FormatBool(def.Deterministic))
		assertScalarValue(t, mustMappingValue(t, node, "message_template"), def.MessageTemplate)
		assertScalarValue(t, mustMappingValue(t, node, "remediation_template"), def.RemediationTemplate)
	}

	selectors := mustMappingValue(t, taxonomy, "selectors")
	for _, selector := range []string{runtimefailures.SelectorAny, runtimefailures.SelectorAnyTaskFailure} {
		members, ok := runtimefailures.SelectorMembers(selector)
		if !ok {
			t.Fatalf("executable selector %s missing", selector)
		}
		want := make([]string, 0, len(members))
		for _, class := range members {
			want = append(want, string(class))
		}
		got := failureSequenceScalars(mustMappingValue(t, mustMappingValue(t, selectors, selector), "members"))
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("selector %s members = %#v, want %#v", selector, got, want)
		}
	}
}

func failureMappingKeys(node *yaml.Node) []string {
	if node == nil || node.Kind != yaml.MappingNode {
		return nil
	}
	out := make([]string, 0, len(node.Content)/2)
	for i := 0; i+1 < len(node.Content); i += 2 {
		out = append(out, scalarValue(node.Content[i]))
	}
	return out
}

func failureSequenceScalars(node *yaml.Node) []string {
	if node == nil || node.Kind != yaml.SequenceNode {
		return nil
	}
	out := make([]string, 0, len(node.Content))
	for _, item := range node.Content {
		out = append(out, scalarValue(item))
	}
	return out
}
