package apispec

import (
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestPlatformSpecPromotesVersionedRoutingTopologyArtifact(t *testing.T) {
	root := loadPlatformSpecYAMLNode(t)
	routing := mustYAMLPath(t, root, "flow_model", "flow_instance_authoring", "analyzer_obligations", "expand_minimize_tooling", "routing_topology")

	assertScalarValue(t, mustMappingValue(t, routing, "command"), "swarm describe routes")
	assertScalarValue(t, mustMappingValue(t, routing, "schema_version"), "routing-topology/v1")
	assertScalarValue(t, mustMappingValue(t, routing, "projection_only"), "true")
	assertScalarContains(t, mustMappingValue(t, routing, "canonical_code_owner"), "internal/runtime/routingtopology.Build")
	assertScalarContains(t, mustMappingValue(t, routing, "evolution_rule"), "new artifact schema identity")
	assertScalarContains(t, mustMappingValue(t, routing, "evolution_rule"), "Runtime recipients")
	assertScalarContains(t, mustMappingValue(t, routing, "endpoint_rule"), "immutable authored endpoint census")
	assertScalarContains(t, mustMappingValue(t, routing, "endpoint_rule"), "interface exposures")
	resolutionRule := mustMappingValue(t, routing, "resolution_rule")
	assertScalarContains(t, resolutionRule, "connect edges are edge-only")
	assertScalarContains(t, resolutionRule, "receiver-owned input")
	assertScalarContains(t, resolutionRule, "address, create, select, select-or-create, fan-in, and reply")
	assertScalarContains(t, resolutionRule, "target_set")
	assertScalarContains(t, resolutionRule, "typed-reply facts")
	assertScalarContains(t, resolutionRule, "fan-out remains non-runnable under #1934")
	for _, retired := range []string{"broadcast declarations", "delivery declarations"} {
		if strings.Contains(resolutionRule.Value, retired) {
			t.Fatalf("resolution_rule retains retired connect authority %q: %s", retired, resolutionRule.Value)
		}
	}
	assertScalarContains(t, mustMappingValue(t, routing, "identity_rule"), "fail closed")
	assertScalarContains(t, mustMappingValue(t, routing, "delivery_scope_rule"), "Delivery between authored producers and consumers has exactly two canonical scopes")
	assertScalarContains(t, mustMappingValue(t, routing, "delivery_scope_rule"), "Standing ingress is admission")
	assertScalarContains(t, mustMappingValue(t, routing, "typed_pubsub_rule"), "evaluates each producer-or-input/consumer pair exactly once")
	assertScalarContains(t, mustMappingValue(t, routing, "typed_pubsub_rule"), "Bounded observe-grant patterns")
	assertScalarContains(t, mustMappingValue(t, routing, "typed_pubsub_rule"), "source-relative local identity paired with its source-qualified canonical identity")
	assertScalarContains(t, mustMappingValue(t, routing, "typed_pubsub_rule"), "must not be flattened into independent candidates")
	assertScalarContains(t, mustMappingValue(t, routing, "typed_pubsub_rule"), "runtime route matching uses that canonical identity")
	assertScalarContains(t, mustMappingValue(t, routing, "typed_pubsub_rule"), "{instance_path}/{local_event}")
	assertScalarContains(t, mustMappingValue(t, routing, "typed_pubsub_rule"), "only from lifecycle proof")
	assertScalarContains(t, mustMappingValue(t, routing, "typed_pubsub_rule"), "Concrete path")
	assertScalarContains(t, mustMappingValue(t, routing, "typed_pubsub_rule"), "is not instance-ownership proof")
	assertScalarContains(t, mustMappingValue(t, routing, "typed_pubsub_rule"), "authored static")
	assertScalarContains(t, mustMappingValue(t, routing, "typed_pubsub_rule"), "Before any")
	assertScalarContains(t, mustMappingValue(t, routing, "typed_pubsub_rule"), "canonical namespace collision is a hard")
	assertScalarContains(t, mustMappingValue(t, routing, "typed_pubsub_rule"), "sibling instance routes unchanged")
	assertScalarContains(t, mustMappingValue(t, routing, "typed_pubsub_rule"), "declared nested template")
	assertScalarContains(t, mustMappingValue(t, routing, "typed_pubsub_rule"), "parent ancestry alone is not collision proof")
	assertScalarContains(t, mustMappingValue(t, routing, "typed_pubsub_rule"), "complete normalized `(scope_key, instance_id,")
	assertScalarContains(t, mustMappingValue(t, routing, "typed_pubsub_rule"), "Exact replay of the complete identity is idempotent")
	assertScalarContains(t, mustMappingValue(t, routing, "typed_pubsub_rule"), "removal must match the complete stored owner")
	assertScalarContains(t, mustMappingValue(t, routing, "typed_pubsub_rule"), "Normal template creation, recovery, and ad hoc fallback")
	assertScalarContains(t, mustMappingValue(t, routing, "typed_pubsub_rule"), "static witness remains the sole")
	assertScalarContains(t, mustMappingValue(t, routing, "typed_pubsub_rule"), "Canonical name equality alone never authorizes a cross-flow edge")
	assertScalarContains(t, mustMappingValue(t, routing, "typed_pubsub_rule"), "runtime route admission")
	assertScalarContains(t, mustMappingValue(t, routing, "typed_pubsub_rule"), "including when a caller supplies a prebuilt route table")
	assertScalarContains(t, mustMappingValue(t, routing, "typed_pubsub_rule"), "every declared materializable event independently of current producer-census membership")
	assertScalarContains(t, mustMappingValue(t, routing, "typed_pubsub_rule"), "low-level event.publish")
	assertScalarContains(t, mustMappingValue(t, routing, "connect_source_rule"), "exact authored package.yaml file:line")
	assertScalarContains(t, mustMappingValue(t, routing, "connect_source_rule"), "connect_source_location_missing")
	qualifiedRetirement := mustMappingValue(t, routing, "qualified_cross_flow_subscription_retirement")
	assertScalarContains(t, qualifiedRetirement, "hard invalidity")
	assertScalarContains(t, qualifiedRetirement, "never creates a route")
	assertScalarContains(t, qualifiedRetirement, "parent connect")
	deliveryScopes := mustMappingValue(t, routing, "delivery_scopes")
	if deliveryScopes.Kind != yaml.SequenceNode || len(deliveryScopes.Content) != 2 {
		t.Fatalf("delivery_scopes = %#v, want exactly two scopes for delivery between authored producers and consumers", deliveryScopes)
	}
	assertScalarContains(t, deliveryScopes.Content[0], "typed_pubsub delivery between authored producers and consumers")
	assertScalarContains(t, deliveryScopes.Content[1], "inter_flow_connect delivery between authored producers and consumers")
	rejectedScope := "intra" + "_" + "flow"
	if strings.Contains(deliveryScopes.Content[0].Value+deliveryScopes.Content[1].Value, rejectedScope) {
		t.Fatalf("delivery_scopes retained rejected scope %q", rejectedScope)
	}

	rows := mustYAMLPath(t, root, "cli_specification", "foundations", "output_contract", "command_support", "output_conformance_registry", "rows")
	row := mustMappingValue(t, rows, "describe_routes")
	assertScalarValue(t, mustMappingValue(t, row, "command"), "swarm describe routes")
	assertScalarValue(t, mustMappingValue(t, row, "classification"), "shared_output")
	assertScalarContains(t, mustMappingValue(t, row, "json_shape"), "routing-topology/v1")
}
