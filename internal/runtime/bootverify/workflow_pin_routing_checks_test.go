package bootverify

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/notifyallchildren"
)

func TestPinTargetResolution_FailsClosedForPinOutputWithoutTargetMechanism(t *testing.T) {
	bundle := loadPinRoutingVerifyBundle(t, "emit:\n        event: result.ready\n")
	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})
	if !reportContains(report.Errors(), "pin_target_resolution", "target_required_missing") {
		t.Fatalf("expected pin_target_resolution target_required_missing, got %#v", report.Errors())
	}
}

func TestPinTargetResolution_AllowsExplicitBroadcastOptOut(t *testing.T) {

	bundle := loadPinRoutingVerifyBundle(t, "emit:\n        event: result.ready\n        broadcast: true\n")
	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})
	if reportContains(report.Errors(), "pin_target_resolution", "") {
		t.Fatalf("unexpected pin_target_resolution error: %#v", report.Errors())
	}
}

func TestPinTargetResolution_FailsClosedForProducerTargetCommonCompositionPath(t *testing.T) {
	bundle := loadPinRoutingProducerRouteBundle(t, `
      emit:
        event: shared.ready
        fields:
          entity_id: payload.entity_id
        target:
          flow: consumer
          match:
            entity_id: payload.entity_id
`, true)

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})
	if !reportContains(report.Errors(), "pin_target_resolution", "producer_target_common_path_forbidden") {
		t.Fatalf("expected producer_target_common_path_forbidden, got %#v", report.Errors())
	}
}

func TestPinTargetResolution_FailsClosedForProducerBroadcastCommonCompositionPath(t *testing.T) {

	bundle := loadPinRoutingProducerRouteBundle(t, `
      emit:
        event: shared.ready
        fields:
          entity_id: payload.entity_id
        broadcast: true
`, true)

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})
	if !reportContains(report.Errors(), "pin_target_resolution", "producer_broadcast_common_path_forbidden") {
		t.Fatalf("expected producer_broadcast_common_path_forbidden, got %#v", report.Errors())
	}
}

func TestPinTargetResolution_AllowsTargetlessFanOutEmitThroughParentConnect(t *testing.T) {
	source := notifyallchildren.LoadSource(t, notifyallchildren.Options{})
	report := Run(context.Background(), source, Options{})
	if reportContains(report.Errors(), "pin_target_resolution", "") ||
		reportContains(report.Errors(), "composition_connect_validation", "") ||
		reportContains(report.Errors(), "output_pin_key_carries_validation", "") {
		t.Fatalf("targetless fan_out.emit should verify through parent connect authority, got %#v", report.Errors())
	}
}

func TestPinTargetResolution_FanOutEmitFailsClosedWithoutRouteAuthority(t *testing.T) {
	source := notifyallchildren.LoadSource(t, notifyallchildren.Options{OmitConnect: true})
	report := Run(context.Background(), source, Options{})
	if !reportContains(report.Errors(), "pin_target_resolution", "handler.fan_out.emit") ||
		!reportContains(report.Errors(), "pin_target_resolution", "target_required_missing") {
		t.Fatalf("expected targetless fan_out.emit target_required_missing, got %#v", report.Errors())
	}
}

func TestPinTargetResolution_FanOutEmitFailsClosedWithoutOutputPin(t *testing.T) {
	source := notifyallchildren.LoadSource(t, notifyallchildren.Options{OmitOutputPin: true})
	report := Run(context.Background(), source, Options{})
	if !reportContains(report.Errors(), "composition_connect_validation", "producer_output_pin_missing") {
		t.Fatalf("expected missing output pin to fail connect validation, got %#v", report.Errors())
	}
}

func TestPinTargetResolution_FanOutEmitFailsClosedForMissingConnectKeyMaterial(t *testing.T) {
	source := notifyallchildren.LoadSource(t, notifyallchildren.Options{BadConnectMapping: true})
	report := Run(context.Background(), source, Options{})
	if !reportContains(report.Errors(), "composition_connect_validation", "instance_resolution_invalid") {
		t.Fatalf("expected incompatible instance adapter to fail closed, got %#v", report.Errors())
	}
}

func TestPinTargetResolution_FanOutEmitFailsClosedForMissingCarriesPayload(t *testing.T) {
	source := notifyallchildren.LoadSource(t, notifyallchildren.Options{MissingEmitCarry: true})
	report := Run(context.Background(), source, Options{})
	if !reportContains(report.Errors(), "output_pin_key_carries_validation", "emit_payload_missing_key") ||
		!reportContains(report.Errors(), "output_pin_key_carries_validation", "handler.fan_out.emit") {
		t.Fatalf("expected fan_out.emit missing carried fields to fail closed, got %#v", report.Errors())
	}
}

func TestPinTargetResolution_FanOutEmitRejectsProducerTargetAndBroadcastCommonPath(t *testing.T) {
	tests := []struct {
		name string
		opts notifyallchildren.Options
		want string
	}{
		{
			name: "producer target",
			opts: notifyallchildren.Options{ProducerTarget: true},
			want: "producer_target_common_path_forbidden",
		},
		{
			name: "producer broadcast",
			opts: notifyallchildren.Options{ProducerBroadcast: true},
			want: "producer_broadcast_common_path_forbidden",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			source := notifyallchildren.LoadSource(t, tc.opts)
			report := Run(context.Background(), source, Options{})
			if !reportContains(report.Errors(), "pin_target_resolution", tc.want) {
				t.Fatalf("expected %s, got %#v", tc.want, report.Errors())
			}
		})
	}
}

func TestRedundantInTopologySelectEntityFailsClosedForParentConnect(t *testing.T) {
	bundle := loadSelectEntityDemotionBundle(t, canonicalrouting.SelectEntityDemotionOptions{
		Acquisition:  canonicalrouting.SelectEntityAcquire,
		WithProducer: true,
	})

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if !reportContains(report.Errors(), "redundant_in_topology_select_entity", "instance.by plus parent connect") {
		t.Fatalf("expected redundant_in_topology_select_entity hard invalidity, got errors=%#v warnings=%#v", report.Errors(), report.Warnings())
	}
	if reportContains(report.Warnings(), "redundant_in_topology_select_entity", "") {
		t.Fatalf("redundant_in_topology_select_entity must not remain warning-only, got %#v", report.Warnings())
	}
}

func TestRedundantInTopologySelectEntityFailsClosedForRenamedConnectEvents(t *testing.T) {
	bundle := loadSelectEntityDemotionBundle(t, canonicalrouting.SelectEntityDemotionOptions{
		Acquisition:       canonicalrouting.SelectEntityAcquire,
		WithProducer:      true,
		RenameReceiverPin: true,
	})

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if !reportContains(report.Errors(), "redundant_in_topology_select_entity", "instance.by plus parent connect") {
		t.Fatalf("renamed connect must retain producer topology proof, got errors=%#v warnings=%#v", report.Errors(), report.Warnings())
	}
}

func TestRedundantInTopologySelectEntityFailsClosedForStagedParentConnect(t *testing.T) {
	bundle := loadSelectEntityDemotionBundle(t, canonicalrouting.SelectEntityDemotionOptions{
		Acquisition:  canonicalrouting.SelectEntityAcquire,
		WithProducer: true,
	})
	useStagedLifecycleForFlow(t, bundle, "consumer", "pending", []string{"pending", "done"}, []string{"done"})

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if !reportContains(report.Errors(), "redundant_in_topology_select_entity", "instance.by plus parent connect") {
		t.Fatalf("expected redundant_in_topology_select_entity hard invalidity for staged flow, got errors=%#v warnings=%#v", report.Errors(), report.Warnings())
	}
	if reportContains(report.Warnings(), "redundant_in_topology_select_entity", "") {
		t.Fatalf("redundant_in_topology_select_entity must not remain warning-only for staged flow, got %#v", report.Warnings())
	}
}

func TestRedundantInTopologySelectOrCreateEntityFailsClosedForParentConnect(t *testing.T) {
	bundle := loadSelectEntityDemotionBundle(t, canonicalrouting.SelectEntityDemotionOptions{
		Acquisition:  canonicalrouting.SelectOrCreateEntityAcquire,
		WithProducer: true,
	})

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if !reportContains(report.Errors(), "redundant_in_topology_select_entity", "select_or_create_entity") ||
		!reportContains(report.Errors(), "redundant_in_topology_select_entity", "instance.by plus parent connect") {
		t.Fatalf("expected redundant_in_topology_select_entity hard invalidity for select_or_create_entity, got errors=%#v warnings=%#v", report.Errors(), report.Warnings())
	}
}

func TestRedundantInTopologySelectEntityAllowsTemplateInstanceConnectReplacement(t *testing.T) {
	bundle := loadSelectEntityDemotionBundle(t, canonicalrouting.SelectEntityDemotionOptions{
		TemplateReceiver: true,
		WithProducer:     true,
	})

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if report.HasErrors() {
		t.Fatalf("template instance.by plus parent connect should verify without receiver acquisition, got %#v", report.Errors())
	}
	if reportContains(report.Warnings(), "redundant_in_topology_select_entity", "") {
		t.Fatalf("connect-routed replacement should not report select_entity warning, got %#v", report.Warnings())
	}
}

func TestRedundantInTopologySelectEntityRejectsExternalAndMixedStaticAcquisitionByRetirement(t *testing.T) {
	tests := []struct {
		name         string
		acquisition  canonicalrouting.SelectEntityAcquisition
		withProducer bool
	}{
		{name: "external select_entity", acquisition: canonicalrouting.SelectEntityAcquire},
		{name: "external select_or_create_entity", acquisition: canonicalrouting.SelectOrCreateEntityAcquire},
		{name: "mixed select_entity", acquisition: canonicalrouting.SelectEntityAcquire, withProducer: true},
		{name: "mixed select_or_create_entity", acquisition: canonicalrouting.SelectOrCreateEntityAcquire, withProducer: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			bundle := loadSelectEntityDemotionBundle(t, canonicalrouting.SelectEntityDemotionOptions{
				Acquisition:  tc.acquisition,
				External:     true,
				WithProducer: tc.withProducer,
			})

			report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

			if reportContains(report.Errors(), "redundant_in_topology_select_entity", "") ||
				reportContains(report.Warnings(), "redundant_in_topology_select_entity", "") {
				t.Fatalf("external/mixed provenance must not be mislabeled as in-topology, got errors=%#v warnings=%#v", report.Errors(), report.Warnings())
			}
			if !reportContains(report.Errors(), "select_entity_validation", "static multi-row entity ownership is retired") {
				t.Fatalf("external/mixed static acquisition should fail closed by #1554 retirement, got %#v", report.Errors())
			}
		})
	}
}

func TestRedundantInTopologySelectEntityIgnoresProducerConnectedOnlyToOtherReceiver(t *testing.T) {
	bundle := loadSelectEntityDemotionBundle(t, canonicalrouting.SelectEntityDemotionOptions{
		Acquisition:            canonicalrouting.SelectEntityAcquire,
		WithProducer:           true,
		ConnectProducerToOther: true,
	})

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if reportContains(report.Errors(), "redundant_in_topology_select_entity", "") ||
		reportContains(report.Warnings(), "redundant_in_topology_select_entity", "") {
		t.Fatalf("producer connected only to another receiver must not prove in-topology authority for this receiver, got errors=%#v warnings=%#v", report.Errors(), report.Warnings())
	}
	if !reportContains(report.Errors(), "select_entity_validation", "static multi-row entity ownership is retired") {
		t.Fatalf("static receiver selection should still fail closed by #1554 retirement, got %#v", report.Errors())
	}
}

func TestPinTargetResolution_FailsClosedForProducerTargetAdaptedConnectCommonPath(t *testing.T) {
	bundle := loadPinRoutingProducerRouteBundleForEvents(t, "shared.ready", "consumer.ready", `
      emit:
        event: shared.ready
        fields:
          entity_id: payload.entity_id
        target:
          flow: consumer
          match:
            entity_id: payload.entity_id
`, pinRoutingProducerRouteAdaptedConnect())

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})
	if !reportContains(report.Errors(), "pin_target_resolution", "producer_target_common_path_forbidden") {
		t.Fatalf("expected producer_target_common_path_forbidden for adapted connect, got %#v", report.Errors())
	}
}

func TestPinTargetResolution_FailsClosedForProducerBroadcastAdaptedConnectCommonPath(t *testing.T) {

	bundle := loadPinRoutingProducerRouteBundleForEvents(t, "shared.ready", "consumer.ready", `
      emit:
        event: shared.ready
        fields:
          entity_id: payload.entity_id
        broadcast: true
`, pinRoutingProducerRouteAdaptedConnect())

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})
	if !reportContains(report.Errors(), "pin_target_resolution", "producer_broadcast_common_path_forbidden") {
		t.Fatalf("expected producer_broadcast_common_path_forbidden for adapted connect, got %#v", report.Errors())
	}
}

func TestPinTargetResolution_FailsClosedForProducerTargetCommonPathEvenWithParentConnect(t *testing.T) {
	bundle := loadPinRoutingProducerRouteBundle(t, `
      emit:
        event: shared.ready
        fields:
          entity_id: payload.entity_id
        target:
          flow: consumer
          match:
            entity_id: payload.entity_id
`, true, pinRoutingProducerRouteConnect())

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})
	if !reportContains(report.Errors(), "pin_target_resolution", "producer_target_common_path_forbidden") {
		t.Fatalf("expected producer_target_common_path_forbidden with parent connect, got %#v", report.Errors())
	}
}

func TestPinTargetResolution_FailsClosedForUnknownProducerTargetFlowEvenWithParentConnect(t *testing.T) {
	bundle := loadPinRoutingProducerRouteBundle(t, `
      emit:
        event: shared.ready
        fields:
          entity_id: payload.entity_id
        target:
          flow: missing-consumer
          match:
            entity_id: payload.entity_id
`, true, pinRoutingProducerRouteConnect())

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})
	if !reportContains(report.Errors(), "pin_target_resolution", "target_unknown_flow") {
		t.Fatalf("expected target_unknown_flow with parent connect, got %#v", report.Errors())
	}
}

func TestPinTargetResolution_AllowsFlowScopedAgentEmitEventsThroughParentConnect(t *testing.T) {
	bundle := loadPinRoutingProducerAgentRouteBundleForEvents(t, "shared.ready", "shared.ready", pinRoutingProducerRouteConnect())

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})
	if reportContains(report.Errors(), "pin_target_resolution", "producer-agent") {
		t.Fatalf("parent connect should satisfy agent emit_events output pin target proof, got %#v", report.Errors())
	}
}

func TestPinTargetResolution_DoesNotTreatMergedFlowAgentAsRootEmitSite(t *testing.T) {
	bundle := loadPinRoutingProducerAgentRouteBundleWithRootOutputs(t, "shared.ready", "shared.ready", []string{"shared.ready"}, pinRoutingProducerRouteConnect())

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})
	if reportContains(report.Errors(), "pin_target_resolution", "root agent emit_events on agent producer-agent") {
		t.Fatalf("flow-scoped agent was evaluated as root: %#v", report.Errors())
	}
	if reportContains(report.Errors(), "pin_target_resolution", "producer-agent") {
		t.Fatalf("parent connect should satisfy the real flow-scoped agent emit_events site, got %#v", report.Errors())
	}
}

func TestPinTargetResolution_FailsClosedForFlowScopedAgentEmitEventsWithoutRouteAuthority(t *testing.T) {
	bundle := loadPinRoutingProducerAgentRouteBundleForEvents(t, "shared.ready", "shared.ready")

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})
	if !reportContains(report.Errors(), "pin_target_resolution", "flow producer agent emit_events on agent producer-agent") ||
		!reportContains(report.Errors(), "pin_target_resolution", "target_required_missing") {
		t.Fatalf("expected agent emit_events target_required_missing, got %#v", report.Errors())
	}
}

func TestPinTargetResolution_ChecksProjectScopeAgentUnderOwningFlow(t *testing.T) {
	bundle := loadPinRoutingVerifySourceFixture(t, pinRoutingVerifySourceFixture{
		extrasAgents: `
extras-agent:
  id: extras-agent
  role: producer
  mode: task
  emit_events:
    - support.ready
`,
	})
	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})
	if !reportContains(report.Errors(), "pin_target_resolution", "flow support agent emit_events on agent extras-agent") ||
		!reportContains(report.Errors(), "pin_target_resolution", "target_required_missing") {
		t.Fatalf("expected project-scope agent under support flow to fail closed, got %#v", report.Errors())
	}
}

func TestPinTargetResolution_AllowsExplicitTargetEscapeHatches(t *testing.T) {
	for _, tt := range []struct {
		name      string
		emitBlock string
	}{
		{
			name:      "sender",
			emitBlock: "emit:\n        event: result.ready\n        target: sender\n",
		},
		{
			name:      "instance_id",
			emitBlock: "emit:\n        event: result.ready\n        target:\n          instance_id: worker-001\n",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			bundle := loadPinRoutingVerifyBundle(t, tt.emitBlock)
			report := Run(context.Background(), semanticview.Wrap(bundle), Options{})
			if reportContains(report.Errors(), "pin_target_resolution", "") {
				t.Fatalf("unexpected pin_target_resolution error: %#v", report.Errors())
			}
		})
	}
}

func TestPinTargetResolution_AllowsSingularDynamicFlowMatchWhenNotPackageComposition(t *testing.T) {
	bundle := loadPinRoutingProducerRouteBundle(t, `
      emit:
        event: shared.ready
        fields:
          entity_id: payload.entity_id
        target:
          flow: consumer
          match:
            entity_id: payload.entity_id
`, false)

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})
	if reportContains(report.Errors(), "pin_target_resolution", "producer_target_common_path_forbidden") {
		t.Fatalf("unexpected producer_target_common_path_forbidden for non-receiver dynamic target: %#v", report.Errors())
	}
}

func TestPinTargetResolution_DoesNotLeafMatchDistinctQualifiedEvents(t *testing.T) {

	for _, tt := range []struct {
		name string
		body string
	}{
		{
			name: "broadcast",
			body: `
      emit:
        event: billing/order.completed
        fields:
          entity_id: payload.entity_id
        broadcast: true
`,
		},
		{
			name: "target_flow_match",
			body: `
      emit:
        event: billing/order.completed
        fields:
          entity_id: payload.entity_id
        target:
          flow: consumer
          match:
            entity_id: payload.entity_id
`,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			bundle := loadPinRoutingProducerRouteBundleForEvents(t, "billing/order.completed", "shipping/order.completed", tt.body)
			report := Run(context.Background(), semanticview.Wrap(bundle), Options{})
			if reportContains(report.Errors(), "pin_target_resolution", "producer_") {
				t.Fatalf("unexpected producer common-path failure for distinct qualified events: %#v", report.Errors())
			}
		})
	}
}

func TestPinTargetResolution_FailsClosedForRootPinOutputWithoutTargetMechanism(t *testing.T) {
	bundle := loadPinRoutingVerifySourceFixture(t, pinRoutingVerifySourceFixture{
		rootNodes: pinRoutingVerifyNodeYAML("root-node", "root.start", "root.ready", false),
	})
	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})
	if !reportContains(report.Errors(), "pin_target_resolution", "root handler.emit on node root-node") {
		t.Fatalf("expected root pin_target_resolution error, got %#v", report.Errors())
	}
}

func TestPinTargetResolution_FailsClosedForNestedRootPinOutputWithoutTargetMechanism(t *testing.T) {
	bundle := loadPinRoutingVerifySourceFixture(t, pinRoutingVerifySourceFixture{
		rootNodes: pinRoutingVerifyRuleNodeYAML("root-node", "root.start", "root.ready", false),
	})
	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})
	if !reportContains(report.Errors(), "pin_target_resolution", "root handler.rules.emit on node root-node") {
		t.Fatalf("expected nested root pin_target_resolution error, got %#v", report.Errors())
	}
}

func TestPinTargetResolution_FailsClosedForRootGuardEscalationPinOutput(t *testing.T) {
	bundle := loadPinRoutingVerifySourceFixture(t, pinRoutingVerifySourceFixture{
		rootNodes: pinRoutingVerifyGuardNodeYAML("root-node", "root.start", "root.ready"),
	})
	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})
	if !reportContains(report.Errors(), "pin_target_resolution", "root handler.guard.on_fail.escalate on node root-node") {
		t.Fatalf("expected root guard escalation pin_target_resolution error, got %#v", report.Errors())
	}
}

func TestPinTargetResolution_AllowsRootExplicitBroadcastOptOut(t *testing.T) {
	bundle := loadPinRoutingVerifySourceFixture(t, pinRoutingVerifySourceFixture{
		rootNodes: pinRoutingVerifyNodeYAML("root-node", "root.start", "root.ready", true),
	})
	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})
	if reportContains(report.Errors(), "pin_target_resolution", "root") {
		t.Fatalf("unexpected root pin_target_resolution error: %#v", report.Errors())
	}
}

func TestPinTargetResolution_AllowsRootPinOutputThroughRootConnect(t *testing.T) {
	bundle := loadPinRoutingRootConnectBundle(t, `
      emit:
        event: root.ready
        fields:
          entity_id: payload.entity_id
`)
	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})
	if reportContains(report.Errors(), "pin_target_resolution", "root") {
		t.Fatalf("root connect should satisfy root pin target proof, got %#v", report.Errors())
	}
}

func TestPinTargetResolution_FailsClosedForRootProducerBroadcastCommonPath(t *testing.T) {

	bundle := loadPinRoutingRootConnectBundle(t, `
      emit:
        event: root.ready
        fields:
          entity_id: payload.entity_id
        broadcast: true
`)
	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})
	if !reportContains(report.Errors(), "pin_target_resolution", "producer_broadcast_common_path_forbidden") {
		t.Fatalf("expected root producer_broadcast_common_path_forbidden, got %#v", report.Errors())
	}
}

func TestPinTargetResolution_FailsClosedForRootProducerTargetCommonPath(t *testing.T) {
	bundle := loadPinRoutingRootConnectBundle(t, `
      emit:
        event: root.ready
        fields:
          entity_id: payload.entity_id
        target:
          flow: consumer
          match:
            entity_id: payload.entity_id
`)
	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})
	if !reportContains(report.Errors(), "pin_target_resolution", "producer_target_common_path_forbidden") {
		t.Fatalf("expected root producer_target_common_path_forbidden, got %#v", report.Errors())
	}
}

func TestPinTargetResolution_ChecksRootNodeWhenFlowNodeIDCollides(t *testing.T) {
	bundle := loadPinRoutingVerifySourceFixture(t, pinRoutingVerifySourceFixture{
		rootNodes:        pinRoutingVerifyNodeYAML("shared-node", "root.start", "root.ready", false),
		supportFlowNodes: pinRoutingVerifyNodeYAML("shared-node", "support.start", "support.ready", true),
	})
	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})
	if countPinTargetFindings(report, "root handler.emit on node shared-node") != 1 {
		t.Fatalf("expected exactly one root colliding-node finding, got %#v", pinTargetFindingMessages(report))
	}
}

func TestPinTargetResolution_EvaluatesPackageBackedFlowOnceNotRoot(t *testing.T) {
	bundle := loadPinRoutingVerifySourceFixture(t, pinRoutingVerifySourceFixture{
		supportFlowNodes: pinRoutingVerifyNodeYAML("support-node", "support.start", "support.ready", false),
	})
	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})
	if countPinTargetFindings(report, "flow support handler.emit on node support-node") != 1 {
		t.Fatalf("expected exactly one flow support finding, got %#v", pinTargetFindingMessages(report))
	}
	if countPinTargetFindings(report, "root handler.emit on node support-node") != 0 {
		t.Fatalf("package-backed flow node was reported as root: %#v", pinTargetFindingMessages(report))
	}
}

func TestPinTargetResolution_EvaluatesSoleParentPackageUnderOwningFlow(t *testing.T) {
	bundle := loadPinRoutingVerifySourceFixture(t, pinRoutingVerifySourceFixture{
		extrasNodes: pinRoutingVerifyNodeYAML("extras-node", "support.start", "support.ready", false),
	})
	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})
	if countPinTargetFindings(report, "flow support handler.emit on node extras-node") != 1 {
		t.Fatalf("expected exactly one sole-parent package flow finding, got %#v", pinTargetFindingMessages(report))
	}
}

func TestPinTargetResolution_DistinctSameFlowNodeIDSourceSitesBothConsidered(t *testing.T) {
	bundle := loadPinRoutingVerifySourceFixture(t, pinRoutingVerifySourceFixture{
		supportFlowNodes: pinRoutingVerifyNodeYAML("support-node", "support.start", "support.ready", false),
		extrasNodes:      pinRoutingVerifyNodeYAML("support-node", "support.start", "support.ready", true),
	})
	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})
	if countPinTargetFindings(report, "flow support handler.emit on node support-node") != 1 {
		t.Fatalf("expected invalid flow-scope support-node site not to be suppressed, got %#v", pinTargetFindingMessages(report))
	}
}

func loadPinRoutingVerifyBundle(t *testing.T, emitBlock string) *runtimecontracts.WorkflowContractBundle {
	t.Helper()
	root := t.TempDir()
	writePinRoutingVerifyFile(t, filepath.Join(root, "package.yaml"), `
name: pin-routing-verify
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
flows:
  - id: worker
    flow: worker
    mode: static
`)
	writePinRoutingVerifyFile(t, filepath.Join(root, "schema.yaml"), "name: pin-routing-verify\n")
	writePinRoutingVerifyFile(t, filepath.Join(root, "policy.yaml"), "{}\n")
	writePinRoutingVerifyFile(t, filepath.Join(root, "tools.yaml"), "{}\n")
	writePinRoutingVerifyFile(t, filepath.Join(root, "agents.yaml"), "{}\n")
	writePinRoutingVerifyFile(t, filepath.Join(root, "events.yaml"), "{}\n")
	writePinRoutingVerifyFile(t, filepath.Join(root, "entities.yaml"), "{}\n")
	writePinRoutingVerifyFile(t, filepath.Join(root, "flows", "worker", "schema.yaml"), `
name: worker
initial_state: pending
states: [pending, done]
terminal_states: [done]
pins:
  inputs:
    events:
      - task.started
  outputs:
    events:
      - result.ready
`)
	writePinRoutingVerifyFile(t, filepath.Join(root, "flows", "worker", "events.yaml"), `
task.started:
  entity_id: text
result.ready:
  entity_id: text
`)
	writePinRoutingVerifyFile(t, filepath.Join(root, "flows", "worker", "entities.yaml"), `
task:
  entity_id: text
`)
	writePinRoutingVerifyFile(t, filepath.Join(root, "flows", "worker", "nodes.yaml"), `
worker-node:
  id: worker-node
  execution_type: system_node
  event_handlers:
    task.started:
      create_entity: true
      advances_to: done
      `+emitBlock+`  state_schema:
    fields:
      seen: integer
`)
	repoRoot := filepath.Clean(filepath.Join("..", "..", ".."))
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	return bundle
}

func loadPinRoutingProducerRouteBundle(t *testing.T, producerHandlerBody string, consumerConsumesSharedReady bool, connectBlocks ...string) *runtimecontracts.WorkflowContractBundle {
	consumerInputEvent := "consumer.start"
	if consumerConsumesSharedReady {
		consumerInputEvent = "shared.ready"
	}
	return loadPinRoutingProducerRouteBundleForEvents(t, "shared.ready", consumerInputEvent, producerHandlerBody, connectBlocks...)
}

func loadPinRoutingProducerRouteBundleForEvents(t *testing.T, producerOutputEvent, consumerInputEvent, producerHandlerBody string, connectBlocks ...string) *runtimecontracts.WorkflowContractBundle {
	t.Helper()
	root := t.TempDir()
	writePinRoutingVerifyFile(t, filepath.Join(root, "package.yaml"), `
name: pin-routing-producer-route
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
flows:
  - id: producer
    flow: producer
    mode: static
  - id: consumer
    flow: consumer
    mode: static
`+strings.Join(connectBlocks, ""))
	writePinRoutingVerifyFile(t, filepath.Join(root, "schema.yaml"), "name: pin-routing-producer-route\n")
	writePinRoutingVerifyFile(t, filepath.Join(root, "policy.yaml"), "{}\n")
	writePinRoutingVerifyFile(t, filepath.Join(root, "tools.yaml"), "{}\n")
	writePinRoutingVerifyFile(t, filepath.Join(root, "agents.yaml"), "{}\n")
	writePinRoutingVerifyFile(t, filepath.Join(root, "events.yaml"), "{}\n")
	writePinRoutingVerifyFile(t, filepath.Join(root, "entities.yaml"), "{}\n")
	writePinRoutingVerifyFile(t, filepath.Join(root, "flows", "producer", "schema.yaml"), `
name: producer
initial_state: pending
states: [pending, done]
terminal_states: [done]
pins:
  inputs:
    events: [producer.start]
  outputs:
    events: [`+producerOutputEvent+`]
`)
	writePinRoutingVerifyFile(t, filepath.Join(root, "flows", "producer", "events.yaml"), `
producer.start:
  entity_id: text
`+producerOutputEvent+`:
  entity_id: text
`)
	writePinRoutingVerifyFile(t, filepath.Join(root, "flows", "producer", "entities.yaml"), `
producer:
  entity_id: text
`)
	writePinRoutingVerifyFile(t, filepath.Join(root, "flows", "producer", "nodes.yaml"), `
producer-node:
  id: producer-node
  execution_type: system_node
  event_handlers:
    producer.start:
`+producerHandlerBody+`
`)
	writePinRoutingVerifyFile(t, filepath.Join(root, "flows", "consumer", "schema.yaml"), `
name: consumer
initial_state: pending
states: [pending, done]
terminal_states: [done]
pins:
  inputs:
    events: [`+consumerInputEvent+`]
  outputs:
    events: [consumer.done]
`)
	consumerEvents := `
consumer.start:
  entity_id: text
`
	if consumerInputEvent != "consumer.start" {
		consumerEvents += consumerInputEvent + `:
  entity_id: text
`
	}
	consumerEvents += `consumer.done:
  entity_id: text
`
	writePinRoutingVerifyFile(t, filepath.Join(root, "flows", "consumer", "events.yaml"), consumerEvents)
	writePinRoutingVerifyFile(t, filepath.Join(root, "flows", "consumer", "entities.yaml"), `
consumer:
  entity_id: text
`)
	writePinRoutingVerifyFile(t, filepath.Join(root, "flows", "consumer", "nodes.yaml"), "{}\n")
	repoRoot := filepath.Clean(filepath.Join("..", "..", ".."))
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	return bundle
}

func loadPinRoutingProducerAgentRouteBundleForEvents(t *testing.T, producerOutputEvent, consumerInputEvent string, connectBlocks ...string) *runtimecontracts.WorkflowContractBundle {
	return loadPinRoutingProducerAgentRouteBundleWithRootOutputs(t, producerOutputEvent, consumerInputEvent, nil, connectBlocks...)
}

func loadPinRoutingProducerAgentRouteBundleWithRootOutputs(t *testing.T, producerOutputEvent, consumerInputEvent string, rootOutputEvents []string, connectBlocks ...string) *runtimecontracts.WorkflowContractBundle {
	t.Helper()
	root := t.TempDir()
	writePinRoutingVerifyFile(t, filepath.Join(root, "package.yaml"), `
name: pin-routing-producer-agent-route
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
flows:
  - id: producer
    flow: producer
    mode: static
  - id: consumer
    flow: consumer
    mode: static
`+strings.Join(connectBlocks, ""))
	rootSchema := "name: pin-routing-producer-agent-route\n"
	if len(rootOutputEvents) > 0 {
		rootSchema = `
name: pin-routing-producer-agent-route
pins:
  outputs:
    events: [` + strings.Join(rootOutputEvents, ", ") + `]
`
	}
	writePinRoutingVerifyFile(t, filepath.Join(root, "schema.yaml"), rootSchema)
	writePinRoutingVerifyFile(t, filepath.Join(root, "policy.yaml"), "{}\n")
	writePinRoutingVerifyFile(t, filepath.Join(root, "tools.yaml"), "{}\n")
	writePinRoutingVerifyFile(t, filepath.Join(root, "agents.yaml"), "{}\n")
	writePinRoutingVerifyFile(t, filepath.Join(root, "events.yaml"), "{}\n")
	writePinRoutingVerifyFile(t, filepath.Join(root, "entities.yaml"), "{}\n")
	writePinRoutingVerifyFile(t, filepath.Join(root, "flows", "producer", "schema.yaml"), `
name: producer
initial_state: pending
states: [pending, done]
terminal_states: [done]
pins:
  inputs:
    events: [producer.start]
  outputs:
    events: [`+producerOutputEvent+`]
`)
	writePinRoutingVerifyFile(t, filepath.Join(root, "flows", "producer", "events.yaml"), `
producer.start:
  entity_id: text
`+producerOutputEvent+`:
  entity_id: text
`)
	writePinRoutingVerifyFile(t, filepath.Join(root, "flows", "producer", "entities.yaml"), `
producer:
  entity_id: text
`)
	writePinRoutingVerifyFile(t, filepath.Join(root, "flows", "producer", "agents.yaml"), `
producer-agent:
  id: producer-agent
  role: producer
  mode: task
  emit_events:
    - `+producerOutputEvent+`
`)
	writePinRoutingVerifyFile(t, filepath.Join(root, "flows", "producer", "nodes.yaml"), "{}\n")
	writePinRoutingVerifyFile(t, filepath.Join(root, "flows", "consumer", "schema.yaml"), `
name: consumer
initial_state: pending
states: [pending, done]
terminal_states: [done]
pins:
  inputs:
    events: [`+consumerInputEvent+`]
  outputs:
    events: [consumer.done]
`)
	consumerEvents := `
consumer.start:
  entity_id: text
`
	if consumerInputEvent != "consumer.start" {
		consumerEvents += consumerInputEvent + `:
  entity_id: text
`
	}
	consumerEvents += `consumer.done:
  entity_id: text
`
	writePinRoutingVerifyFile(t, filepath.Join(root, "flows", "consumer", "events.yaml"), consumerEvents)
	writePinRoutingVerifyFile(t, filepath.Join(root, "flows", "consumer", "entities.yaml"), `
consumer:
  entity_id: text
`)
	writePinRoutingVerifyFile(t, filepath.Join(root, "flows", "consumer", "nodes.yaml"), "{}\n")
	repoRoot := filepath.Clean(filepath.Join("..", "..", ".."))
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	return bundle
}

func pinRoutingProducerRouteConnect() string {
	return `
connect:
  - from: producer.shared.ready
    to: consumer.shared.ready
`
}

func pinRoutingProducerRouteAdaptedConnect() string {
	return `
connect:
  - from: producer.shared.ready
    to: consumer.consumer.ready
    adapter: producer-shared-to-consumer-ready
`
}

func loadSelectEntityDemotionBundle(t *testing.T, opts canonicalrouting.SelectEntityDemotionOptions) *runtimecontracts.WorkflowContractBundle {
	t.Helper()
	repoRoot := filepath.Clean(filepath.Join("..", "..", ".."))
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(
		repoRoot,
		canonicalrouting.CopySelectEntityDemotion(t, opts),
		runtimecontracts.DefaultPlatformSpecFile(repoRoot),
	)
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	return bundle
}

func useStagedLifecycleForFlow(t *testing.T, bundle *runtimecontracts.WorkflowContractBundle, flowID, initial string, states, terminals []string) {
	t.Helper()
	if bundle == nil {
		t.Fatal("bundle is nil")
	}
	flowID = strings.TrimSpace(flowID)
	if flowID == "" {
		t.Fatal("flowID is required")
	}
	schema, ok := bundle.FlowSchemas[flowID]
	if !ok {
		t.Fatalf("flow schema %s missing", flowID)
	}
	terminalSet := map[string]struct{}{}
	for _, terminal := range terminals {
		terminal = strings.TrimSpace(terminal)
		if terminal != "" {
			terminalSet[terminal] = struct{}{}
		}
	}
	entries := make([]runtimecontracts.FlowStageDeclaration, 0, len(states))
	for _, state := range states {
		state = strings.TrimSpace(state)
		if state == "" {
			continue
		}
		_, terminal := terminalSet[state]
		entries = append(entries, runtimecontracts.FlowStageDeclaration{
			ID:       state,
			Initial:  state == strings.TrimSpace(initial),
			Terminal: terminal,
		})
	}
	schema.InitialState = ""
	schema.InitialStateDeclared = false
	schema.States = nil
	schema.StatesDeclared = false
	schema.TerminalStates = nil
	schema.TerminalStatesDeclared = false
	schema.StageDeclarations = runtimecontracts.FlowStageDeclarations{Declared: true, Entries: entries}
	bundle.FlowSchemas[flowID] = schema
	if bundle.Semantics.FlowInitial == nil {
		bundle.Semantics.FlowInitial = map[string]string{}
	}
	if bundle.Semantics.FlowStates == nil {
		bundle.Semantics.FlowStates = map[string][]string{}
	}
	if bundle.Semantics.FlowTerminal == nil {
		bundle.Semantics.FlowTerminal = map[string][]string{}
	}
	bundle.Semantics.FlowInitial[flowID] = schema.LoweredInitialState()
	bundle.Semantics.FlowStates[flowID] = schema.LoweredStates()
	bundle.Semantics.FlowTerminal[flowID] = schema.LoweredTerminalStates()
	if view, ok := bundle.FlowViewByID(flowID); ok && view != nil {
		view.Schema = schema
	}
}

type pinRoutingVerifySourceFixture struct {
	rootNodes        string
	supportFlowNodes string
	extrasNodes      string
	extrasAgents     string
}

func loadPinRoutingVerifySourceFixture(t *testing.T, opts pinRoutingVerifySourceFixture) *runtimecontracts.WorkflowContractBundle {
	t.Helper()
	root := t.TempDir()
	writePinRoutingVerifyFile(t, filepath.Join(root, "package.yaml"), `
name: pin-routing-source-identity
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
packages:
  - path: extras
flows:
  - id: support
    flow: support
    mode: static
`)
	writePinRoutingVerifyFile(t, filepath.Join(root, "schema.yaml"), `
name: pin-routing-source-identity
pins:
  inputs:
    events: [root.start]
  outputs:
    events: [root.ready]
`)
	writePinRoutingVerifyFile(t, filepath.Join(root, "events.yaml"), `
root.start: {}
root.ready: {}
`)
	writePinRoutingVerifyFile(t, filepath.Join(root, "policy.yaml"), "{}\n")
	writePinRoutingVerifyFile(t, filepath.Join(root, "tools.yaml"), "{}\n")
	writePinRoutingVerifyFile(t, filepath.Join(root, "agents.yaml"), "{}\n")
	writePinRoutingVerifyFile(t, filepath.Join(root, "entities.yaml"), "{}\n")
	writePinRoutingVerifyFile(t, filepath.Join(root, "nodes.yaml"), defaultPinRoutingFixtureYAML(opts.rootNodes))
	writePinRoutingVerifyFile(t, filepath.Join(root, "flows", "support", "package.yaml"), `
name: support
version: "1.0.0"
flows: []
`)
	writePinRoutingVerifyFile(t, filepath.Join(root, "flows", "support", "schema.yaml"), `
name: support
initial_state: pending
states: [pending, done]
terminal_states: [done]
pins:
  inputs:
    events: [support.start]
  outputs:
    events: [support.ready]
`)
	writePinRoutingVerifyFile(t, filepath.Join(root, "flows", "support", "events.yaml"), `
support.start: {}
support.ready: {}
`)
	writePinRoutingVerifyFile(t, filepath.Join(root, "flows", "support", "policy.yaml"), "{}\n")
	writePinRoutingVerifyFile(t, filepath.Join(root, "flows", "support", "agents.yaml"), "{}\n")
	writePinRoutingVerifyFile(t, filepath.Join(root, "flows", "support", "nodes.yaml"), defaultPinRoutingFixtureYAML(opts.supportFlowNodes))
	writePinRoutingVerifyFile(t, filepath.Join(root, "extras", "package.yaml"), `
name: extras
version: "1.0.0"
flows: []
`)
	writePinRoutingVerifyFile(t, filepath.Join(root, "extras", "nodes.yaml"), defaultPinRoutingFixtureYAML(opts.extrasNodes))
	writePinRoutingVerifyFile(t, filepath.Join(root, "extras", "agents.yaml"), defaultPinRoutingFixtureYAML(opts.extrasAgents))
	repoRoot := filepath.Clean(filepath.Join("..", "..", ".."))
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	return bundle
}

func loadPinRoutingRootConnectBundle(t *testing.T, emitBlock string) *runtimecontracts.WorkflowContractBundle {
	t.Helper()
	emit := canonicalrouting.RootConnectCanonicalEmit
	if strings.Contains(emitBlock, "broadcast: true") {
		emit = canonicalrouting.RootConnectBroadcastEmit
	} else if strings.Contains(emitBlock, "target:") {
		emit = canonicalrouting.RootConnectTargetEmit
	} else if !strings.Contains(emitBlock, "event: root.ready") {
		t.Fatalf("unsupported root connect emit shape %q", emitBlock)
	}
	root := canonicalrouting.CopyRootOutputConnect(t, emit)
	repoRoot := filepath.Clean(filepath.Join("..", "..", ".."))
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	return bundle
}

func pinRoutingVerifyNodeYAML(nodeID, trigger, eventType string, broadcast bool) string {
	if strings.TrimSpace(nodeID) == "" {
		return "{}\n"
	}
	broadcastLine := ""
	if broadcast {
		broadcastLine = "        broadcast: true\n"
	}
	return `
` + nodeID + `:
  id: ` + nodeID + `
  execution_type: system_node
  event_handlers:
    ` + trigger + `:
      emit:
        event: ` + eventType + `
` + broadcastLine
}

func pinRoutingVerifyGuardNodeYAML(nodeID, trigger, eventType string) string {
	if strings.TrimSpace(nodeID) == "" {
		return "{}\n"
	}
	return `
` + nodeID + `:
  id: ` + nodeID + `
  execution_type: system_node
  event_handlers:
    ` + trigger + `:
      guard:
        id: guard-escalate
        check: "false"
        on_fail: "escalate:` + eventType + `"
`
}

func pinRoutingVerifyRuleNodeYAML(nodeID, trigger, eventType string, broadcast bool) string {
	if strings.TrimSpace(nodeID) == "" {
		return "{}\n"
	}
	broadcastLine := ""
	if broadcast {
		broadcastLine = "          broadcast: true\n"
	}
	return `
` + nodeID + `:
  id: ` + nodeID + `
  execution_type: system_node
  event_handlers:
    ` + trigger + `:
      rules:
        - id: emit-root
          condition: "true"
          emit:
            event: ` + eventType + `
` + broadcastLine
}

func defaultPinRoutingFixtureYAML(value string) string {
	if strings.TrimSpace(value) == "" {
		return "{}\n"
	}
	return value
}

func countPinTargetFindings(report Report, messagePart string) int {
	count := 0
	for _, finding := range report.Errors() {
		if finding.CheckID == "pin_target_resolution" && strings.Contains(finding.Message, messagePart) {
			count++
		}
	}
	return count
}

func pinTargetFindingMessages(report Report) []string {
	out := []string{}
	for _, finding := range report.Errors() {
		if finding.CheckID == "pin_target_resolution" {
			out = append(out, finding.Message)
		}
	}
	return out
}

func writePinRoutingVerifyFile(t *testing.T, path, contents string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", filepath.Dir(path), err)
	}
	if err := os.WriteFile(path, []byte(contents), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}
