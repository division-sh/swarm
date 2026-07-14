package canonicalrouting

import (
	"fmt"
	"testing"
)

type RuntimeAgentMemoryVariant uint8

const (
	RuntimeAgentMemoryPackageBacked RuntimeAgentMemoryVariant = iota + 1
	RuntimeAgentMemorySoleParent
)

func CopyRuntimeAgentMemory(t testing.TB, variant RuntimeAgentMemoryVariant) string {
	t.Helper()
	root := CopyExample(t, RootIngress)
	removeInheritedScenarios(t, root)
	packageBody := "name: session-scope-validation\nversion: \"1.0.0\"\nplatform_version: \">=0.7.0 <0.8.0\"\nflows:\n  - id: support\n    flow: support\n    mode: static\n"
	if variant == RuntimeAgentMemorySoleParent {
		packageBody = "name: session-scope-validation\nversion: \"1.0.0\"\nplatform_version: \">=0.7.0 <0.8.0\"\npackages:\n  - path: extras\nflows:\n  - id: support\n    flow: support\n    mode: static\n"
	} else if variant != RuntimeAgentMemoryPackageBacked {
		t.Fatalf("unsupported runtime agent-memory variant %d", variant)
	}
	writeClosedVariantFile(t, root, "package.yaml", packageBody)
	writeClosedVariantFile(t, root, "entities.yaml", "item:\n  item_id:\n    type: string\n    _unused_reason: startup scope fixture field\n")
	writeClosedVariantFile(t, root, "schema.yaml", "name: session-scope-validation\n")
	for _, file := range []string{"policy.yaml", "tools.yaml", "agents.yaml", "nodes.yaml"} {
		writeClosedVariantFile(t, root, file, "{}\n")
	}
	writeClosedVariantFile(t, root, "events.yaml", "item.created:\n  swarm:\n    source: external (bootstrap fixture)\n  entity_id: string\n")
	writeClosedVariantFile(t, root, "flows/support/schema.yaml", "name: support\ninitial_state: waiting\nstates:\n  - waiting\n")
	writeClosedVariantFile(t, root, "flows/support/policy.yaml", "{}\n")
	writeClosedVariantFile(t, root, "flows/support/events.yaml", "support/item.created:\n  entity_id: string\n")
	agentBody := "backend:\n  id: backend-{vertical_id}\n  type: generic\n  role: backend\n  model: regular\n  memory: true\n  subscriptions:\n    - support/item.created\n  emit_events:\n    - support/item.created\n"
	if variant == RuntimeAgentMemoryPackageBacked {
		writeClosedVariantFile(t, root, "flows/support/package.yaml", "name: support\nversion: \"1.0.0\"\nplatform_version: \">=0.7.0 <0.8.0\"\nflows: []\n")
		writeClosedVariantFile(t, root, "flows/support/prompts/backend.md", "Handle support events.\n")
		writeClosedVariantFile(t, root, "flows/support/agents.yaml", agentBody)
	} else {
		writeClosedVariantFile(t, root, "extras/package.yaml", "name: extras\nversion: \"1.0.0\"\nplatform_version: \">=0.7.0 <0.8.0\"\nflows: []\n")
		writeClosedVariantFile(t, root, "extras/prompts/backend.md", "Handle support events.\n")
		writeClosedVariantFile(t, root, "extras/agents.yaml", agentBody)
	}
	return root
}

type EventMetadataAuthorityVariant uint8

const (
	EventMetadataAuthorityDefault EventMetadataAuthorityVariant = iota + 1
	EventMetadataAuthorityTaskProducerNode
	EventMetadataAuthorityTaskConsumerNode
	EventMetadataAuthorityTaskSourceNode
	EventMetadataAuthorityTaskProducerAgent
	EventMetadataAuthorityTaskConsumerAgent
	EventMetadataAuthorityTaskProducerTimer
	EventMetadataAuthorityExternalProof
)

func CopyEventMetadataAuthority(t testing.TB, variant EventMetadataAuthorityVariant) string {
	t.Helper()
	root := CopyExample(t, RootIngress)
	removeInheritedScenarios(t, root)
	taskDoneSwarm := ""
	agents := "{}\n"
	timerBlock := ""
	externalRequestedSwarm := "    source: external"
	switch variant {
	case EventMetadataAuthorityDefault:
	case EventMetadataAuthorityTaskProducerNode:
		taskDoneSwarm = "    producer: worker\n"
	case EventMetadataAuthorityTaskConsumerNode:
		taskDoneSwarm = "    consumer: observer\n"
	case EventMetadataAuthorityTaskSourceNode:
		taskDoneSwarm = "    source: worker\n"
	case EventMetadataAuthorityTaskProducerAgent:
		taskDoneSwarm = "    producer: reviewer\n"
		agents = "reviewer-agent:\n  id: reviewer-agent\n  role: reviewer\n  emit_events: [task.done]\n"
	case EventMetadataAuthorityTaskConsumerAgent:
		taskDoneSwarm = "    consumer: reviewer\n"
		agents = "reviewer-agent:\n  id: reviewer-agent\n  role: reviewer\n  subscriptions: [task.done]\n"
	case EventMetadataAuthorityTaskProducerTimer:
		taskDoneSwarm = "    producer: reminder\n"
		timerBlock = "  timers:\n    - id: reminder\n      owner: worker\n      event: task.done\n      delay: 1m\n      start_on: event:task.start\n"
	case EventMetadataAuthorityExternalProof:
		externalRequestedSwarm = "    source: external webhook\n    producer: mailbox_human\n    consumer: external_ui"
		taskDoneSwarm = "    source: platform timer\n    producer: mailbox_human\n    consumer: external_ui\n"
	default:
		t.Fatalf("unsupported event-metadata authority variant %d", variant)
	}
	writeClosedVariantFile(t, root, "package.yaml", "name: event-metadata-authority\nversion: \"1.0.0\"\nplatform_version: \">=0.7.0 <0.8.0\"\n")
	writeClosedVariantFile(t, root, "schema.yaml", "name: event-metadata-authority\n")
	for _, file := range []string{"policy.yaml", "tools.yaml"} {
		writeClosedVariantFile(t, root, file, "{}\n")
	}
	writeClosedVariantFile(t, root, "agents.yaml", agents)
	events := "external.requested:\n  swarm:\n" + externalRequestedSwarm + "\ntask.start:\n  swarm:\n    source: external\ntask.done:\n"
	if taskDoneSwarm != "" {
		events += "  swarm:\n" + taskDoneSwarm
	}
	writeClosedVariantFile(t, root, "events.yaml", events)
	writeClosedVariantFile(t, root, "nodes.yaml", "worker:\n  id: worker\n  execution_type: system_node\n"+timerBlock+"  event_handlers:\n    task.start:\n      emit:\n        event: task.done\nobserver:\n  id: observer\n  execution_type: system_node\n  event_handlers:\n    task.done: {}\n")
	return root
}

func CopyDeadEventSchemaExternalSource(t testing.TB) string {
	t.Helper()
	root := CopyExample(t, RootIngress)
	removeInheritedScenarios(t, root)
	writeClosedVariantFile(t, root, "package.yaml", "name: dead-event-schema-external-source\nversion: \"1.0.0\"\nplatform_version: \">=0.7.0 <0.8.0\"\nflows:\n  - id: support\n    flow: support\n    mode: static\n")
	writeClosedVariantFile(t, root, "schema.yaml", "name: dead-event-schema-external-source\n")
	for _, file := range []string{"policy.yaml", "tools.yaml", "agents.yaml", "events.yaml", "nodes.yaml"} {
		writeClosedVariantFile(t, root, file, "{}\n")
	}
	writeClosedVariantFile(t, root, "flows/support/schema.yaml", "name: support\ninitial_state: idle\nterminal_states: [done]\nstates: [idle, done]\npins:\n  inputs:\n    events: []\n  outputs:\n    events: []\n")
	for _, file := range []string{"policy.yaml", "agents.yaml", "nodes.yaml"} {
		writeClosedVariantFile(t, root, "flows/support/"+file, "{}\n")
	}
	writeClosedVariantFile(t, root, "flows/support/events.yaml", "ticket.ready:\n  swarm:\n    source: external (manual handoff)\n")
	return root
}

type TimerValidationVariant uint8

const (
	TimerValidationDefault TimerValidationVariant = iota + 1
	TimerValidationUnprefixedStart
	TimerValidationCancelBoot
	TimerValidationUnknownStartState
	TimerValidationUnknownCancelState
	TimerValidationUnknownStartEvent
	TimerValidationNoConsumer
	TimerValidationAgentConsumer
	TimerValidationWildcardConsumer
	TimerValidationExternalConsumer
	TimerValidationOutputBoundary
	TimerValidationMissingStartProducer
	TimerValidationMissingCancelProducer
	TimerValidationUnknownCancelEvent
	TimerValidationBoot
	TimerValidationBootCancelState
	TimerValidationBootCancelEvent
)

type timerValidationSettings struct {
	startOn              string
	cancelOn             string
	omitTimerHandler     bool
	timerHandlerKey      string
	timerEventSwarm      string
	flowOutput           bool
	flowAgent            bool
	externalTicketClosed bool
}

func CopyTimerValidation(t testing.TB, variant TimerValidationVariant) string {
	t.Helper()
	settings := timerValidationSettings{startOn: "event:ticket.opened", externalTicketClosed: true}
	switch variant {
	case TimerValidationDefault:
	case TimerValidationUnprefixedStart:
		settings.startOn = "ticket.opened"
	case TimerValidationCancelBoot:
		settings.cancelOn = "boot"
	case TimerValidationUnknownStartState:
		settings.startOn = "state:missing_state"
	case TimerValidationUnknownCancelState:
		settings.cancelOn = "state:missing_state"
	case TimerValidationUnknownStartEvent:
		settings.startOn = "event:ticket.unknown"
	case TimerValidationNoConsumer:
		settings.omitTimerHandler = true
	case TimerValidationAgentConsumer:
		settings.omitTimerHandler, settings.flowAgent = true, true
	case TimerValidationWildcardConsumer:
		settings.timerHandlerKey = "timer.*"
	case TimerValidationExternalConsumer:
		settings.omitTimerHandler, settings.timerEventSwarm = true, "consumer: mailbox_system"
	case TimerValidationOutputBoundary:
		settings.omitTimerHandler, settings.flowOutput = true, true
	case TimerValidationMissingStartProducer:
		settings.startOn, settings.externalTicketClosed = "event:ticket.closed", false
	case TimerValidationMissingCancelProducer:
		settings.cancelOn, settings.externalTicketClosed = "event:ticket.closed", false
	case TimerValidationUnknownCancelEvent:
		settings.cancelOn = "event:ticket.unknown"
	case TimerValidationBoot:
		settings.startOn = "boot"
	case TimerValidationBootCancelState:
		settings.startOn, settings.cancelOn = "boot", "state:done"
	case TimerValidationBootCancelEvent:
		settings.startOn, settings.cancelOn = "boot", "event:ticket.closed"
	default:
		t.Fatalf("unsupported timer-validation variant %d", variant)
	}
	return copyTimerValidation(t, settings)
}

func copyTimerValidation(t testing.TB, settings timerValidationSettings) string {
	root := CopyExample(t, RootIngress)
	removeInheritedScenarios(t, root)
	writeClosedVariantFile(t, root, "package.yaml", "name: timer-validation\nversion: \"1.0.0\"\nplatform_version: \">=0.7.0 <0.8.0\"\nflows:\n  - id: support\n    flow: support\n    mode: static\n")
	writeClosedVariantFile(t, root, "entities.yaml", "ticket:\n  ticket_id: string\n")
	writeClosedVariantFile(t, root, "schema.yaml", "name: timer-validation\n")
	for _, file := range []string{"policy.yaml", "tools.yaml", "agents.yaml", "events.yaml", "nodes.yaml"} {
		writeClosedVariantFile(t, root, file, "{}\n")
	}
	flowPins := ""
	if settings.flowOutput {
		flowPins = "pins:\n  inputs:\n    events: []\n  outputs:\n    events:\n      - timer.reminder\n"
	}
	writeClosedVariantFile(t, root, "flows/support/schema.yaml", "name: support\ninitial_state: waiting\nterminal_states: [done]\nstates: [waiting, active, done]\n"+flowPins)
	writeClosedVariantFile(t, root, "flows/support/policy.yaml", "{}\n")
	agents := "{}\n"
	if settings.flowAgent {
		agents = "reminder-agent:\n  model: regular\n  memory: false\n  subscriptions: [timer.reminder]\n  emit_events: []\n"
	}
	writeClosedVariantFile(t, root, "flows/support/agents.yaml", agents)
	closedSource := ""
	if settings.externalTicketClosed {
		closedSource = "  swarm:\n    source: external\n"
	}
	events := "ticket.opened:\n  entity_id: string\n  swarm:\n    source: external\nticket.closed:\n  entity_id: string\n" + closedSource + "timer.reminder:\n  entity_id: string\n"
	if settings.timerEventSwarm != "" {
		events += "  swarm:\n    " + settings.timerEventSwarm + "\n"
	}
	writeClosedVariantFile(t, root, "flows/support/events.yaml", events)
	timerHandlerKey := settings.timerHandlerKey
	if timerHandlerKey == "" {
		timerHandlerKey = "timer.reminder"
	}
	timerBlock := "    - id: reminder\n      owner: support-node\n      event: timer.reminder\n      delay: 1m\n      start_on: " + settings.startOn + "\n"
	if settings.cancelOn != "" {
		timerBlock += "      cancel_on: " + settings.cancelOn + "\n"
	}
	timerHandler := ""
	if !settings.omitTimerHandler {
		timerHandler = "    " + timerHandlerKey + ":\n      advances_to: done\n"
	}
	writeClosedVariantFile(t, root, "flows/support/nodes.yaml", "support-node:\n  id: support-node\n  execution_type: system_node\n  subscribes_to:\n    - ticket.opened\n    - ticket.closed\n    - timer.reminder\n  timers:\n"+timerBlock+"  event_handlers:\n    ticket.opened:\n      create_entity: true\n      advances_to: active\n    ticket.closed:\n      advances_to: done\n"+timerHandler)
	return root
}

type TimerStateCancelVariant uint8

const (
	TimerStateCancelReachableState TimerStateCancelVariant = iota + 1
	TimerStateCancelUnreachableState
	TimerStateCancelGloballyUnreachableReview
	TimerStateCancelReachableEvent
	TimerStateCancelPartialEventActivation
	TimerStateCancelUnreachableEvent
	TimerStateCancelTimerFireOnly
)

type timerStateCancelSettings struct {
	startOn                         string
	cancelOn                        string
	includeClosePath                bool
	includeGlobalDonePath           bool
	includeGlobalReviewPath         bool
	includeTimerFireHandler         bool
	includeEventStartReviewBranch   bool
	treatReviewAsTerminalActivation bool
}

func CopyTimerStateCancelReachability(t testing.TB, variant TimerStateCancelVariant) string {
	t.Helper()
	settings := timerStateCancelSettings{}
	switch variant {
	case TimerStateCancelReachableState:
		settings.startOn, settings.cancelOn, settings.includeClosePath = "state:active", "state:done", true
	case TimerStateCancelUnreachableState:
		settings.startOn, settings.cancelOn = "state:active", "state:done"
		settings.includeGlobalDonePath, settings.includeGlobalReviewPath = true, true
	case TimerStateCancelGloballyUnreachableReview:
		settings.startOn, settings.cancelOn, settings.includeGlobalDonePath = "state:active", "state:review", true
	case TimerStateCancelReachableEvent:
		settings.startOn, settings.cancelOn, settings.includeClosePath = "event:ticket.opened", "state:done", true
	case TimerStateCancelPartialEventActivation:
		settings.startOn, settings.cancelOn, settings.includeClosePath = "event:ticket.opened", "state:done", true
		settings.includeEventStartReviewBranch, settings.treatReviewAsTerminalActivation = true, true
	case TimerStateCancelUnreachableEvent:
		settings.startOn, settings.cancelOn = "event:ticket.opened", "state:review"
		settings.includeGlobalDonePath, settings.includeGlobalReviewPath = true, true
	case TimerStateCancelTimerFireOnly:
		settings.startOn, settings.cancelOn = "state:active", "state:done"
		settings.includeTimerFireHandler, settings.includeGlobalDonePath = true, true
	default:
		t.Fatalf("unsupported timer-state-cancel variant %d", variant)
	}
	return copyTimerStateCancelReachability(t, settings)
}

func copyTimerStateCancelReachability(t testing.TB, settings timerStateCancelSettings) string {
	root := CopyExample(t, RootIngress)
	removeInheritedScenarios(t, root)
	writeClosedVariantFile(t, root, "package.yaml", "name: timer-state-cancel-reachability\nversion: \"1.0.0\"\nplatform_version: \">=0.7.0 <0.8.0\"\nflows:\n  - id: support\n    flow: support\n    mode: static\n")
	writeClosedVariantFile(t, root, "entities.yaml", "ticket:\n  ticket_id: string\n")
	writeClosedVariantFile(t, root, "schema.yaml", "name: timer-state-cancel-reachability\n")
	for _, file := range []string{"policy.yaml", "tools.yaml", "agents.yaml", "events.yaml", "nodes.yaml"} {
		writeClosedVariantFile(t, root, file, "{}\n")
	}
	terminalStates := "[done]"
	if settings.treatReviewAsTerminalActivation {
		terminalStates = "[review, done]"
	}
	writeClosedVariantFile(t, root, "flows/support/schema.yaml", "name: support\ninitial_state: waiting\nterminal_states: "+terminalStates+"\nstates: [waiting, active, review, done]\n")
	for _, file := range []string{"policy.yaml", "agents.yaml"} {
		writeClosedVariantFile(t, root, "flows/support/"+file, "{}\n")
	}
	writeClosedVariantFile(t, root, "flows/support/events.yaml", "ticket.opened:\n  swarm:\n    source: external (test)\nticket.closed:\n  entity_id: string\nadmin.done:\n  swarm:\n    source: external (test)\nadmin.review:\n  swarm:\n    source: external (test)\ntimer.reminder:\n  swarm:\n    consumer: mailbox_system\n")
	timerBlock := "    - id: reminder\n      owner: support-node\n      event: timer.reminder\n      delay: 1m\n      start_on: " + settings.startOn + "\n"
	if settings.cancelOn != "" {
		timerBlock += "      cancel_on: " + settings.cancelOn + "\n"
	}
	handlers := ""
	if settings.includeEventStartReviewBranch {
		handlers += "    ticket.opened:\n      rules:\n        - id: active_path\n          condition: \"true\"\n          advances_to: active\n        - id: review_path\n          condition: \"true\"\n          advances_to: review\n"
	} else {
		handlers += "    ticket.opened:\n      create_entity: true\n      advances_to: active\n"
	}
	if settings.includeClosePath {
		handlers += "    ticket.closed:\n      advances_to: done\n"
	}
	if settings.includeGlobalDonePath {
		handlers += "    admin.done:\n      create_entity: true\n      advances_to: done\n"
	}
	if settings.includeGlobalReviewPath {
		handlers += "    admin.review:\n      create_entity: true\n      advances_to: review\n"
	}
	if settings.includeTimerFireHandler {
		handlers += "    timer.reminder:\n      advances_to: done\n"
	}
	writeClosedVariantFile(t, root, "flows/support/nodes.yaml", "support-node:\n  id: support-node\n  execution_type: system_node\n  subscribes_to:\n    - ticket.opened\n    - ticket.closed\n    - admin.done\n    - admin.review\n    - timer.reminder\n  timers:\n"+timerBlock+"  event_handlers:\n"+handlers)
	return root
}

type PinRoutingProducerRouteVariant uint8

const (
	PinRoutingTargetAdapted PinRoutingProducerRouteVariant = iota + 1
	PinRoutingBroadcastAdapted
	PinRoutingTargetDirect
	PinRoutingUnknownTargetDirect
	PinRoutingAgentDirect
	PinRoutingAgentDirectWithRootOutput
)

func CopyPinRoutingProducerRoute(t testing.TB, variant PinRoutingProducerRouteVariant) string {
	t.Helper()
	root := CopyExample(t, ParentConnect)
	removeInheritedScenarios(t, root)
	adapted := variant == PinRoutingTargetAdapted || variant == PinRoutingBroadcastAdapted
	agent := variant == PinRoutingAgentDirect || variant == PinRoutingAgentDirectWithRootOutput
	consumerEvent := "shared.ready"
	connect := "connect:\n  - from: producer.shared.ready\n    to: consumer.shared.ready\n"
	if adapted {
		consumerEvent = "consumer.ready"
		connect = "connect:\n  - from: producer.shared.ready\n    to: consumer.consumer.ready\n    adapter: producer-shared-to-consumer-ready\n"
	}
	rootSchema := "name: pin-routing-producer-route\n"
	if variant == PinRoutingAgentDirectWithRootOutput {
		rootSchema += "pins:\n  outputs:\n    events: [shared.ready]\n"
	}
	writeClosedVariantFile(t, root, "package.yaml", "name: pin-routing-producer-route\nversion: \"1.0.0\"\nplatform_version: \">=0.7.0 <0.8.0\"\nflows:\n  - id: producer\n    flow: producer\n    mode: static\n  - id: consumer\n    flow: consumer\n    mode: static\n"+connect)
	writeClosedVariantFile(t, root, "schema.yaml", rootSchema)
	for _, file := range []string{"policy.yaml", "tools.yaml", "agents.yaml", "events.yaml", "entities.yaml", "nodes.yaml"} {
		writeClosedVariantFile(t, root, file, "{}\n")
	}
	writeClosedVariantFile(t, root, "flows/producer/schema.yaml", "name: producer\ninitial_state: pending\nstates: [pending, done]\nterminal_states: [done]\npins:\n  inputs:\n    events: [producer.start]\n  outputs:\n    events: [shared.ready]\n")
	writeClosedVariantFile(t, root, "flows/producer/events.yaml", "producer.start:\n  entity_id: text\nshared.ready:\n  entity_id: text\n")
	writeClosedVariantFile(t, root, "flows/producer/entities.yaml", "producer:\n  entity_id: text\n")
	if agent {
		writeClosedVariantFile(t, root, "flows/producer/agents.yaml", "producer-agent:\n  id: producer-agent\n  role: producer\n  memory: false\n  emit_events:\n    - shared.ready\n")
		writeClosedVariantFile(t, root, "flows/producer/nodes.yaml", "{}\n")
	} else {
		handler := "      emit:\n        event: shared.ready\n        fields:\n          entity_id: payload.entity_id\n"
		switch variant {
		case PinRoutingTargetAdapted, PinRoutingTargetDirect:
			handler += "        target:\n          flow: consumer\n          match:\n            entity_id: payload.entity_id\n"
		case PinRoutingUnknownTargetDirect:
			handler += "        target:\n          flow: missing-consumer\n          match:\n            entity_id: payload.entity_id\n"
		case PinRoutingBroadcastAdapted:
			handler += "        broadcast: true\n"
		default:
			t.Fatalf("unsupported system-node pin-routing variant %d", variant)
		}
		writeClosedVariantFile(t, root, "flows/producer/agents.yaml", "{}\n")
		writeClosedVariantFile(t, root, "flows/producer/nodes.yaml", "producer-node:\n  id: producer-node\n  execution_type: system_node\n  event_handlers:\n    producer.start:\n"+handler)
	}
	writeClosedVariantFile(t, root, "flows/consumer/schema.yaml", "name: consumer\ninitial_state: pending\nstates: [pending, done]\nterminal_states: [done]\npins:\n  inputs:\n    events: ["+consumerEvent+"]\n  outputs:\n    events: [consumer.done]\n")
	consumerEvents := "consumer.start:\n  entity_id: text\n"
	if consumerEvent != "consumer.start" {
		consumerEvents += consumerEvent + ":\n  entity_id: text\n"
	}
	consumerEvents += "consumer.done:\n  entity_id: text\n"
	writeClosedVariantFile(t, root, "flows/consumer/events.yaml", consumerEvents)
	writeClosedVariantFile(t, root, "flows/consumer/entities.yaml", "consumer:\n  entity_id: text\n")
	writeClosedVariantFile(t, root, "flows/consumer/nodes.yaml", "{}\n")
	return root
}

func CopyVerifyStateSchemaFloat(t testing.TB) string {
	t.Helper()
	root := CopyExample(t, RootIngress)
	removeInheritedScenarios(t, root)
	writeClosedVariantFile(t, root, "package.yaml", "name: verify-state-schema-float\nversion: \"1.0.0\"\nplatform_version: \">=0.7.0 <0.8.0\"\nflows:\n  - id: child\n    flow: child\n")
	writeClosedVariantFile(t, root, "schema.yaml", "name: verify-state-schema-float\n")
	for _, file := range []string{"entities.yaml", "policy.yaml", "tools.yaml", "agents.yaml", "nodes.yaml", "events.yaml"} {
		writeClosedVariantFile(t, root, file, "{}\n")
	}
	writeClosedVariantFile(t, root, "flows/child/schema.yaml", "name: child\ninitial_state: idle\nterminal_states: [done]\nstates: [idle, done]\n")
	writeClosedVariantFile(t, root, "flows/child/entities.yaml", "case: {}\n")
	for _, file := range []string{"policy.yaml", "agents.yaml"} {
		writeClosedVariantFile(t, root, "flows/child/"+file, "{}\n")
	}
	writeClosedVariantFile(t, root, "flows/child/events.yaml", "task.assigned:\n  swarm:\n    source: external (state schema float verify test)\n")
	writeClosedVariantFile(t, root, "flows/child/nodes.yaml", "accumulator:\n  id: accumulator\n  execution_type: system_node\n  subscribes_to: [task.assigned]\n  event_handlers:\n    task.assigned:\n      advances_to: done\n  state_schema:\n    fields:\n      composite: float\n")
	return root
}

func CopyVerifyAccumulatorEntityProjection(t testing.TB) string {
	t.Helper()
	root := CopyExample(t, RootIngress)
	removeInheritedScenarios(t, root)
	writeClosedVariantFile(t, root, "package.yaml", "name: verify-accumulator-entity-projection\nversion: \"1.0.0\"\nplatform_version: \">=0.7.0 <0.8.0\"\nflows: []\n")
	writeClosedVariantFile(t, root, "schema.yaml", "name: verify-accumulator-entity-projection\ninitial_state: collecting\nterminal_states: [complete]\nstates: [collecting, complete]\npins:\n  inputs:\n    events: [score.dimension_complete]\n  outputs:\n    events: [score.completed]\n")
	for _, file := range []string{"policy.yaml", "tools.yaml", "agents.yaml"} {
		writeClosedVariantFile(t, root, file, "{}\n")
	}
	writeClosedVariantFile(t, root, "types.yaml", "types:\n  DimensionScore:\n    dimension: text\n    tier: integer\n    score: integer\n    evidence: text\n    confidence: text\n")
	writeClosedVariantFile(t, root, "entities.yaml", "vertical:\n  scores:\n    type: list<DimensionScore>\n    materialize_from: scorer.dimensions_received\n")
	writeClosedVariantFile(t, root, "events.yaml", "score.dimension_complete:\n  swarm:\n    source: external (verify accumulator projection fixture)\n  expected_dimensions: integer\n  vertical_id: string\n  dimension: text\n  tier: integer\n  score: integer\n  evidence: text\n  confidence: text\nscore.completed: {}\n")
	writeClosedVariantFile(t, root, "nodes.yaml", "scorer:\n  id: scorer\n  execution_type: system_node\n  subscribes_to: [score.dimension_complete]\n  produces: [score.completed]\n  event_handlers:\n    score.dimension_complete:\n      accumulate:\n        into: dimensions_received\n        from: payload\n      emit:\n        event: score.completed\n        broadcast: true\n      advances_to: complete\n  state_schema:\n    fields:\n      dimensions_received: list<DimensionScore>\n")
	return root
}

type VerifyModelAliasVariant uint8

const (
	VerifyModelAliasUndefined VerifyModelAliasVariant = iota + 1
	VerifyModelAliasConfigured
)

func CopyVerifyModelAlias(t testing.TB, variant VerifyModelAliasVariant) string {
	t.Helper()
	model := ""
	switch variant {
	case VerifyModelAliasUndefined:
		model = "not_configured"
	case VerifyModelAliasConfigured:
		model = "audit.custom"
	default:
		t.Fatalf("unsupported verify model-alias variant %d", variant)
	}
	root := CopyExample(t, RootIngress)
	removeInheritedScenarios(t, root)
	writeClosedVariantFile(t, root, "package.yaml", "name: verify-model-alias\nversion: \"1.0.0\"\nplatform_version: \">=0.7.0 <0.8.0\"\nflows:\n  - id: child\n    flow: child\n")
	writeClosedVariantFile(t, root, "schema.yaml", "name: verify-model-alias\n")
	for _, file := range []string{"policy.yaml", "tools.yaml", "agents.yaml", "nodes.yaml", "events.yaml", "entities.yaml"} {
		writeClosedVariantFile(t, root, file, "{}\n")
	}
	writeClosedVariantFile(t, root, "flows/child/schema.yaml", "name: child\ninitial_state: idle\nterminal_states: [done]\nstates: [idle, done]\n")
	writeClosedVariantFile(t, root, "flows/child/entities.yaml", "case: {}\n")
	writeClosedVariantFile(t, root, "flows/child/policy.yaml", "{}\n")
	writeClosedVariantFile(t, root, "flows/child/agents.yaml", fmt.Sprintf("worker:\n  id: worker\n  type: factory\n  role: worker\n  prompt_ref: worker\n  model: %s\n  memory: false\n  subscriptions: [task.assigned]\n", model))
	writeClosedVariantFile(t, root, "flows/child/events.yaml", "task.assigned:\n  swarm:\n    source: external (verify model alias test)\n")
	writeClosedVariantFile(t, root, "flows/child/nodes.yaml", "closer:\n  id: closer\n  execution_type: system_node\n  subscribes_to: [task.assigned]\n  event_handlers:\n    task.assigned:\n      advances_to: done\n")
	writeClosedVariantFile(t, root, "flows/child/prompts/worker.md", "Handle the task.\n")
	return root
}
