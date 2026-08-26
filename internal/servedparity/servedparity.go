package servedparity

import (
	"fmt"
	"strings"
	"testing"
)

type Backend string

const (
	BackendDefaultSQLite    Backend = "default_sqlite"
	BackendExplicitPostgres Backend = "explicit_postgres"
)

var RequiredBackends = []Backend{BackendDefaultSQLite, BackendExplicitPostgres}

type Postcondition string

const (
	PostconditionNoNonTerminalDeliveries Postcondition = "no_nonterminal_deliveries"
	PostconditionNoPendingPipelineEvents Postcondition = "no_pending_pipeline_events"
	PostconditionNoUnfiredDueTimers      Postcondition = "no_unfired_due_timers"
)

const (
	ScenarioEventPublishDynamicAutoEmitLifecycle = "event_publish_dynamic_auto_emit_lifecycle"
	ScenarioEventReplayLiveAgentLifecycle        = "event_replay_live_agent_lifecycle"
	ScenarioAgentReplayLiveAgentLifecycle        = "agent_replay_live_agent_lifecycle"
	ScenarioRunStopControlLifecycle              = "run_stop_control_lifecycle"
	ScenarioRunPauseControlLifecycle             = "run_pause_control_lifecycle"
	ScenarioRunContinueControlLifecycle          = "run_continue_control_lifecycle"
	ScenarioAgentRestartLifecycle                = "agent_restart_lifecycle"
	ScenarioAgentDirectiveOutcomeLifecycle       = "agent_directive_outcome_lifecycle"
	ScenarioRuntimePauseIngressLifecycle         = "runtime_pause_ingress_lifecycle"
	ScenarioRuntimeResumeIngressLifecycle        = "runtime_resume_ingress_lifecycle"
	ScenarioStandingServiceSuspendLifecycle      = "standing_service_suspend_lifecycle"
	ScenarioStandingServiceResumeLifecycle       = "standing_service_resume_lifecycle"
	ScenarioStandingServiceResetLifecycle        = "standing_service_reset_lifecycle"
	ScenarioMailboxNoticeAcknowledgmentLifecycle = "mailbox_notice_acknowledgment_lifecycle"
	ScenarioMailboxBeginInputLifecycle           = "mailbox_begin_input_lifecycle"
	ScenarioMailboxCancelInputLifecycle          = "mailbox_cancel_input_lifecycle"
	ScenarioMailboxDecisionCardLifecycle         = "mailbox_decision_card_lifecycle"
	ScenarioMailboxDeferDecisionLifecycle        = "mailbox_defer_decision_lifecycle"
	ScenarioTestSetupEntitiesLifecycle           = "test_setup_entities_lifecycle"
	ScenarioGeneratedInputFixtureLifecycle       = "generated_input_fixture_lifecycle"
	ScenarioDerivedScenarioLifecycle             = "derived_scenario_lifecycle"
	ScenarioPublicMockApprovalLifecycle          = "public_mock_approval_lifecycle"
	ScenarioBundleRegisterLifecycle              = "bundle_register_lifecycle"
	ScenarioConversationForkLifecycle            = "conversation_fork_lifecycle"
	ScenarioConversationForkChatLifecycle        = "conversation_fork_chat_lifecycle"
	ScenarioConversationForkDeleteLifecycle      = "conversation_fork_delete_lifecycle"
	ScenarioOperatorChannelConnectLifecycle      = "operator_channel_connect_lifecycle"
	ScenarioOperatorChannelReconnectLifecycle    = "operator_channel_reconnect_lifecycle"
	ScenarioOperatorChannelRebindLifecycle       = "operator_channel_rebind_lifecycle"
	ScenarioOperatorChannelConfirmLifecycle      = "operator_channel_confirm_lifecycle"
	ScenarioOperatorChannelUnbindLifecycle       = "operator_channel_unbind_lifecycle"
	ScenarioOperatorChannelProofRevokeLifecycle  = "operator_channel_proof_revoke_lifecycle"
)

type Scenario struct {
	ID             string
	APIMethod      string
	TestName       string
	Backends       []Backend
	Postconditions []Postcondition
}

type SettlementCounts struct {
	NonTerminalDeliveries int
	PendingPipelineEvents int
	UnfiredDueTimers      int
}

func Scenarios() []Scenario {
	return []Scenario{
		{
			ID:        ScenarioEventPublishDynamicAutoEmitLifecycle,
			APIMethod: "event.publish",
			TestName:  "TestServedParityHarnessEventPublishDynamicAutoEmitLifecycle",
			Backends:  append([]Backend(nil), RequiredBackends...),
			Postconditions: []Postcondition{
				PostconditionNoNonTerminalDeliveries,
				PostconditionNoPendingPipelineEvents,
				PostconditionNoUnfiredDueTimers,
			},
		},
		servedControlScenario(ScenarioEventReplayLiveAgentLifecycle, "event.replay", "TestServedParityHarnessLiveAgentEventReplayLifecycle"),
		servedControlScenario(ScenarioAgentReplayLiveAgentLifecycle, "agent.replay", "TestServedParityHarnessLiveAgentEventReplayLifecycle"),
		servedControlScenario(ScenarioRunStopControlLifecycle, "run.stop", "TestServedParityHarnessRunControlLifecycle"),
		servedControlScenario(ScenarioRunPauseControlLifecycle, "run.pause", "TestServedParityHarnessRunControlLifecycle"),
		servedControlScenario(ScenarioRunContinueControlLifecycle, "run.continue", "TestServedParityHarnessRunControlLifecycle"),
		servedControlScenario(ScenarioAgentRestartLifecycle, "agent.restart", "TestServedParityHarnessAgentRestartLifecycle"),
		servedControlScenario(ScenarioAgentDirectiveOutcomeLifecycle, "agent.send_directive", "TestServedParityHarnessAgentDirectiveOutcomeLifecycle"),
		servedControlScenario(ScenarioRuntimePauseIngressLifecycle, "runtime.pause", "TestServedParityHarnessRuntimeIngressControlLifecycle"),
		servedControlScenario(ScenarioRuntimeResumeIngressLifecycle, "runtime.resume", "TestServedParityHarnessRuntimeIngressControlLifecycle"),
		servedControlScenario(ScenarioStandingServiceSuspendLifecycle, "standing.suspend", "TestServedParityHarnessStandingServiceLifecycle"),
		servedControlScenario(ScenarioStandingServiceResumeLifecycle, "standing.resume", "TestServedParityHarnessStandingServiceLifecycle"),
		servedControlScenario(ScenarioStandingServiceResetLifecycle, "standing.reset", "TestServedParityHarnessStandingServiceLifecycle"),
		servedMailboxDecisionScenario(ScenarioMailboxNoticeAcknowledgmentLifecycle, "mailbox.acknowledge"),
		servedMailboxDecisionScenario(ScenarioMailboxBeginInputLifecycle, "mailbox.begin_input"),
		servedMailboxDecisionScenario(ScenarioMailboxCancelInputLifecycle, "mailbox.cancel_input"),
		servedMailboxDecisionScenario(ScenarioMailboxDecisionCardLifecycle, "mailbox.decide"),
		servedMailboxDecisionScenario(ScenarioMailboxDeferDecisionLifecycle, "mailbox.defer"),
		servedControlScenario(ScenarioTestSetupEntitiesLifecycle, "test.setup_entities", "TestServedParityHarnessTestSetupEntitiesLifecycle"),
		servedControlScenario(ScenarioGeneratedInputFixtureLifecycle, "event.publish", "TestServedParityHarnessGeneratedInputFixtureLifecycle"),
		servedControlScenario(ScenarioDerivedScenarioLifecycle, "event.publish", "TestServedParityHarnessDerivedScenarioLifecycle"),
		servedControlScenario(ScenarioPublicMockApprovalLifecycle, "event.publish", "TestServedParityHarnessPublicMockApprovalLifecycle"),
		servedControlScenario(ScenarioBundleRegisterLifecycle, "bundle.register", "TestServedParityHarnessBundleRegisterLifecycle"),
		servedControlScenario(ScenarioConversationForkLifecycle, "conversation.fork", "TestServedParityHarnessConversationForkLifecycle"),
		servedControlScenario(ScenarioConversationForkChatLifecycle, "conversation.fork_chat", "TestServedParityHarnessConversationForkLifecycle"),
		servedControlScenario(ScenarioConversationForkDeleteLifecycle, "conversation.fork_delete", "TestServedParityHarnessConversationForkLifecycle"),
		servedControlScenario(ScenarioOperatorChannelConnectLifecycle, "channel.connect", "TestServedParityHarnessOperatorChannelLifecycle"),
		servedControlScenario(ScenarioOperatorChannelReconnectLifecycle, "channel.reconnect", "TestServedParityHarnessOperatorChannelLifecycle"),
		servedControlScenario(ScenarioOperatorChannelRebindLifecycle, "channel.rebind", "TestServedParityHarnessOperatorChannelLifecycle"),
		servedControlScenario(ScenarioOperatorChannelConfirmLifecycle, "channel.confirm", "TestServedParityHarnessOperatorChannelLifecycle"),
		servedControlScenario(ScenarioOperatorChannelUnbindLifecycle, "channel.unbind", "TestServedParityHarnessOperatorChannelLifecycle"),
		servedControlScenario(ScenarioOperatorChannelProofRevokeLifecycle, "channel.proof_revoke", "TestServedParityHarnessOperatorChannelLifecycle"),
	}
}

func servedControlScenario(id, apiMethod, testName string) Scenario {
	return Scenario{
		ID:        id,
		APIMethod: apiMethod,
		TestName:  testName,
		Backends:  append([]Backend(nil), RequiredBackends...),
		Postconditions: []Postcondition{
			PostconditionNoNonTerminalDeliveries,
			PostconditionNoPendingPipelineEvents,
			PostconditionNoUnfiredDueTimers,
		},
	}
}

func servedMailboxDecisionScenario(id, apiMethod string) Scenario {
	return Scenario{
		ID:        id,
		APIMethod: apiMethod,
		TestName:  "TestServedParityHarnessMailboxDecisionLifecycle",
		Backends:  append([]Backend(nil), RequiredBackends...),
		Postconditions: []Postcondition{
			PostconditionNoNonTerminalDeliveries,
			PostconditionNoPendingPipelineEvents,
			PostconditionNoUnfiredDueTimers,
		},
	}
}

func ScenarioByID(id string) (Scenario, bool) {
	id = strings.TrimSpace(id)
	for _, scenario := range Scenarios() {
		if scenario.ID == id {
			return scenario, true
		}
	}
	return Scenario{}, false
}

func MustScenario(id string) Scenario {
	scenario, ok := ScenarioByID(id)
	if !ok {
		panic(fmt.Sprintf("unknown served parity scenario %q", id))
	}
	return scenario
}

func Run(t *testing.T, scenario Scenario, run func(*testing.T, Backend)) {
	t.Helper()
	requireValidScenario(t, scenario)
	for _, backend := range scenario.Backends {
		backend := backend
		t.Run(string(backend), func(t *testing.T) {
			run(t, backend)
		})
	}
}

func RunScenarioGroup(t *testing.T, scenarios []Scenario, run func(*testing.T, Backend)) {
	t.Helper()
	if len(scenarios) == 0 {
		t.Fatal("served parity scenario group is empty")
	}
	for _, scenario := range scenarios {
		requireValidScenario(t, scenario)
		if !sameBackends(scenarios[0].Backends, scenario.Backends) {
			t.Fatalf("served parity scenario %s backends = %v, want group backends %v", scenario.ID, scenario.Backends, scenarios[0].Backends)
		}
	}
	for _, backend := range scenarios[0].Backends {
		backend := backend
		t.Run(string(backend), func(t *testing.T) {
			run(t, backend)
		})
	}
}

func requireValidScenario(t *testing.T, scenario Scenario) {
	t.Helper()
	if strings.TrimSpace(scenario.ID) == "" {
		t.Fatal("served parity scenario missing id")
	}
	if len(scenario.Backends) == 0 {
		t.Fatalf("served parity scenario %s missing backends", scenario.ID)
	}
}

func sameBackends(a, b []Backend) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func SettlementPostconditionFailures(scenario Scenario, counts SettlementCounts) []string {
	var failures []string
	for _, postcondition := range scenario.Postconditions {
		switch postcondition {
		case PostconditionNoNonTerminalDeliveries:
			if counts.NonTerminalDeliveries != 0 {
				failures = append(failures, fmt.Sprintf("%s=%d", postcondition, counts.NonTerminalDeliveries))
			}
		case PostconditionNoPendingPipelineEvents:
			if counts.PendingPipelineEvents != 0 {
				failures = append(failures, fmt.Sprintf("%s=%d", postcondition, counts.PendingPipelineEvents))
			}
		case PostconditionNoUnfiredDueTimers:
			if counts.UnfiredDueTimers != 0 {
				failures = append(failures, fmt.Sprintf("%s=%d", postcondition, counts.UnfiredDueTimers))
			}
		default:
			failures = append(failures, fmt.Sprintf("unknown_postcondition=%s", postcondition))
		}
	}
	return failures
}

func AssertSettlementPostconditions(t *testing.T, scenario Scenario, counts SettlementCounts) {
	t.Helper()
	if failures := SettlementPostconditionFailures(scenario, counts); len(failures) > 0 {
		t.Fatalf("served parity scenario %s settlement postconditions failed: %s", scenario.ID, strings.Join(failures, ", "))
	}
}
