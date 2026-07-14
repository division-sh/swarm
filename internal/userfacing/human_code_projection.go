package userfacing

import "strings"

type HumanCodeFamily string

const (
	HumanCodeRunStatus                   HumanCodeFamily = "run_status"
	HumanCodeOperationalState            HumanCodeFamily = "operational_state"
	HumanCodeRunBlockingLayer            HumanCodeFamily = "run_blocking_layer"
	HumanCodeRunBlockingReason           HumanCodeFamily = "run_blocking_reason"
	HumanCodeAgentStatus                 HumanCodeFamily = "agent_status"
	HumanCodeMemorySource                HumanCodeFamily = "memory_source"
	HumanCodeDeliveryStatus              HumanCodeFamily = "delivery_status"
	HumanCodeAgentLifecycleState         HumanCodeFamily = "agent_lifecycle_state"
	HumanCodeAgentLifecycleBlockingLayer HumanCodeFamily = "agent_lifecycle_blocking_layer"
	HumanCodeWatchdogState               HumanCodeFamily = "watchdog_state"
	HumanCodeWatchdogBlockingLayer       HumanCodeFamily = "watchdog_blocking_layer"
	HumanCodeWatchdogAction              HumanCodeFamily = "watchdog_action"
	HumanCodeWatchdogOutcome             HumanCodeFamily = "watchdog_outcome"
	HumanCodeProviderSubjectKind         HumanCodeFamily = "provider_subject_kind"
	HumanCodeProviderSubjectStatus       HumanCodeFamily = "provider_subject_status"
	HumanCodeProviderCapability          HumanCodeFamily = "provider_capability"
	HumanCodeProviderGuarantee           HumanCodeFamily = "provider_guarantee"
	HumanCodeProviderRequirementStatus   HumanCodeFamily = "provider_requirement_status"
	HumanCodeRoutingTopology             HumanCodeFamily = "routing_topology"
)

var humanCodePhrases = map[HumanCodeFamily]map[string]string{
	HumanCodeRunStatus: {
		"running": "running", "paused": "paused", "completed": "completed",
		"failed": "failed", "cancelled": "cancelled", "forked": "forked",
	},
	HumanCodeOperationalState: {
		"running": "running", "stalled": "stalled", "paused": "paused",
		"completed": "completed", "failed": "failed", "cancelled": "cancelled", "forked": "forked",
	},
	HumanCodeRunBlockingLayer: {
		"scoring_terminal_outcome": "scoring outcome",
		"delivery_lifecycle":       "delivery lifecycle",
	},
	HumanCodeRunBlockingReason: {
		"terminal_scoring_outcome_missing": "waiting for a terminal scoring outcome",
		"no_active_deliveries":             "no active deliveries",
	},
	HumanCodeAgentStatus: {
		"idle": "idle", "running": "running", "paused": "paused",
		"failed": "failed", "terminated": "terminated",
	},
	HumanCodeMemorySource: {
		"authored": "authored", "platform_default": "platform default",
	},
	HumanCodeDeliveryStatus: {
		"pending": "pending", "in_progress": "in progress", "delivered": "delivered",
		"failed": "failed", "dead_letter": "dead letter",
	},
	HumanCodeAgentLifecycleState: {
		"queued": "queued", "launching": "launching", "active": "active",
		"retrying": "retrying", "exhausted": "exhausted",
	},
	HumanCodeAgentLifecycleBlockingLayer: {
		"delivery_queue":    "delivery queue",
		"session_launch":    "session launch",
		"session_execution": "session execution",
		"delivery_retry":    "delivery retry",
		"delivery_terminal": "delivery terminal",
	},
	HumanCodeWatchdogState: {
		"healthy_long_running": "healthy, long-running",
		"no_output":            "no output",
	},
	HumanCodeWatchdogBlockingLayer: {
		"session_execution": "session execution",
	},
	HumanCodeWatchdogAction: {
		"turn_long_running": "turn running for a long time",
		"session_no_output": "session produced no output",
	},
	HumanCodeWatchdogOutcome: {
		"observed": "observed", "warning_emitted": "warning emitted",
	},
	HumanCodeProviderSubjectKind: {
		"provider_trigger":   "provider trigger pack",
		"provider_connector": "provider connector",
	},
	HumanCodeProviderSubjectStatus: {
		"READY": "READY", "NOT_READY": "NOT READY", "AVAILABLE": "AVAILABLE",
	},
	HumanCodeProviderCapability: {
		"receive_https_route":       "receive HTTPS route",
		"verify_secret":             "verify named secret",
		"emit_event":                "emit named event",
		"persist_dedupe_markers":    "persist dedupe markers",
		"call_provider_action":      "call provider action",
		"lower_through_activity":    "lower through platform.activity_requested",
		"journal_activity_attempts": "journal non-idempotent attempts in activity_attempts",
	},
	HumanCodeProviderGuarantee: {
		"emit_undeclared_events":                   "emit undeclared events",
		"run_code_before_admission":                "run code before admission",
		"touch_unbound_resources":                  "touch unbound resources",
		"bypass_activity_attempts":                 "bypass activity_attempts",
		"retry_non_idempotent_write_automatically": "retry non_idempotent_write automatically",
		"expose_credential_values":                 "expose credential values",
	},
	HumanCodeProviderRequirementStatus: {
		"BOUND": "BOUND", "UNBOUND": "UNBOUND", "CONNECTED": "CONNECTED",
		"UNCONNECTED": "UNCONNECTED", "PENDING_CONSENT": "PENDING CONSENT",
		"REFRESH_FAILED": "REFRESH FAILED", "SCOPE_INSUFFICIENT": "SCOPE INSUFFICIENT",
		"NOT_IMPORTED": "NOT IMPORTED", "UNKNOWN": "UNKNOWN",
	},
}

func ProjectHumanCode(family HumanCodeFamily, raw string) string {
	if phrase, ok := humanCodePhrases[family][strings.TrimSpace(raw)]; ok {
		return phrase
	}
	return raw
}

func HumanCodePhrases() map[HumanCodeFamily]map[string]string {
	out := make(map[HumanCodeFamily]map[string]string, len(humanCodePhrases))
	for family, phrases := range humanCodePhrases {
		out[family] = make(map[string]string, len(phrases))
		for code, phrase := range phrases {
			out[family][code] = phrase
		}
	}
	return out
}
