package main

import "strings"

type cliIdentifierFamily string

const (
	cliIdentifierFamilyNone         cliIdentifierFamily = "none"
	cliIdentifierFamilyAgent        cliIdentifierFamily = "agent"
	cliIdentifierFamilyBundle       cliIdentifierFamily = "bundle"
	cliIdentifierFamilyRun          cliIdentifierFamily = "run"
	cliIdentifierFamilyEntity       cliIdentifierFamily = "entity"
	cliIdentifierFamilyEvent        cliIdentifierFamily = "event"
	cliIdentifierFamilySession      cliIdentifierFamily = "session"
	cliIdentifierFamilyFork         cliIdentifierFamily = "fork"
	cliIdentifierFamilyMailbox      cliIdentifierFamily = "mailbox"
	cliIdentifierFamilyFlowInstance cliIdentifierFamily = "flow_instance"
	cliIdentifierFamilyContext      cliIdentifierFamily = "context"
	cliIdentifierFamilySubscriber   cliIdentifierFamily = "subscriber"
)

type cliIdentifierInputMode string

const (
	cliIdentifierModeResolverBounded cliIdentifierInputMode = "resolver_bounded"
	cliIdentifierModeResolverScoped  cliIdentifierInputMode = "resolver_scoped"
	cliIdentifierModeFullOnly        cliIdentifierInputMode = "full_only"
	cliIdentifierModeDifferent       cliIdentifierInputMode = "different_concept"
	cliIdentifierModeSplit           cliIdentifierInputMode = "split"
)

type cliIdentifierFamilyPolicy struct {
	Family          cliIdentifierFamily
	CandidateSource string
	ScopeRule       string
	Normalization   string
}

type cliIdentifierInputRegistration struct {
	Command         string
	Selector        string
	Family          cliIdentifierFamily
	Mode            cliIdentifierInputMode
	CandidateSource string
	ScopeRule       string
	Normalization   string
	Safety          string
}

var cliIdentifierFamilyRegistry = map[cliIdentifierFamily]cliIdentifierFamilyPolicy{
	cliIdentifierFamilyAgent: {
		Family:          cliIdentifierFamilyAgent,
		CandidateSource: "agent.list",
		ScopeRule:       "global authored-slug set; duplicate live slugs remain unsupported",
		Normalization:   "trim only; case-sensitive",
	},
	cliIdentifierFamilyBundle: {
		Family:          cliIdentifierFamilyBundle,
		CandidateSource: "bundle.list",
		ScopeRule:       "bounded registered bundle catalog",
		Normalization:   "canonical full-string or bare digest-hex prefix; lowercase hex folding",
	},
	cliIdentifierFamilyRun: {
		Family:          cliIdentifierFamilyRun,
		CandidateSource: "run.list",
		ScopeRule:       "unbounded; full ID unless a narrower promoted scope exists",
		Normalization:   "trim only; case-sensitive",
	},
	cliIdentifierFamilyEntity: {
		Family:          cliIdentifierFamilyEntity,
		CandidateSource: "entity.list",
		ScopeRule:       "full run ID required for prefix resolution",
		Normalization:   "trim only; case-sensitive",
	},
	cliIdentifierFamilyEvent: {
		Family:          cliIdentifierFamilyEvent,
		CandidateSource: "event.list",
		ScopeRule:       "unbounded; full ID unless a narrower promoted scope exists",
		Normalization:   "trim only; case-sensitive",
	},
	cliIdentifierFamilySession: {
		Family:          cliIdentifierFamilySession,
		CandidateSource: "conversation.list",
		ScopeRule:       "global boundedness not promoted; full ID",
		Normalization:   "trim only; case-sensitive",
	},
	cliIdentifierFamilyFork: {
		Family:          cliIdentifierFamilyFork,
		CandidateSource: "conversation.fork_list",
		ScopeRule:       "boundedness and required scope not promoted",
		Normalization:   "trim only; case-sensitive",
	},
	cliIdentifierFamilyMailbox: {
		Family:          cliIdentifierFamilyMailbox,
		CandidateSource: "mailbox.list",
		ScopeRule:       "boundedness and required scope not promoted",
		Normalization:   "trim only; case-sensitive",
	},
	cliIdentifierFamilyFlowInstance: {
		Family:          cliIdentifierFamilyFlowInstance,
		CandidateSource: "split: no promoted flow-instance list owner",
		ScopeRule:       "full exact value",
		Normalization:   "trim surrounding slashes only where the command contract already does so",
	},
	cliIdentifierFamilyContext: {
		Family:          cliIdentifierFamilyContext,
		CandidateSource: "local context registry",
		ScopeRule:       "bounded local descriptor set",
		Normalization:   "trim only; case-sensitive",
	},
	cliIdentifierFamilySubscriber: {
		Family:          cliIdentifierFamilySubscriber,
		CandidateSource: "polymorphic agent/node identity",
		ScopeRule:       "full exact value unless subscriber type supplies a promoted family owner",
		Normalization:   "trim only; case-sensitive",
	},
}

// This is the living public-input ledger. A row describes input semantics, not
// whether the command happens to validate the value locally today.
var cliIdentifierInputRegistry = []cliIdentifierInputRegistration{
	{Command: "swarm agent view", Selector: "arg:agent-id", Family: cliIdentifierFamilyAgent, Mode: cliIdentifierModeResolverBounded},
	{Command: "swarm agent diagnose", Selector: "arg:agent-id", Family: cliIdentifierFamilyAgent, Mode: cliIdentifierModeResolverBounded},
	{Command: "swarm agent deliveries", Selector: "arg:agent-id", Family: cliIdentifierFamilyAgent, Mode: cliIdentifierModeResolverBounded},
	{Command: "swarm agent restart", Selector: "arg:agent-id", Family: cliIdentifierFamilyAgent, Mode: cliIdentifierModeFullOnly, Safety: "mutating"},
	{Command: "swarm agent replay", Selector: "arg:agent-id", Family: cliIdentifierFamilyAgent, Mode: cliIdentifierModeFullOnly, Safety: "mutating"},
	{Command: "swarm agent replay-backlog", Selector: "arg:agent-id", Family: cliIdentifierFamilyAgent, Mode: cliIdentifierModeFullOnly, Safety: "mutating"},
	{Command: "swarm agent directive", Selector: "arg:agent-id", Family: cliIdentifierFamilyAgent, Mode: cliIdentifierModeFullOnly, Safety: "mutating"},
	{Command: "swarm conversation list", Selector: "flag:agent-id", Family: cliIdentifierFamilyAgent, Mode: cliIdentifierModeFullOnly},
	{Command: "swarm event replay", Selector: "flag:subscriber", Family: cliIdentifierFamilyAgent, Mode: cliIdentifierModeFullOnly, Safety: "mutating"},

	{Command: "swarm bundle show", Selector: "arg:bundle-hash", Family: cliIdentifierFamilyBundle, Mode: cliIdentifierModeResolverBounded},
	{Command: "swarm bundle agents", Selector: "arg:bundle-hash", Family: cliIdentifierFamilyBundle, Mode: cliIdentifierModeResolverBounded},
	{Command: "swarm bundle delete", Selector: "arg:bundle-hash", Family: cliIdentifierFamilyBundle, Mode: cliIdentifierModeFullOnly, Safety: "mutating"},
	{Command: "swarm serve", Selector: "flag:bundle-hash", Family: cliIdentifierFamilyBundle, Mode: cliIdentifierModeFullOnly, Safety: "boot"},
	{Command: "swarm run start", Selector: "flag:bundle-hash", Family: cliIdentifierFamilyBundle, Mode: cliIdentifierModeFullOnly, Safety: "creation"},
	{Command: "swarm run start", Selector: "flag:bundle-fingerprint", Family: cliIdentifierFamilyBundle, Mode: cliIdentifierModeFullOnly, Safety: "creation"},
	{Command: "swarm run fork", Selector: "flag:bundle-hash", Family: cliIdentifierFamilyBundle, Mode: cliIdentifierModeFullOnly, Safety: "mutating"},
	{Command: "swarm event publish", Selector: "flag:bundle-hash", Family: cliIdentifierFamilyBundle, Mode: cliIdentifierModeFullOnly, Safety: "mutating"},
	{Command: "swarm event publish", Selector: "flag:bundle-fingerprint", Family: cliIdentifierFamilyBundle, Mode: cliIdentifierModeFullOnly, Safety: "mutating"},

	{Command: "swarm run status", Selector: "arg:run-id", Family: cliIdentifierFamilyRun, Mode: cliIdentifierModeFullOnly},
	{Command: "swarm run trace", Selector: "arg:run-id", Family: cliIdentifierFamilyRun, Mode: cliIdentifierModeFullOnly},
	{Command: "swarm run start", Selector: "flag:reattach", Family: cliIdentifierFamilyRun, Mode: cliIdentifierModeFullOnly},
	{Command: "swarm run fork", Selector: "arg:source-run-id", Family: cliIdentifierFamilyRun, Mode: cliIdentifierModeFullOnly, Safety: "mutating"},
	{Command: "swarm control pause", Selector: "arg:run-id", Family: cliIdentifierFamilyRun, Mode: cliIdentifierModeFullOnly, Safety: "mutating"},
	{Command: "swarm control continue", Selector: "arg:run-id", Family: cliIdentifierFamilyRun, Mode: cliIdentifierModeFullOnly, Safety: "mutating"},
	{Command: "swarm control stop", Selector: "arg:run-id", Family: cliIdentifierFamilyRun, Mode: cliIdentifierModeFullOnly, Safety: "mutating"},
	{Command: "swarm agent deliveries", Selector: "flag:run-id", Family: cliIdentifierFamilyRun, Mode: cliIdentifierModeFullOnly},
	{Command: "swarm agent directive", Selector: "flag:run-id", Family: cliIdentifierFamilyRun, Mode: cliIdentifierModeFullOnly, Safety: "mutating"},
	{Command: "swarm conversation list", Selector: "flag:run-id", Family: cliIdentifierFamilyRun, Mode: cliIdentifierModeFullOnly},
	{Command: "swarm entity list", Selector: "flag:run-id", Family: cliIdentifierFamilyRun, Mode: cliIdentifierModeFullOnly},
	{Command: "swarm entity view", Selector: "flag:run-id", Family: cliIdentifierFamilyRun, Mode: cliIdentifierModeFullOnly},
	{Command: "swarm entity aggregate", Selector: "flag:run-id", Family: cliIdentifierFamilyRun, Mode: cliIdentifierModeFullOnly},
	{Command: "swarm event list", Selector: "flag:run-id", Family: cliIdentifierFamilyRun, Mode: cliIdentifierModeFullOnly},
	{Command: "swarm event follow", Selector: "flag:run-id", Family: cliIdentifierFamilyRun, Mode: cliIdentifierModeFullOnly},
	{Command: "swarm event publish", Selector: "flag:run-id", Family: cliIdentifierFamilyRun, Mode: cliIdentifierModeFullOnly, Safety: "mutating"},
	{Command: "swarm mailbox list", Selector: "flag:run-id", Family: cliIdentifierFamilyRun, Mode: cliIdentifierModeFullOnly},
	{Command: "swarm logs", Selector: "flag:run-id", Family: cliIdentifierFamilyRun, Mode: cliIdentifierModeFullOnly},
	{Command: "swarm run start", Selector: "flag:run-id", Family: cliIdentifierFamilyNone, Mode: cliIdentifierModeDifferent, ScopeRule: "caller-minted new-run ID"},

	{Command: "swarm entity view", Selector: "arg:entity-id", Family: cliIdentifierFamilyEntity, Mode: cliIdentifierModeResolverScoped, ScopeRule: "full --run-id required"},
	{Command: "swarm event list", Selector: "flag:entity-id", Family: cliIdentifierFamilyEntity, Mode: cliIdentifierModeFullOnly},
	{Command: "swarm event follow", Selector: "flag:entity-id", Family: cliIdentifierFamilyEntity, Mode: cliIdentifierModeFullOnly},
	{Command: "swarm mailbox list", Selector: "flag:entity-id", Family: cliIdentifierFamilyEntity, Mode: cliIdentifierModeFullOnly},
	{Command: "swarm logs", Selector: "flag:entity-id", Family: cliIdentifierFamilyEntity, Mode: cliIdentifierModeFullOnly},
	{Command: "swarm run trace", Selector: "flag:entity-id", Family: cliIdentifierFamilyEntity, Mode: cliIdentifierModeFullOnly},
	{Command: "swarm event publish", Selector: "flag:target-entity-id", Family: cliIdentifierFamilyEntity, Mode: cliIdentifierModeFullOnly, Safety: "mutating"},

	{Command: "swarm event view", Selector: "arg:event-id", Family: cliIdentifierFamilyEvent, Mode: cliIdentifierModeFullOnly},
	{Command: "swarm event replay", Selector: "arg:event-id", Family: cliIdentifierFamilyEvent, Mode: cliIdentifierModeFullOnly, Safety: "mutating"},
	{Command: "swarm agent replay", Selector: "flag:event-id", Family: cliIdentifierFamilyEvent, Mode: cliIdentifierModeFullOnly, Safety: "mutating"},
	{Command: "swarm run fork", Selector: "flag:at-event", Family: cliIdentifierFamilyEvent, Mode: cliIdentifierModeFullOnly, Safety: "mutating"},
	{Command: "swarm forkchat new", Selector: "flag:event-id", Family: cliIdentifierFamilyEvent, Mode: cliIdentifierModeFullOnly, Safety: "mutating"},
	{Command: "swarm event publish", Selector: "flag:source-event-id", Family: cliIdentifierFamilyEvent, Mode: cliIdentifierModeFullOnly, Safety: "mutating"},

	{Command: "swarm conversation view", Selector: "arg:session-id", Family: cliIdentifierFamilySession, Mode: cliIdentifierModeFullOnly},
	{Command: "swarm conversation turn", Selector: "arg:session-id", Family: cliIdentifierFamilySession, Mode: cliIdentifierModeFullOnly},
	{Command: "swarm forkchat new", Selector: "arg:source-session-id", Family: cliIdentifierFamilySession, Mode: cliIdentifierModeFullOnly, Safety: "mutating"},
	{Command: "swarm forkchat list", Selector: "flag:source-session-id", Family: cliIdentifierFamilySession, Mode: cliIdentifierModeFullOnly},
	{Command: "swarm logs", Selector: "flag:session-id", Family: cliIdentifierFamilySession, Mode: cliIdentifierModeFullOnly},

	{Command: "swarm forkchat view", Selector: "arg:fork-id", Family: cliIdentifierFamilyFork, Mode: cliIdentifierModeSplit},
	{Command: "swarm forkchat resume", Selector: "arg:fork-id", Family: cliIdentifierFamilyFork, Mode: cliIdentifierModeFullOnly, Safety: "mutating"},
	{Command: "swarm forkchat delete", Selector: "arg:fork-id", Family: cliIdentifierFamilyFork, Mode: cliIdentifierModeFullOnly, Safety: "mutating"},

	{Command: "swarm mailbox view", Selector: "arg:mailbox-item-id", Family: cliIdentifierFamilyMailbox, Mode: cliIdentifierModeSplit},
	{Command: "swarm mailbox approve", Selector: "arg:mailbox-item-id", Family: cliIdentifierFamilyMailbox, Mode: cliIdentifierModeFullOnly, Safety: "mutating"},
	{Command: "swarm mailbox reject", Selector: "arg:mailbox-item-id", Family: cliIdentifierFamilyMailbox, Mode: cliIdentifierModeFullOnly, Safety: "mutating"},
	{Command: "swarm mailbox defer", Selector: "arg:mailbox-item-id", Family: cliIdentifierFamilyMailbox, Mode: cliIdentifierModeFullOnly, Safety: "mutating"},

	{Command: "swarm entity list", Selector: "flag:flow", Family: cliIdentifierFamilyFlowInstance, Mode: cliIdentifierModeSplit},
	{Command: "swarm event publish", Selector: "flag:target-flow-instance", Family: cliIdentifierFamilyFlowInstance, Mode: cliIdentifierModeFullOnly, Safety: "mutating"},
	{Command: "swarm agent list", Selector: "flag:flow", Family: cliIdentifierFamilyNone, Mode: cliIdentifierModeDifferent, ScopeRule: "authored canonical flow path"},

	{Command: "swarm <api-backed>", Selector: "flag:context", Family: cliIdentifierFamilyContext, Mode: cliIdentifierModeFullOnly},
	{Command: "swarm serve", Selector: "flag:context", Family: cliIdentifierFamilyNone, Mode: cliIdentifierModeDifferent, ScopeRule: "caller-authored context registration name"},

	{Command: "swarm event list", Selector: "flag:subscriber-id", Family: cliIdentifierFamilySubscriber, Mode: cliIdentifierModeFullOnly},
	{Command: "swarm event follow", Selector: "flag:subscriber-id", Family: cliIdentifierFamilySubscriber, Mode: cliIdentifierModeFullOnly},
	{Command: "swarm run trace", Selector: "flag:subscriber-id", Family: cliIdentifierFamilySubscriber, Mode: cliIdentifierModeFullOnly},

	{Command: "swarm <mutating>", Selector: "flag:idempotency-key", Family: cliIdentifierFamilyNone, Mode: cliIdentifierModeDifferent, ScopeRule: "caller-authored retry key"},
	{Command: "swarm connections <key>", Selector: "arg:key", Family: cliIdentifierFamilyNone, Mode: cliIdentifierModeDifferent, ScopeRule: "authored stable connection key"},
	{Command: "swarm secrets <key>", Selector: "arg:key", Family: cliIdentifierFamilyNone, Mode: cliIdentifierModeDifferent, ScopeRule: "authored stable secret key"},
	{Command: "swarm connections connect", Selector: "flag:client-id", Family: cliIdentifierFamilyNone, Mode: cliIdentifierModeDifferent, ScopeRule: "external OAuth client identifier"},
	{Command: "swarm connections connect", Selector: "flag:installation-id", Family: cliIdentifierFamilyNone, Mode: cliIdentifierModeDifferent, ScopeRule: "external provider installation identifier"},
	{Command: "swarm event publish", Selector: "flag:emitter", Family: cliIdentifierFamilyNone, Mode: cliIdentifierModeDifferent, ScopeRule: "producer label"},
}

func cliIdentifierRegistryKey(command, selector string) string {
	return strings.TrimSpace(command) + "\x00" + strings.TrimSpace(selector)
}

func cliIdentifierRegistration(command, selector string) (cliIdentifierInputRegistration, bool) {
	key := cliIdentifierRegistryKey(command, selector)
	for _, row := range cliIdentifierInputRegistry {
		if cliIdentifierRegistryKey(row.Command, row.Selector) == key {
			return row, true
		}
	}
	return cliIdentifierInputRegistration{}, false
}

func cliIdentifierFamilyDisplayEligible(family cliIdentifierFamily) bool {
	found := false
	for _, row := range cliIdentifierInputRegistry {
		if row.Family != family {
			continue
		}
		found = true
		switch row.Mode {
		case cliIdentifierModeResolverBounded, cliIdentifierModeResolverScoped:
		default:
			return false
		}
	}
	return found
}
