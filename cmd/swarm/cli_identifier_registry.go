package main

import (
	"fmt"
	"strings"
)

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

type cliIdentifierSafetyMode string

const (
	cliIdentifierSafetyNone     cliIdentifierSafetyMode = ""
	cliIdentifierSafetyMutating cliIdentifierSafetyMode = "mutating"
	cliIdentifierSafetyCreation cliIdentifierSafetyMode = "creation"
	cliIdentifierSafetyBoot     cliIdentifierSafetyMode = "boot"
)

type cliIdentifierCandidateSource string

const (
	cliIdentifierSourceAgentList    cliIdentifierCandidateSource = "/v1/rpc agent.list"
	cliIdentifierSourceBundleList   cliIdentifierCandidateSource = "/v1/rpc bundle.list"
	cliIdentifierSourceRunList      cliIdentifierCandidateSource = "/v1/rpc run.list"
	cliIdentifierSourceEntityList   cliIdentifierCandidateSource = "/v1/rpc entity.list"
	cliIdentifierSourceEventList    cliIdentifierCandidateSource = "/v1/rpc event.list"
	cliIdentifierSourceConversation cliIdentifierCandidateSource = "/v1/rpc conversation.list"
	cliIdentifierSourceForkList     cliIdentifierCandidateSource = "/v1/rpc conversation.fork_list"
	cliIdentifierSourceMailboxList  cliIdentifierCandidateSource = "/v1/rpc mailbox.list"
	cliIdentifierSourceUnpromoted   cliIdentifierCandidateSource = "unpromoted"
	cliIdentifierSourceLocalContext cliIdentifierCandidateSource = "local_context_registry"
	cliIdentifierSourcePolymorphic  cliIdentifierCandidateSource = "polymorphic_subscriber_identity"
)

type cliIdentifierScopeMode string

const (
	cliIdentifierScopeGlobalBounded   cliIdentifierScopeMode = "global_bounded"
	cliIdentifierScopeBoundedCatalog  cliIdentifierScopeMode = "bounded_catalog"
	cliIdentifierScopeUnboundedFull   cliIdentifierScopeMode = "unbounded_full_only"
	cliIdentifierScopeFullRunRequired cliIdentifierScopeMode = "full_run_required"
	cliIdentifierScopeUnpromoted      cliIdentifierScopeMode = "unpromoted_full_only"
	cliIdentifierScopeLocalBounded    cliIdentifierScopeMode = "local_bounded"
	cliIdentifierScopePolymorphicFull cliIdentifierScopeMode = "polymorphic_full_only"
)

type cliIdentifierNormalizationMode string

const (
	cliIdentifierNormalizeCaseSensitive cliIdentifierNormalizationMode = "trim_case_sensitive"
	cliIdentifierNormalizeBundleDigest  cliIdentifierNormalizationMode = "bundle_digest_hex_case_fold"
	cliIdentifierNormalizeFlowPath      cliIdentifierNormalizationMode = "existing_flow_path"
)

type cliIdentifierDisplayProjection string

const cliIdentifierDisplayFull cliIdentifierDisplayProjection = "full"

type cliIdentifierFamilyPolicy struct {
	Family            cliIdentifierFamily
	CandidateSource   cliIdentifierCandidateSource
	ScopeMode         cliIdentifierScopeMode
	ScopeRule         string
	NormalizationMode cliIdentifierNormalizationMode
	NormalizationRule string
	DisplayProjection cliIdentifierDisplayProjection
}

type cliIdentifierInputRegistration struct {
	Command   string
	Selector  string
	Family    cliIdentifierFamily
	Mode      cliIdentifierInputMode
	ScopeRule string
	Safety    cliIdentifierSafetyMode
}

var cliIdentifierFamilyRegistry = map[cliIdentifierFamily]cliIdentifierFamilyPolicy{
	cliIdentifierFamilyAgent: {
		Family:            cliIdentifierFamilyAgent,
		CandidateSource:   cliIdentifierSourceAgentList,
		ScopeMode:         cliIdentifierScopeGlobalBounded,
		ScopeRule:         "global authored-slug set; duplicate live slugs remain unsupported",
		NormalizationMode: cliIdentifierNormalizeCaseSensitive,
		NormalizationRule: "trim only; case-sensitive",
		DisplayProjection: cliIdentifierDisplayFull,
	},
	cliIdentifierFamilyBundle: {
		Family:            cliIdentifierFamilyBundle,
		CandidateSource:   cliIdentifierSourceBundleList,
		ScopeMode:         cliIdentifierScopeBoundedCatalog,
		ScopeRule:         "bounded registered bundle catalog",
		NormalizationMode: cliIdentifierNormalizeBundleDigest,
		NormalizationRule: "canonical full-string or bare digest-hex prefix; lowercase hex folding",
		DisplayProjection: cliIdentifierDisplayFull,
	},
	cliIdentifierFamilyRun: {
		Family:            cliIdentifierFamilyRun,
		CandidateSource:   cliIdentifierSourceRunList,
		ScopeMode:         cliIdentifierScopeUnboundedFull,
		ScopeRule:         "unbounded; full ID unless a narrower promoted scope exists",
		NormalizationMode: cliIdentifierNormalizeCaseSensitive,
		NormalizationRule: "trim only; case-sensitive",
		DisplayProjection: cliIdentifierDisplayFull,
	},
	cliIdentifierFamilyEntity: {
		Family:            cliIdentifierFamilyEntity,
		CandidateSource:   cliIdentifierSourceEntityList,
		ScopeMode:         cliIdentifierScopeFullRunRequired,
		ScopeRule:         "full run ID required for prefix resolution",
		NormalizationMode: cliIdentifierNormalizeCaseSensitive,
		NormalizationRule: "trim only; case-sensitive",
		DisplayProjection: cliIdentifierDisplayFull,
	},
	cliIdentifierFamilyEvent: {
		Family:            cliIdentifierFamilyEvent,
		CandidateSource:   cliIdentifierSourceEventList,
		ScopeMode:         cliIdentifierScopeUnboundedFull,
		ScopeRule:         "unbounded; full ID unless a narrower promoted scope exists",
		NormalizationMode: cliIdentifierNormalizeCaseSensitive,
		NormalizationRule: "trim only; case-sensitive",
		DisplayProjection: cliIdentifierDisplayFull,
	},
	cliIdentifierFamilySession: {
		Family:            cliIdentifierFamilySession,
		CandidateSource:   cliIdentifierSourceConversation,
		ScopeMode:         cliIdentifierScopeUnpromoted,
		ScopeRule:         "global boundedness not promoted; full ID",
		NormalizationMode: cliIdentifierNormalizeCaseSensitive,
		NormalizationRule: "trim only; case-sensitive",
		DisplayProjection: cliIdentifierDisplayFull,
	},
	cliIdentifierFamilyFork: {
		Family:            cliIdentifierFamilyFork,
		CandidateSource:   cliIdentifierSourceForkList,
		ScopeMode:         cliIdentifierScopeUnpromoted,
		ScopeRule:         "boundedness and required scope not promoted",
		NormalizationMode: cliIdentifierNormalizeCaseSensitive,
		NormalizationRule: "trim only; case-sensitive",
		DisplayProjection: cliIdentifierDisplayFull,
	},
	cliIdentifierFamilyMailbox: {
		Family:            cliIdentifierFamilyMailbox,
		CandidateSource:   cliIdentifierSourceMailboxList,
		ScopeMode:         cliIdentifierScopeUnpromoted,
		ScopeRule:         "boundedness and required scope not promoted",
		NormalizationMode: cliIdentifierNormalizeCaseSensitive,
		NormalizationRule: "trim only; case-sensitive",
		DisplayProjection: cliIdentifierDisplayFull,
	},
	cliIdentifierFamilyFlowInstance: {
		Family:            cliIdentifierFamilyFlowInstance,
		CandidateSource:   cliIdentifierSourceUnpromoted,
		ScopeMode:         cliIdentifierScopeUnpromoted,
		ScopeRule:         "full exact value",
		NormalizationMode: cliIdentifierNormalizeFlowPath,
		NormalizationRule: "trim surrounding slashes only where the command contract already does so",
		DisplayProjection: cliIdentifierDisplayFull,
	},
	cliIdentifierFamilyContext: {
		Family:            cliIdentifierFamilyContext,
		CandidateSource:   cliIdentifierSourceLocalContext,
		ScopeMode:         cliIdentifierScopeLocalBounded,
		ScopeRule:         "bounded local descriptor set",
		NormalizationMode: cliIdentifierNormalizeCaseSensitive,
		NormalizationRule: "trim only; case-sensitive",
		DisplayProjection: cliIdentifierDisplayFull,
	},
	cliIdentifierFamilySubscriber: {
		Family:            cliIdentifierFamilySubscriber,
		CandidateSource:   cliIdentifierSourcePolymorphic,
		ScopeMode:         cliIdentifierScopePolymorphicFull,
		ScopeRule:         "full exact value unless subscriber type supplies a promoted family owner",
		NormalizationMode: cliIdentifierNormalizeCaseSensitive,
		NormalizationRule: "trim only; case-sensitive",
		DisplayProjection: cliIdentifierDisplayFull,
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

func cliIdentifierFamilyPolicyFor(family cliIdentifierFamily) (cliIdentifierFamilyPolicy, bool) {
	policy, ok := cliIdentifierFamilyRegistry[family]
	return policy, ok
}

func expectedCLIIdentifierFamilyPolicyModes(family cliIdentifierFamily) (cliIdentifierCandidateSource, cliIdentifierScopeMode, cliIdentifierNormalizationMode, bool) {
	switch family {
	case cliIdentifierFamilyAgent:
		return cliIdentifierSourceAgentList, cliIdentifierScopeGlobalBounded, cliIdentifierNormalizeCaseSensitive, true
	case cliIdentifierFamilyBundle:
		return cliIdentifierSourceBundleList, cliIdentifierScopeBoundedCatalog, cliIdentifierNormalizeBundleDigest, true
	case cliIdentifierFamilyRun:
		return cliIdentifierSourceRunList, cliIdentifierScopeUnboundedFull, cliIdentifierNormalizeCaseSensitive, true
	case cliIdentifierFamilyEntity:
		return cliIdentifierSourceEntityList, cliIdentifierScopeFullRunRequired, cliIdentifierNormalizeCaseSensitive, true
	case cliIdentifierFamilyEvent:
		return cliIdentifierSourceEventList, cliIdentifierScopeUnboundedFull, cliIdentifierNormalizeCaseSensitive, true
	case cliIdentifierFamilySession:
		return cliIdentifierSourceConversation, cliIdentifierScopeUnpromoted, cliIdentifierNormalizeCaseSensitive, true
	case cliIdentifierFamilyFork:
		return cliIdentifierSourceForkList, cliIdentifierScopeUnpromoted, cliIdentifierNormalizeCaseSensitive, true
	case cliIdentifierFamilyMailbox:
		return cliIdentifierSourceMailboxList, cliIdentifierScopeUnpromoted, cliIdentifierNormalizeCaseSensitive, true
	case cliIdentifierFamilyFlowInstance:
		return cliIdentifierSourceUnpromoted, cliIdentifierScopeUnpromoted, cliIdentifierNormalizeFlowPath, true
	case cliIdentifierFamilyContext:
		return cliIdentifierSourceLocalContext, cliIdentifierScopeLocalBounded, cliIdentifierNormalizeCaseSensitive, true
	case cliIdentifierFamilySubscriber:
		return cliIdentifierSourcePolymorphic, cliIdentifierScopePolymorphicFull, cliIdentifierNormalizeCaseSensitive, true
	default:
		return "", "", "", false
	}
}

func validateCLIIdentifierFamilyPolicy(family cliIdentifierFamily, policy cliIdentifierFamilyPolicy) error {
	expectedSource, expectedScope, expectedNormalization, ok := expectedCLIIdentifierFamilyPolicyModes(family)
	if !ok {
		return fmt.Errorf("identifier family policy %q has unsupported family", family)
	}
	if policy.Family != family {
		return fmt.Errorf("identifier family policy %q has mismatched family %q", family, policy.Family)
	}
	if strings.TrimSpace(policy.ScopeRule) == "" || strings.TrimSpace(policy.NormalizationRule) == "" {
		return fmt.Errorf("identifier family policy %q is incomplete", family)
	}
	if policy.CandidateSource != expectedSource || policy.ScopeMode != expectedScope || policy.NormalizationMode != expectedNormalization {
		return fmt.Errorf(
			"identifier family policy %q has unsupported source/scope/normalization combination %q/%q/%q",
			family,
			policy.CandidateSource,
			policy.ScopeMode,
			policy.NormalizationMode,
		)
	}
	switch policy.DisplayProjection {
	case cliIdentifierDisplayFull:
	default:
		return fmt.Errorf("identifier family policy %q has unsupported display projection %q", family, policy.DisplayProjection)
	}
	return nil
}

func validateCLIIdentifierRegistry() error {
	seen := map[string]struct{}{}
	for family, policy := range cliIdentifierFamilyRegistry {
		if err := validateCLIIdentifierFamilyPolicy(family, policy); err != nil {
			return err
		}
	}
	for _, row := range cliIdentifierInputRegistry {
		key := cliIdentifierRegistryKey(row.Command, row.Selector)
		if _, ok := seen[key]; ok {
			return fmt.Errorf("duplicate identifier input row %s %s", row.Command, row.Selector)
		}
		seen[key] = struct{}{}
		if row.Family == cliIdentifierFamilyNone {
			if row.Mode != cliIdentifierModeDifferent {
				return fmt.Errorf("identifier input %s %s has no family but mode %s", row.Command, row.Selector, row.Mode)
			}
			continue
		}
		policy, ok := cliIdentifierFamilyPolicyFor(row.Family)
		if !ok {
			return fmt.Errorf("identifier input %s %s references unregistered family %s", row.Command, row.Selector, row.Family)
		}
		switch row.Safety {
		case cliIdentifierSafetyNone, cliIdentifierSafetyMutating, cliIdentifierSafetyCreation, cliIdentifierSafetyBoot:
		default:
			return fmt.Errorf("identifier input %s %s has unknown safety mode %s", row.Command, row.Selector, row.Safety)
		}
		if row.Safety != cliIdentifierSafetyNone && row.Mode != cliIdentifierModeFullOnly {
			return fmt.Errorf("safe identifier input %s %s must be full_only, got %s", row.Command, row.Selector, row.Mode)
		}
		switch row.Mode {
		case cliIdentifierModeResolverBounded:
			if policy.ScopeMode != cliIdentifierScopeGlobalBounded && policy.ScopeMode != cliIdentifierScopeBoundedCatalog {
				return fmt.Errorf("bounded resolver input %s %s uses family scope %s", row.Command, row.Selector, policy.ScopeMode)
			}
		case cliIdentifierModeResolverScoped:
			if policy.ScopeMode != cliIdentifierScopeFullRunRequired {
				return fmt.Errorf("scoped resolver input %s %s uses family scope %s", row.Command, row.Selector, policy.ScopeMode)
			}
		case cliIdentifierModeFullOnly, cliIdentifierModeSplit:
		case cliIdentifierModeDifferent:
			return fmt.Errorf("identifier input %s %s uses different_concept with family %s", row.Command, row.Selector, row.Family)
		default:
			return fmt.Errorf("identifier input %s %s has unknown mode %s", row.Command, row.Selector, row.Mode)
		}
	}
	return nil
}

func cliIdentifierFamilyDisplayEligible(family cliIdentifierFamily) bool {
	if err := validateCLIIdentifierRegistry(); err != nil {
		return false
	}
	if _, ok := cliIdentifierFamilyPolicyFor(family); !ok {
		return false
	}
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

func formatCLIIdentifierForDisplay(family cliIdentifierFamily, value string) string {
	if family == cliIdentifierFamilyNone {
		return value
	}
	policy, ok := cliIdentifierFamilyPolicyFor(family)
	if !ok || !cliIdentifierFamilyDisplayEligible(family) {
		return value
	}
	switch policy.DisplayProjection {
	case cliIdentifierDisplayFull:
		return value
	default:
		return value
	}
}
