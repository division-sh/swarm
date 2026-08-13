package apiv1

import (
	"time"

	"github.com/division-sh/swarm/internal/runtime"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	decisioncard "github.com/division-sh/swarm/internal/runtime/decisioncard"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

// testOperatorCapabilities is fixture assembly only. Production registration
// must use the exact family option types in operator_capabilities.go.
type testOperatorCapabilities struct {
	ExecutionPosture          executionposture.Posture
	Now                       func() time.Time
	Ready                     func() bool
	RepoRoot                  string
	PlatformSpecPath          string
	Database                  Pinger
	Runs                      RunReadStore
	Observability             ObservabilityReadStore
	Entities                  EntityReadStore
	AgentConversations        any
	AgentDeliveryLifecycle    AgentDeliveryLifecycleReadStore
	AgentUsage                AgentUsageReadStore
	BundleCatalog             BundleCatalogReadStore
	AgentFrameEffective       AgentFrameEffectiveResolver
	BundleDelete              BundleDeleteExecutor
	ConversationForks         ConversationForkReadStore
	ConversationForkLifecycle ConversationForkLifecycleStore
	ForkChatExecutor          ForkChatExecutor
	RunBundleContext          RunBundleContextStore
	RunForkAvailability       RunForkAvailabilityStore
	RunFork                   RunForkExecutor
	AgentControl              AgentControlController
	Mailbox                   MailboxAPIStore
	DecisionCards             decisioncard.Store
	DecisionAuthority         DecisionCardAuthority
	TestSetup                 TestSetupStore
	Idempotency               APIIdempotencyStore
	Events                    EventPublisher
	RunControl                RunControlController
	StandingServices          StandingServiceController
	RuntimeIngress            RuntimeIngressController
	RuntimeContexts           *runtime.RuntimeContextManager
	ResetCoordinator          DestructiveResetCoordinator
	Source                    semanticview.Source
	Bundle                    runtimecontracts.BundleIdentity
	RuntimeIdentity           RuntimeIdentityResult
}

func (c testOperatorCapabilities) publication() EventPublicationOptions {
	acknowledged, _ := c.Events.(AcknowledgedEventPublisher)
	recipientPlans, _ := c.Events.(EventRecipientPlanChecker)
	bundleSource, _ := c.Events.(BundleSourceAdmitter)
	return EventPublicationOptions{
		ExecutionPosture: c.posture(),
		Now:              c.Now, Idempotency: c.Idempotency, Events: c.Events, Acknowledged: acknowledged,
		RecipientPlans: recipientPlans, BundleSource: bundleSource,
		Runs: c.Runs, Entities: c.Entities, Observability: c.Observability,
		RunBundleContext: c.RunBundleContext, RuntimeContexts: c.RuntimeContexts,
		Source: c.Source, Bundle: c.Bundle,
	}
}

func (c testOperatorCapabilities) posture() executionposture.Posture {
	if c.ExecutionPosture.Valid() {
		return c.ExecutionPosture
	}
	return executionposture.Live
}

func (c testOperatorCapabilities) decisionCards() DecisionCardHandlerOptions {
	proposedEffects, _ := c.DecisionCards.(decisioncard.ProposedEffectStore)
	noticeAcknowledgment, _ := c.Mailbox.(MailboxNoticeAcknowledgmentStore)
	bundleSource, _ := c.Events.(BundleSourceAdmitter)
	return DecisionCardHandlerOptions{
		Now: c.Now, Cards: c.DecisionCards, ProposedEffects: proposedEffects,
		Mailbox: c.Mailbox, NoticeAcknowledgment: noticeAcknowledgment,
		Authority: c.DecisionAuthority, BundleSource: bundleSource,
		Idempotency: c.Idempotency, RuntimeContexts: c.RuntimeContexts,
	}
}

func testOperatorHandlers(c testOperatorCapabilities) map[string]MethodHandler {
	agents, _ := c.AgentConversations.(AgentReadStore)
	conversations, _ := c.AgentConversations.(ConversationReadStore)
	return MergeOperatorHandlers(
		OperatorHealthHandlers(HealthHandlerOptions{ExecutionPosture: c.posture(), Now: c.Now, Ready: c.Ready, Database: c.Database, Bundle: c.Bundle}),
		OperatorRuntimeIdentityHandlers(RuntimeIdentityHandlerOptions{Identity: c.RuntimeIdentity}),
		OperatorRunReadHandlers(RunReadHandlerOptions{Runs: c.Runs}),
		OperatorMailboxHandlers(MailboxHandlerOptions{Mailbox: c.Mailbox}),
		OperatorDecisionCardHandlers(c.decisionCards()),
		OperatorRunStartHandlers(RunStartHandlerOptions{Publication: c.publication()}),
		OperatorEventPublishHandlers(EventPublishHandlerOptions{Publication: c.publication()}),
		OperatorTestSetupHandlers(TestSetupHandlerOptions{Now: c.Now, Setup: c.TestSetup, Idempotency: c.Idempotency, RunBundleContext: c.RunBundleContext, RuntimeContexts: c.RuntimeContexts, BundleSource: c.publication().BundleSource, Source: c.Source}),
		testOperatorEventReplayHandlers(c),
		testOperatorRunForkHandlers(c),
		OperatorRunControlHandlers(RunControlHandlerOptions{Now: c.Now, Controller: c.RunControl, Idempotency: c.Idempotency, RuntimeContexts: c.RuntimeContexts}),
		OperatorStandingServiceHandlers(StandingServiceHandlerOptions{Controller: c.StandingServices, Idempotency: c.Idempotency}),
		OperatorRuntimeControlHandlers(RuntimeControlHandlerOptions{Now: c.Now, Ingress: c.RuntimeIngress, Idempotency: c.Idempotency, RuntimeContexts: c.RuntimeContexts}),
		OperatorRuntimeNukeHandlers(RuntimeNukeHandlerOptions{Now: c.Now, Coordinator: c.ResetCoordinator, Idempotency: c.Idempotency}),
		OperatorObservabilityHandlers(ObservabilityHandlerOptions{Observability: c.Observability}),
		OperatorEntityHandlers(EntityHandlerOptions{Entities: c.Entities}),
		OperatorAgentConversationHandlers(AgentConversationHandlerOptions{Agents: agents, Conversations: conversations, DeliveryLifecycle: c.AgentDeliveryLifecycle, Usage: c.AgentUsage}),
		OperatorBundleCatalogHandlers(BundleCatalogHandlerOptions{Catalog: c.BundleCatalog}),
		OperatorAgentFrameHandlers(AgentFrameHandlerOptions{Catalog: c.BundleCatalog, Effective: c.AgentFrameEffective}),
		testOperatorBundleRegisterHandlers(c),
		OperatorBundleDeleteHandlers(BundleDeleteHandlerOptions{Now: c.Now, Executor: c.BundleDelete, Idempotency: c.Idempotency}),
		OperatorConversationForkHandlers(ConversationForkHandlerOptions{ExecutionPosture: c.posture(), Now: c.Now, Reads: c.ConversationForks, Lifecycle: c.ConversationForkLifecycle, Chat: c.ForkChatExecutor, Idempotency: c.Idempotency}),
		OperatorAgentControlHandlers(AgentControlHandlerOptions{Now: c.Now, Controller: c.AgentControl, Idempotency: c.Idempotency, RuntimeContexts: c.RuntimeContexts}),
	)
}

func testOperatorSubscriptions(c testOperatorCapabilities, overrides ...SubscriptionRuntimeOptions) *SubscriptionRuntime {
	proposedEffects, _ := c.DecisionCards.(decisioncard.ProposedEffectStore)
	return OperatorSubscriptions(SubscriptionOptions{
		ExecutionPosture: c.posture(),
		Now:              c.Now, Ready: c.Ready, Database: c.Database, Observability: c.Observability,
		DecisionCards: c.DecisionCards, ProposedEffects: proposedEffects, Bundle: c.Bundle,
	}, overrides...)
}

func testOperatorBundleRegisterHandlers(c testOperatorCapabilities) map[string]MethodHandler {
	register, _ := c.BundleCatalog.(BundleCatalogRegisterStore)
	return OperatorBundleRegisterHandlers(BundleRegisterHandlerOptions{Now: c.Now, RepoRoot: c.RepoRoot, PlatformSpecPath: c.PlatformSpecPath, Register: register, Idempotency: c.Idempotency})
}

func testOperatorConversationForkHandlers(c testOperatorCapabilities) map[string]MethodHandler {
	return OperatorConversationForkHandlers(ConversationForkHandlerOptions{ExecutionPosture: c.posture(), Now: c.Now, Reads: c.ConversationForks, Lifecycle: c.ConversationForkLifecycle, Chat: c.ForkChatExecutor, Idempotency: c.Idempotency})
}

func testOperatorEventReplayHandlers(c testOperatorCapabilities) map[string]MethodHandler {
	events, _ := c.Events.(EventReplayOwner)
	agentIdentities, _ := c.AgentConversations.(AgentIdentityResolver)
	return OperatorEventReplayHandlers(EventReplayHandlerOptions{ExecutionPosture: c.posture(), Now: c.Now, Idempotency: c.Idempotency, Events: events, Observability: c.Observability, AgentIdentities: agentIdentities, RuntimeContexts: c.RuntimeContexts})
}

func testOperatorRunForkHandlers(c testOperatorCapabilities) map[string]MethodHandler {
	selector, _ := c.RunFork.(RunForkExecutorSelector)
	return OperatorRunForkHandlers(RunForkHandlerOptions{Now: c.Now, Availability: c.RunForkAvailability, Executor: c.RunFork, Selector: selector, Idempotency: c.Idempotency, RuntimeContexts: c.RuntimeContexts})
}

func testOperatorRunControlHandlers(c testOperatorCapabilities) map[string]MethodHandler {
	return OperatorRunControlHandlers(RunControlHandlerOptions{Now: c.Now, Controller: c.RunControl, Idempotency: c.Idempotency, RuntimeContexts: c.RuntimeContexts})
}

func testOperatorBundleDeleteHandlers(c testOperatorCapabilities) map[string]MethodHandler {
	return OperatorBundleDeleteHandlers(BundleDeleteHandlerOptions{Now: c.Now, Executor: c.BundleDelete, Idempotency: c.Idempotency})
}
