package apiv1

import (
	"fmt"
	"sync"
	"time"

	"github.com/division-sh/swarm/internal/runtime"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
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
	Database                  Pinger
	Runs                      RunReadStore
	Observability             ObservabilityReadStore
	Entities                  EntityReadStore
	AgentConversations        any
	AgentDeliveryLifecycle    AgentDeliveryLifecycleReadStore
	AgentUsage                AgentUsageReadStore
	Data                      DurableDataStore
	AgentFrameEffective       AgentFrameEffectiveResolver
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
	RuntimePublication        RuntimePublicationReader
	ResetCoordinator          DestructiveResetCoordinator
	Source                    semanticview.Source
	Bundle                    runtimecontracts.BundleIdentity
	RuntimeIdentity           RuntimeIdentityResult
}

type testRuntimePublicationReader struct {
	snapshot runtime.RuntimeContextPublicationSnapshot
	err      error
}

type mutableTestRuntimePublicationReader struct {
	mu       sync.RWMutex
	snapshot runtime.RuntimeContextPublicationSnapshot
	err      error
}

func (r *mutableTestRuntimePublicationReader) CurrentPublication() (runtime.RuntimeContextPublicationSnapshot, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.snapshot, r.err
}

func (r *mutableTestRuntimePublicationReader) set(snapshot runtime.RuntimeContextPublicationSnapshot, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.snapshot = snapshot
	r.err = err
}

func (r testRuntimePublicationReader) CurrentPublication() (runtime.RuntimeContextPublicationSnapshot, error) {
	return r.snapshot, r.err
}

func (c testOperatorCapabilities) runtimePublication() RuntimePublicationReader {
	if c.RuntimePublication != nil {
		return c.RuntimePublication
	}
	reader := testRuntimePublicationReader{snapshot: runtime.RuntimeContextPublicationSnapshot{PrimaryBundle: c.Bundle}}
	for _, source := range c.RuntimeIdentity.SourceArtifacts {
		fact, err := runtimecorrelation.DecodeSourceArtifactFact(source.BundleHash)
		if err != nil {
			reader.err = fmt.Errorf("decode test runtime publication: %w", err)
			return reader
		}
		reader.snapshot.SourceArtifactFacts = append(reader.snapshot.SourceArtifactFacts, fact)
	}
	if len(reader.snapshot.SourceArtifactFacts) == 0 && c.Bundle.BundleHash != "" {
		fact, err := runtimecorrelation.NewSourceArtifactFact(c.Bundle.BundleHash)
		if err != nil {
			reader.err = fmt.Errorf("construct test runtime publication: %w", err)
			return reader
		}
		reader.snapshot.SourceArtifactFacts = []runtimecorrelation.SourceArtifactFact{fact}
	}
	return reader
}

func (c testOperatorCapabilities) publication() EventPublicationOptions {
	acknowledged, _ := c.Events.(AcknowledgedEventPublisher)
	recipientPlans, _ := c.Events.(EventRecipientPlanChecker)
	bundleSource, _ := c.Events.(SourceArtifactAdmitter)
	return EventPublicationOptions{
		ExecutionPosture: c.posture(),
		Now:              c.Now, Idempotency: c.Idempotency, Events: c.Events, Acknowledged: acknowledged,
		RecipientPlans: recipientPlans, SourceArtifact: bundleSource,
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
	bundleSource, _ := c.Events.(SourceArtifactAdmitter)
	return DecisionCardHandlerOptions{
		Now: c.Now, Cards: c.DecisionCards, ProposedEffects: proposedEffects,
		Mailbox: c.Mailbox, NoticeAcknowledgment: noticeAcknowledgment,
		Authority: c.DecisionAuthority, SourceArtifact: bundleSource,
		Idempotency: c.Idempotency, RuntimeContexts: c.RuntimeContexts,
	}
}

func testOperatorHandlers(c testOperatorCapabilities) map[string]MethodHandler {
	agents, _ := c.AgentConversations.(AgentReadStore)
	conversations, _ := c.AgentConversations.(ConversationReadStore)
	return MergeOperatorHandlers(
		OperatorHealthHandlers(HealthHandlerOptions{ExecutionPosture: c.posture(), Now: c.Now, Ready: c.Ready, Database: c.Database, Publication: c.runtimePublication()}),
		OperatorRuntimeIdentityHandlers(RuntimeIdentityHandlerOptions{Identity: c.RuntimeIdentity, Publication: c.runtimePublication()}),
		OperatorRunReadHandlers(RunReadHandlerOptions{Runs: c.Runs}),
		OperatorMailboxHandlers(MailboxHandlerOptions{Mailbox: c.Mailbox}),
		OperatorDecisionCardHandlers(c.decisionCards()),
		OperatorRunStartHandlers(RunStartHandlerOptions{Publication: c.publication()}),
		OperatorEventPublishHandlers(EventPublishHandlerOptions{Publication: c.publication()}),
		OperatorTestSetupHandlers(TestSetupHandlerOptions{Now: c.Now, Setup: c.TestSetup, Idempotency: c.Idempotency, RunBundleContext: c.RunBundleContext, RuntimeContexts: c.RuntimeContexts, SourceArtifact: c.publication().SourceArtifact, Source: c.Source}),
		testOperatorEventReplayHandlers(c),
		testOperatorRunForkHandlers(c),
		OperatorRunControlHandlers(RunControlHandlerOptions{Now: c.Now, Controller: c.RunControl, Idempotency: c.Idempotency, RuntimeContexts: c.RuntimeContexts}),
		OperatorStandingServiceHandlers(StandingServiceHandlerOptions{Controller: c.StandingServices, Idempotency: c.Idempotency}),
		OperatorRuntimeControlHandlers(RuntimeControlHandlerOptions{Now: c.Now, Ingress: c.RuntimeIngress, Idempotency: c.Idempotency, RuntimeContexts: c.RuntimeContexts}),
		OperatorRuntimeNukeHandlers(RuntimeNukeHandlerOptions{Now: c.Now, Coordinator: c.ResetCoordinator, Idempotency: c.Idempotency}),
		OperatorObservabilityHandlers(ObservabilityHandlerOptions{Observability: c.Observability}),
		OperatorEntityHandlers(EntityHandlerOptions{Entities: c.Entities}),
		OperatorAgentConversationHandlers(AgentConversationHandlerOptions{Agents: agents, Conversations: conversations, DeliveryLifecycle: c.AgentDeliveryLifecycle, Usage: c.AgentUsage}),
		OperatorDataHandlers(DataHandlerOptions{Store: c.Data}),
		OperatorAgentFrameHandlers(AgentFrameHandlerOptions{Effective: c.AgentFrameEffective}),
		OperatorConversationForkHandlers(ConversationForkHandlerOptions{ExecutionPosture: c.posture(), Now: c.Now, Reads: c.ConversationForks, Lifecycle: c.ConversationForkLifecycle, Chat: c.ForkChatExecutor, Idempotency: c.Idempotency}),
		OperatorAgentControlHandlers(AgentControlHandlerOptions{Now: c.Now, Controller: c.AgentControl, Idempotency: c.Idempotency, RuntimeContexts: c.RuntimeContexts}),
	)
}

func testOperatorSubscriptions(c testOperatorCapabilities, overrides ...SubscriptionRuntimeOptions) *SubscriptionRuntime {
	proposedEffects, _ := c.DecisionCards.(decisioncard.ProposedEffectStore)
	return OperatorSubscriptions(SubscriptionOptions{
		ExecutionPosture: c.posture(),
		Now:              c.Now, Ready: c.Ready, Database: c.Database, Observability: c.Observability,
		DecisionCards: c.DecisionCards, ProposedEffects: proposedEffects, Publication: c.runtimePublication(),
	}, overrides...)
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
