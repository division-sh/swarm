package apiv1

import (
	"context"
	"encoding/json"
	"time"

	"github.com/division-sh/swarm/internal/packartifact"
	"github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/bundledelete"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	decisioncard "github.com/division-sh/swarm/internal/runtime/decisioncard"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/scenarioexecution"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

type BundleDeleteExecutor interface {
	Execute(context.Context, bundledelete.Request) (bundledelete.Result, error)
}

type TestSetupStore interface {
	SetupScenarioEntities(context.Context, runtimepipeline.ScenarioSetupRequest) (runtimepipeline.ScenarioSetupResult, error)
}

type DecisionCardAuthority interface {
	CommitDecisionCardMutation(
		context.Context,
		runtimepipeline.DecisionCardMutationIdempotency,
		runtimepipeline.DecisionCardMutationIdempotencyRequest,
		runtimepipeline.DecisionCardMutation,
	) (json.RawMessage, bool, error)
}

type StandingServiceController interface {
	SuspendStandingService(context.Context, runtimepipeline.StandingServiceOperation) (runtimepipeline.StandingServiceReconciliation, error)
	ResumeStandingService(context.Context, runtimepipeline.StandingServiceOperation) (runtimepipeline.StandingServiceReconciliation, error)
	ResetStandingService(context.Context, runtimepipeline.StandingServiceOperation) (runtimepipeline.StandingServiceReconciliation, error)
}

type RuntimePublicationReader interface {
	CurrentPublication() (runtime.RuntimeContextPublicationSnapshot, error)
}

// Each handler family receives only the capabilities needed to execute its
// declared methods. Runtime selection remains explicit on the families that
// can target a loaded bundle context.
type HealthHandlerOptions struct {
	ExecutionPosture executionposture.Posture
	Now              func() time.Time
	Ready            func() bool
	Database         Pinger
	Publication      RuntimePublicationReader
}

type RuntimeIdentityHandlerOptions struct {
	Identity    RuntimeIdentityResult
	Publication RuntimePublicationReader
}

type RunReadHandlerOptions struct {
	Runs RunReadStore
}

type BundleCatalogHandlerOptions struct {
	Catalog BundleCatalogReadStore
}

type AgentConversationHandlerOptions struct {
	Agents            AgentReadStore
	Conversations     ConversationReadStore
	DeliveryLifecycle AgentDeliveryLifecycleReadStore
	Usage             AgentUsageReadStore
}

type AgentFrameInspectionHandlerOptions = AgentFrameHandlerOptions

type EntityHandlerOptions struct {
	Entities EntityReadStore
}

type ObservabilityHandlerOptions struct {
	Observability ObservabilityReadStore
}

type MailboxHandlerOptions struct {
	Mailbox MailboxAPIStore
}

type EventPublicationOptions struct {
	ExecutionPosture          executionposture.Posture
	Now                       func() time.Time
	Idempotency               APIIdempotencyStore
	Events                    EventPublisher
	Acknowledged              AcknowledgedEventPublisher
	RecipientPlans            EventRecipientPlanChecker
	BundleSource              BundleSourceAdmitter
	Runs                      RunReadStore
	Entities                  EntityReadStore
	Observability             ObservabilityReadStore
	RunBundleContext          RunBundleContextStore
	RuntimeContexts           *runtime.RuntimeContextManager
	Source                    semanticview.Source
	Bundle                    runtimecontracts.BundleIdentity
	EffectiveSourceIdentity   scenarioexecution.EffectiveSourceIdentity
	ScenarioProfileCatalog    *scenarioexecution.Catalog
	ScenarioExecutionProfiles runtimepipeline.ScenarioExecutionProfileReader
}

type RunStartHandlerOptions struct {
	Publication EventPublicationOptions
}

type EventPublishHandlerOptions struct {
	Publication EventPublicationOptions
}

type EventReplayHandlerOptions struct {
	ExecutionPosture executionposture.Posture
	Now              func() time.Time
	Idempotency      APIIdempotencyStore
	Events           EventReplayOwner
	Observability    ObservabilityReadStore
	AgentIdentities  AgentIdentityResolver
	RuntimeContexts  *runtime.RuntimeContextManager
}

type BundleRegisterHandlerOptions struct {
	Now                func() time.Time
	RepoRoot           string
	PlatformSpecPath   string
	PlatformPackBases  packartifact.PlatformPackBaseResolver
	AdmitPackInventory func(*packartifact.EffectivePackInventory, runtimecontracts.PlatformSpecDocument) (runtimecontracts.PackAdmissionProjection, error)
	Register           BundleCatalogRegisterStore
	Idempotency        APIIdempotencyStore
}

type BundleDeleteHandlerOptions struct {
	Now         func() time.Time
	Executor    BundleDeleteExecutor
	Idempotency APIIdempotencyStore
}

type ConversationForkHandlerOptions struct {
	Now              func() time.Time
	Reads            ConversationForkReadStore
	Lifecycle        ConversationForkLifecycleStore
	Chat             ForkChatExecutor
	Idempotency      APIIdempotencyStore
	ExecutionPosture executionposture.Posture
}

type DecisionCardHandlerOptions struct {
	Now                  func() time.Time
	Cards                decisioncard.Store
	ProposedEffects      decisioncard.ProposedEffectStore
	Mailbox              MailboxAPIStore
	NoticeAcknowledgment MailboxNoticeAcknowledgmentStore
	Authority            DecisionCardAuthority
	BundleSource         BundleSourceAdmitter
	Idempotency          APIIdempotencyStore
	RuntimeContexts      *runtime.RuntimeContextManager
}

type AgentControlHandlerOptions struct {
	Now             func() time.Time
	Controller      AgentControlController
	Idempotency     APIIdempotencyStore
	RuntimeContexts *runtime.RuntimeContextManager
}

type RunControlHandlerOptions struct {
	Now             func() time.Time
	Controller      RunControlController
	Idempotency     APIIdempotencyStore
	RuntimeContexts *runtime.RuntimeContextManager
}

type RunForkHandlerOptions struct {
	Now             func() time.Time
	Availability    RunForkAvailabilityStore
	Executor        RunForkExecutor
	Selector        RunForkExecutorSelector
	Idempotency     APIIdempotencyStore
	RuntimeContexts *runtime.RuntimeContextManager
}

type RuntimeControlHandlerOptions struct {
	Now             func() time.Time
	Ingress         RuntimeIngressController
	Idempotency     APIIdempotencyStore
	RuntimeContexts *runtime.RuntimeContextManager
}

type RuntimeNukeHandlerOptions struct {
	Now         func() time.Time
	Coordinator DestructiveResetCoordinator
	Idempotency APIIdempotencyStore
}

type StandingServiceHandlerOptions struct {
	Controller  StandingServiceController
	Idempotency APIIdempotencyStore
}

type TestSetupHandlerOptions struct {
	Now                       func() time.Time
	Setup                     TestSetupStore
	Idempotency               APIIdempotencyStore
	RunBundleContext          RunBundleContextStore
	RuntimeContexts           *runtime.RuntimeContextManager
	BundleSource              BundleSourceAdmitter
	Source                    semanticview.Source
	ExecutionPosture          executionposture.Posture
	EffectiveSourceIdentity   scenarioexecution.EffectiveSourceIdentity
	ScenarioProfileCatalog    *scenarioexecution.Catalog
	ScenarioExecutionProfiles runtimepipeline.ScenarioExecutionProfileReader
}

type SubscriptionOptions struct {
	ExecutionPosture executionposture.Posture
	Now              func() time.Time
	Ready            func() bool
	Database         Pinger
	Observability    ObservabilityReadStore
	DecisionCards    decisioncard.Store
	ProposedEffects  decisioncard.ProposedEffectStore
	Publication      RuntimePublicationReader
}

func MergeOperatorHandlers(groups ...map[string]MethodHandler) map[string]MethodHandler {
	handlers := make(map[string]MethodHandler)
	for _, group := range groups {
		for method, handler := range group {
			handlers[method] = handler
		}
	}
	return handlers
}
