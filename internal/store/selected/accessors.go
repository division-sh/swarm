package selected

import (
	"errors"

	apiv1 "github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/durabledata"
	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/runtime"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	worklifetime "github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runbundle"
	runtimerunforkexecution "github.com/division-sh/swarm/internal/runtime/runforkexecution"
	runtimerunquiescence "github.com/division-sh/swarm/internal/runtime/runquiescence"
	runtimerunstalled "github.com/division-sh/swarm/internal/runtime/runstalled"
	runtimestartupownership "github.com/division-sh/swarm/internal/runtime/startupownership"
	runtimetools "github.com/division-sh/swarm/internal/runtime/tools"
	"github.com/division-sh/swarm/internal/store"
)

func (o *Owner) RuntimeDeps() runtime.RuntimeDeps {
	if o == nil {
		return runtime.RuntimeDeps{}
	}
	deps := o.core
	deps.AuthorActivityRegistrars = append([]runtime.AuthorActivityCatalogRegistrar(nil), deps.AuthorActivityRegistrars...)
	return deps
}

func (o *Owner) Schema() store.SchemaBootstrapper                { return o.required.schema }
func (o *Owner) Pinger() apiv1.Pinger                            { return o.required.pinger }
func (o *Owner) AuthorActivity() runtimeauthoractivity.Reader    { return o.required.authorActivity }
func (o *Owner) OperatorChannels() operatorchannel.Store         { return o.required.operatorChannels }
func (o *Owner) ChannelOnboarding() channelonboarding.Store      { return o.required.channelOnboarding }
func (o *Owner) StartupOwnership() runtimestartupownership.Store { return o.required.startupOwnership }
func (o *Owner) RunQuiescence() runtimerunquiescence.ServeAbandonStore {
	return o.required.runQuiescence
}
func (o *Owner) MailboxAPI() apiv1.MailboxAPIStore { return o.required.mailboxAPI }
func (o *Owner) MailboxNoticeAcknowledgment() apiv1.MailboxNoticeAcknowledgmentStore {
	return o.required.mailboxNoticeAck
}
func (o *Owner) Observability() apiv1.ObservabilityReadStore { return o.required.observability }
func (o *Owner) AgentUsage() apiv1.AgentUsageReadStore       { return o.required.agentUsage }
func (o *Owner) AgentDeliveryLifecycle() apiv1.AgentDeliveryLifecycleReadStore {
	return o.required.agentDeliveryLifecycle
}
func (o *Owner) Idempotency() apiv1.APIIdempotencyStore        { return o.required.idempotency }
func (o *Owner) Runs() apiv1.RunReadStore                      { return o.required.runs }
func (o *Owner) Entities() apiv1.EntityReadStore               { return o.required.entities }
func (o *Owner) Agents() apiv1.AgentReadStore                  { return o.required.agents }
func (o *Owner) Conversations() apiv1.ConversationReadStore    { return o.required.conversations }
func (o *Owner) TestSetup() apiv1.TestSetupStore               { return o.required.testSetup }
func (o *Owner) RunBundleContext() apiv1.RunBundleContextStore { return o.required.runBundleContext }
func (o *Owner) Data() apiv1.DurableDataStore                  { return o.required.data }
func (o *Owner) DataAccess() durabledata.ResourceAccessStore   { return o.required.dataAccess }
func (o *Owner) RunBundleAvailability() runbundle.AvailabilityStore {
	return o.required.runBundleAvailability
}
func (o *Owner) RunStalled() runtimerunstalled.ProjectionReader { return o.required.runStalled }
func (o *Owner) SourceArtifactWriter() SourceArtifactDataWriter {
	return o.required.sourceArtifacts
}

func (o *Owner) Effects() runtimeeffects.Store              { return o.core.EffectsStore }
func (o *Owner) Completion() runtimeeffects.CompletionStore { return o.core.CompletionStore }
func (o *Owner) CompletionHeartbeat() runtimeeffects.CompletionHeartbeatStore {
	return o.core.CompletionHeartbeatStore
}
func (o *Owner) Mailbox() runtimetools.MailboxPersistence { return o.core.MailboxStore }
func (o *Owner) ScenarioExecutionProfiles() runtimepipeline.ScenarioExecutionProfileReader {
	return o.core.ScenarioExecutionProfiles
}

func (o *Owner) SourceArtifactStore() runtimerunforkexecution.SourceArtifactSelectedContractSourceStore {
	if o == nil {
		return nil
	}
	return o.required.sourceArtifactReader
}

func (o *Owner) ConversationFork() (ConversationFork, bool) {
	if o == nil || !o.products.conversationAvailable {
		return ConversationFork{}, false
	}
	return o.products.conversationFork, true
}

func (o *Owner) RunFork() (RunFork, bool) {
	if o == nil || !o.products.runForkAvailable {
		return RunFork{}, false
	}
	return o.products.runFork, true
}

func (o *Owner) DestructiveReset() (DestructiveReset, bool) {
	if o == nil || !o.products.destructiveAvailable {
		return DestructiveReset{}, false
	}
	return o.products.destructiveReset, true
}

func (o *Owner) StartupRecovery() (StartupRecovery, bool) {
	if o == nil || !o.products.startupRecoveryAvailable {
		return StartupRecovery{}, false
	}
	return o.products.startupRecovery, true
}

func (o *Owner) Activate(process *worklifetime.Process) error {
	if o == nil || process == nil {
		return errors.New("selected store activation requires a process work owner")
	}
	o.lifetime.mu.Lock()
	defer o.lifetime.mu.Unlock()
	if o.lifetime.state != ownershipUnactivated {
		return errors.New("selected store activation requires unactivated construction state")
	}
	o.lifetime.process = process
	o.lifetime.state = ownershipActivated
	return nil
}

func (o *Owner) CloseUnactivated() error {
	if o == nil {
		return nil
	}
	o.lifetime.mu.Lock()
	defer o.lifetime.mu.Unlock()
	if o.lifetime.state == ownershipClosed {
		return nil
	}
	if o.lifetime.state != ownershipUnactivated {
		return errors.New("activated selected store requires a process join receipt")
	}
	if o.lifetime.resource == nil {
		return errors.New("selected store close resource is required")
	}
	if err := o.lifetime.resource.Close(); err != nil {
		return err
	}
	o.lifetime.state = ownershipClosed
	return nil
}

func (o *Owner) CloseActivated(receipt *worklifetime.ProcessJoinReceipt) error {
	if o == nil {
		return nil
	}
	o.lifetime.mu.Lock()
	defer o.lifetime.mu.Unlock()
	if o.lifetime.state == ownershipClosed {
		return nil
	}
	if o.lifetime.state != ownershipActivated || o.lifetime.process == nil {
		return errors.New("selected store is not activated")
	}
	if err := o.lifetime.process.ValidateJoinReceipt(receipt); err != nil {
		return err
	}
	if o.lifetime.resource == nil {
		return errors.New("selected store close resource is required")
	}
	if err := o.lifetime.resource.Close(); err != nil {
		return err
	}
	o.lifetime.state = ownershipClosed
	return nil
}
