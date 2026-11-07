package runtimepersistence

import (
	"fmt"
	"time"

	storeadmin "github.com/division-sh/swarm/internal/store/internal/adminpersistence"
	storeapiidempotency "github.com/division-sh/swarm/internal/store/internal/apiidempotency"
	storeactivityjournal "github.com/division-sh/swarm/internal/store/internal/backend/activityjournal"
	storeactivityresult "github.com/division-sh/swarm/internal/store/internal/backend/activityresult"
	storeagent "github.com/division-sh/swarm/internal/store/internal/backend/agentpersistence"
	storechannelonboarding "github.com/division-sh/swarm/internal/store/internal/backend/channelonboarding"
	storedecision "github.com/division-sh/swarm/internal/store/internal/backend/decisionpersistence"
	storedelivery "github.com/division-sh/swarm/internal/store/internal/backend/delivery"
	storeeffect "github.com/division-sh/swarm/internal/store/internal/backend/effectpersistence"
	storeentity "github.com/division-sh/swarm/internal/store/internal/backend/entityruntime"
	storeevent "github.com/division-sh/swarm/internal/store/internal/backend/eventpersistence"
	storegenericschedule "github.com/division-sh/swarm/internal/store/internal/backend/genericschedule"
	storellm "github.com/division-sh/swarm/internal/store/internal/backend/llmpersistence"
	storemanagedcapability "github.com/division-sh/swarm/internal/store/internal/backend/managedcapability"
	storeoperatorchannel "github.com/division-sh/swarm/internal/store/internal/backend/operatorchannel"
	storepipeline "github.com/division-sh/swarm/internal/store/internal/backend/pipelinepersistence"
	postgresbackend "github.com/division-sh/swarm/internal/store/internal/backend/postgres"
	storereplycontext "github.com/division-sh/swarm/internal/store/internal/backend/replycontext"
	storerunfork "github.com/division-sh/swarm/internal/store/internal/backend/runforkpersistence"
	storerunlifecycle "github.com/division-sh/swarm/internal/store/internal/backend/runlifecycle"
	sqlitebackend "github.com/division-sh/swarm/internal/store/internal/backend/sqlite"
	storetimerobligation "github.com/division-sh/swarm/internal/store/internal/backend/timerobligation"
	storebudgetspend "github.com/division-sh/swarm/internal/store/internal/budgetspend"
	storedurabledata "github.com/division-sh/swarm/internal/store/internal/durabledata"
	storeingress "github.com/division-sh/swarm/internal/store/internal/ingresspersistence"
	storemailbox "github.com/division-sh/swarm/internal/store/internal/mailboxpersistence"
	storeoperatorsurface "github.com/division-sh/swarm/internal/store/internal/operatorsurface"
	storerunbundle "github.com/division-sh/swarm/internal/store/internal/runbundle"
	storerunhandoff "github.com/division-sh/swarm/internal/store/internal/runhandoff"
	storeschema "github.com/division-sh/swarm/internal/store/internal/schemastore"
	storesourceartifact "github.com/division-sh/swarm/internal/store/internal/sourceartifact"
	storestartupownership "github.com/division-sh/swarm/internal/store/internal/startupownership"
	storeworkflowentityquery "github.com/division-sh/swarm/internal/store/internal/workflowentityquery"
	storeworkflowroute "github.com/division-sh/swarm/internal/store/internal/workflowroute"
)

func newPostgresStoreComposition(backend *postgresbackend.Backend) (*PostgresStore, error) {
	schemaOwner, err := storeschema.NewPostgres(backend)
	if err != nil {
		return nil, err
	}
	runBundles, err := storerunbundle.NewPostgres(backend)
	if err != nil {
		return nil, err
	}
	timerObligations, err := storetimerobligation.NewPostgres(backend, schemaOwner.RequireCurrent)
	if err != nil {
		return nil, err
	}
	workflowEntityQueries, err := storeworkflowentityquery.NewPostgres(backend)
	if err != nil {
		return nil, err
	}
	workflowRoutes, err := storeworkflowroute.NewPostgres(backend)
	if err != nil {
		return nil, err
	}
	candidates := storerunhandoff.NewCandidateCoordinator()
	store := &PostgresStore{
		backend:                backend,
		runBundles:             runBundles,
		runLifecycleCandidates: candidates,
		workflowEntityQueries:  workflowEntityQueries,
		workflowRoutes:         workflowRoutes,
		schemaOwner:            schemaOwner,
	}
	store.timerObligationPostgresReader = timerObligations
	store.timerObligationReaderMethods = timerObligations
	apiIdempotency, err := storeapiidempotency.NewPostgres(backend, store.requireCurrentSchema)
	if err != nil {
		return nil, err
	}
	store.postgresOwner = apiIdempotency
	store.apiIdempotencyMethods = apiIdempotency
	operatorChannels, err := storeoperatorchannel.NewPostgres(backend, store.requireCurrentSchema)
	if err != nil {
		return nil, err
	}
	store.operatorChannelPostgresOwner = operatorChannels
	store.operatorChannelOwnerMethods = operatorChannels
	if err := operatorChannels.BindChannelReconciliation(&store.channelChanges); err != nil {
		return nil, err
	}
	channelOnboarding, err := storechannelonboarding.NewPostgres(backend, store.requireCurrentSchema)
	if err != nil {
		return nil, err
	}
	store.channelOnboardingPostgresOwner = channelOnboarding
	store.channelOnboardingOwnerMethods = channelOnboarding
	if err := channelOnboarding.BindChannelReconciliation(&store.channelChanges); err != nil {
		return nil, err
	}
	sourceArtifacts, err := storesourceartifact.NewPostgres(backend, store.requireCurrentSchema)
	if err != nil {
		return nil, err
	}
	store.sourceArtifactOwner = sourceArtifacts
	durableData, err := storedurabledata.NewPostgres(backend, store.requireCurrentSchema)
	if err != nil {
		return nil, err
	}
	store.durableDataOwner = durableData
	operatorObservability, err := storeoperatorsurface.NewObservabilityPostgres(backend, store.requireCurrentSchema)
	if err != nil {
		return nil, err
	}
	store.operatorObservabilityPostgres = operatorObservability
	store.operatorObservabilityMethods = operatorObservability
	operatorConversation, err := storeoperatorsurface.NewConversationPostgres(backend, store.requireCurrentSchema)
	if err != nil {
		return nil, err
	}
	store.operatorConversationPostgres = operatorConversation
	store.operatorConversationMethods = operatorConversation
	operatorEntity, err := storeoperatorsurface.NewEntityPostgres(backend, store.requireCurrentSchema)
	if err != nil {
		return nil, err
	}
	store.operatorEntityPostgres = operatorEntity
	store.operatorEntityMethods = operatorEntity
	replyContexts, err := storereplycontext.NewPostgres(backend)
	if err != nil {
		return nil, err
	}
	store.replyPostgresOwner = replyContexts
	store.replyOwnerMethods = replyContexts
	managedCapabilities, err := storemanagedcapability.NewPostgres(backend)
	if err != nil {
		return nil, err
	}
	store.managedCapabilityPostgresOwner = managedCapabilities
	store.managedCapabilityOwnerMethods = managedCapabilities
	mailbox, err := storemailbox.NewPostgres(backend, store.requireCurrentSchema, apiIdempotency)
	if err != nil {
		return nil, err
	}
	store.mailboxPostgresOwner = mailbox
	store.mailboxOwnerMethods = mailbox
	if err := mailbox.BindChannelReconciliation(&store.channelChanges); err != nil {
		return nil, err
	}
	destructiveResetOwner, err := storeadmin.NewDestructiveResetPostgres(backend, store.requireCurrentSchema)
	if err != nil {
		return nil, err
	}
	store.destructiveResetPostgresOwner = destructiveResetOwner
	store.destructiveResetOwnerMethods = destructiveResetOwner
	if err := destructiveResetOwner.BindChannelReconciliation(&store.channelChanges); err != nil {
		return nil, err
	}
	activityJournal, err := storeactivityjournal.NewPostgres(backend, store.requireCurrentSchema)
	if err != nil {
		return nil, err
	}
	store.activityPostgresOwner = activityJournal
	store.activityOwnerMethods = activityJournal
	if err := activityJournal.BindChannelReconciliation(&store.channelChanges); err != nil {
		return nil, err
	}
	activityResult, err := storeactivityresult.NewPostgres(backend, store.requireCurrentSchema)
	if err != nil {
		return nil, err
	}
	store.activityResultPostgresOwner = activityResult
	store.activityResultOwnerMethods = activityResult
	budgetSpend, err := storebudgetspend.NewPostgres(backend)
	if err != nil {
		return nil, err
	}
	store.budgetPostgresOwner = budgetSpend
	store.budgetOwnerMethods = budgetSpend
	agentOwner, err := storeagent.NewPostgres(backend, store.requireCurrentSchema, store)
	if err != nil {
		return nil, err
	}
	store.agentPostgresOwner = agentOwner
	store.agentOwnerMethods = agentOwner
	entityOwner, err := storeentity.NewPostgres(backend)
	if err != nil {
		return nil, err
	}
	store.entityPostgresOwner = entityOwner
	store.entityOwnerMethods = entityOwner
	deadLetterOwner, err := storedelivery.NewDeadLetterPostgresOwner(backend, store.requireCurrentSchema)
	if err != nil {
		return nil, err
	}
	operatorAgent, err := storeoperatorsurface.NewAgentPostgres(backend, store.requireCurrentSchema, operatorObservability)
	if err != nil {
		return nil, err
	}
	store.operatorAgentPostgres = operatorAgent
	store.operatorAgentMethods = operatorAgent
	runLifecycle, err := storerunlifecycle.NewPostgres(backend, store.requireCurrentSchema, candidates)
	if err != nil {
		return nil, err
	}
	llmOwner, err := storellm.NewPostgres(backend, store.requireCurrentSchema, runLifecycle, candidates)
	if err != nil {
		return nil, err
	}
	store.lLMPostgresOwner = llmOwner
	store.llmMethods = llmOwner
	effectOwner, err := storeeffect.NewPostgres(backend, store.requireCurrentSchema, runLifecycle, candidates, llmOwner)
	if err != nil {
		return nil, err
	}
	store.effectPostgresOwner = effectOwner
	store.effectOwnerMethods = effectOwner
	if err := effectOwner.BindChannelReconciliation(&store.channelChanges); err != nil {
		return nil, err
	}
	if err := runLifecycle.BindExecutionAuthority(effectOwner); err != nil {
		return nil, err
	}
	genericSchedules, err := storegenericschedule.NewPostgres(backend, store.requireCurrentSchema, runLifecycle)
	if err != nil {
		return nil, err
	}
	store.genericSchedulePostgresOwner = genericSchedules
	deliveryOwner, err := storedelivery.NewDeliveryPostgresOwner(deadLetterOwner, runLifecycle, candidates, agentOwner)
	if err != nil {
		return nil, err
	}
	if err := effectOwner.BindProviderDrainDelivery(deliveryOwner); err != nil {
		return nil, err
	}
	if err := agentOwner.BindProviderAttemptDrains(effectOwner); err != nil {
		return nil, err
	}
	decisionOwner, err := storedecision.NewPostgres(backend, store.requireCurrentSchema, runLifecycle, candidates)
	if err != nil {
		return nil, err
	}
	eventOwner, err := storeevent.NewPostgres(backend, store.requireCurrentSchema, activityJournal, runLifecycle, deliveryOwner, replyContexts, apiIdempotency, durableData)
	if err != nil {
		return nil, err
	}
	pipelineOwner, err := storepipeline.NewPostgres(backend, store.requireCurrentSchema, runLifecycle, candidates, decisionOwner, deliveryOwner, replyContexts, workflowEntityQueries, workflowRoutes, eventOwner, apiIdempotency, durableData)
	if err != nil {
		return nil, err
	}
	if err := pipelineOwner.BindInstanceLifeLifecycle(agentOwner); err != nil {
		return nil, err
	}
	if err := pipelineOwner.BindChannelReconciliation(&store.channelChanges); err != nil {
		return nil, err
	}
	if err := pipelineOwner.BindGenericScheduleTxOwner(genericSchedules); err != nil {
		return nil, err
	}
	if err := eventOwner.BindPipeline(pipelineOwner); err != nil {
		return nil, err
	}
	if err := eventOwner.BindOperatorChannelClaims(operatorChannels); err != nil {
		return nil, err
	}
	if err := runLifecycle.BindDelivery(deliveryOwner); err != nil {
		return nil, err
	}
	if err := runLifecycle.BindPipeline(pipelineOwner); err != nil {
		return nil, err
	}
	if err := runLifecycle.BindDecisionCards(decisionOwner); err != nil {
		return nil, err
	}
	store.deliveryPostgresOwner = deliveryOwner
	store.deliveryOwnerMethods = deliveryOwner
	store.pipelinePostgresOwner = pipelineOwner
	store.pipelineOwnerMethods = pipelineOwner
	if err := pipelineOwner.BindWorkflowTurnTermination(effectOwner); err != nil {
		return nil, err
	}
	store.eventPostgresOwner = eventOwner
	store.eventOwnerMethods = eventOwner
	if err := eventOwner.BindChannelReconciliation(&store.channelChanges); err != nil {
		return nil, err
	}
	store.decisionPostgresOwner = decisionOwner
	store.decisionOwnerMethods = decisionOwner
	if err := decisionOwner.BindChannelReconciliation(&store.channelChanges); err != nil {
		return nil, err
	}
	store.runLifecyclePostgresOwner = runLifecycle
	store.runLifecycleOwnerMethods = runLifecycle
	if err := runLifecycle.BindChannelReconciliation(&store.channelChanges); err != nil {
		return nil, err
	}
	operatorRun, err := storeoperatorsurface.NewRunPostgres(backend, store.requireCurrentSchema, pipelineOwner, timerObligations, operatorObservability, sourceArtifacts)
	if err != nil {
		return nil, err
	}
	store.operatorRunPostgres = operatorRun
	store.operatorRunMethods = operatorRun
	if err := agentOwner.BindDirectiveDependencies(eventOwner, pipelineOwner); err != nil {
		return nil, err
	}
	if err := deliveryOwner.BindReceiverTargetPersistence(pipelineOwner); err != nil {
		return nil, err
	}
	if err := effectOwner.BindProviderDrainDirectives(agentOwner); err != nil {
		return nil, err
	}
	if err := effectOwner.BindCanceledTurnPublication(eventOwner); err != nil {
		return nil, err
	}
	if err := pipelineOwner.BindSelectedForkWriter(eventOwner); err != nil {
		return nil, err
	}
	runForkOwner, err := storerunfork.NewPostgres(backend, store.requireCurrentSchema, runLifecycle, decisionOwner, deliveryOwner, effectOwner, pipelineOwner, eventOwner, operatorConversation, replyContexts, durableData, apiIdempotency, candidates)
	if err != nil {
		return nil, err
	}
	store.runForkPostgresOwner = runForkOwner
	store.runForkOwnerMethods = runForkOwner
	if err := runForkOwner.BindChannelReconciliation(&store.channelChanges); err != nil {
		return nil, err
	}
	if err := pipelineOwner.BindHistoricalFlowInstanceReader(runForkOwner); err != nil {
		return nil, err
	}
	if err := agentOwner.BindLifecycleDiagnostics(eventOwner, runForkOwner); err != nil {
		return nil, err
	}
	if err := runForkOwner.BindLifecycleDiagnostics(eventOwner); err != nil {
		return nil, err
	}
	if err := eventOwner.BindRunFork(runForkOwner); err != nil {
		return nil, err
	}
	startupOwner, err := storestartupownership.NewPostgres(backend, store.requireCurrentSchema, schemaOwner.CatalogEmpty, agentOwner, destructiveResetOwner, pipelineOwner)
	if err != nil {
		return nil, err
	}
	if err := startupOwner.BindLifecycleCandidates(candidates); err != nil {
		return nil, err
	}
	if err := startupOwner.BindActivityJournalRole(activityJournal); err != nil {
		return nil, err
	}
	store.startupPostgresOwner = startupOwner
	store.startupOwnerMethods = startupOwner
	if err := effectOwner.BindPreparedProcessOwner(startupOwner); err != nil {
		return nil, err
	}
	if err := runForkOwner.BindPreparedProcessOwner(startupOwner); err != nil {
		return nil, err
	}
	if err := startupOwner.BindChannelReconciliation(&store.channelChanges); err != nil {
		return nil, err
	}
	ingressOwner, err := storeingress.NewPostgres(backend, store.requireCurrentSchema)
	if err != nil {
		return nil, err
	}
	store.runtimeIngressPostgresOwner = ingressOwner
	store.runtimeIngressOwnerMethods = ingressOwner
	return store, nil
}

// ComposePostgresStore is the exact process-construction entrypoint. Runtime
// consumers receive only the resulting typed facade.
func ComposePostgresStore(backend *postgresbackend.Backend) (*PostgresStore, error) {
	return newPostgresStoreComposition(backend)
}

func newPostgresStoreWithBackend(backend *postgresbackend.Backend) *PostgresStore {
	store, err := newPostgresStoreComposition(backend)
	if err != nil {
		panic(err)
	}
	return store
}

func newSQLiteStoreComposition(schema *SQLiteSchemaStore, backend *sqlitebackend.Backend, backendIdentity *storestartupownership.SQLiteBackendIdentity) (*SQLiteRuntimeStore, error) {
	if schema == nil {
		return nil, fmt.Errorf("sqlite schema store is required")
	}
	if backend == nil || !backend.Valid() {
		return nil, fmt.Errorf("sqlite backend is required")
	}
	runBundles, err := storerunbundle.NewSQLite(backend)
	if err != nil {
		return nil, err
	}
	timerObligations, err := storetimerobligation.NewSQLite(backend, schema.RequireCurrent)
	if err != nil {
		return nil, err
	}
	workflowEntityQueries, err := storeworkflowentityquery.NewSQLite(backend)
	if err != nil {
		return nil, err
	}
	workflowRoutes, err := storeworkflowroute.NewSQLite(backend)
	if err != nil {
		return nil, err
	}
	candidates := storerunhandoff.NewCandidateCoordinator()
	store := &SQLiteRuntimeStore{
		schema:                 schema,
		backend:                backend,
		runBundles:             runBundles,
		runLifecycleCandidates: candidates,
		workflowEntityQueries:  workflowEntityQueries,
		workflowRoutes:         workflowRoutes,
		nowFn:                  time.Now,
	}
	store.timerObligationSQLiteReader = timerObligations
	store.timerObligationReaderMethods = timerObligations
	destructiveReset, err := storeadmin.NewDestructiveResetSQLite(backend, store.requireCurrentSchema)
	if err != nil {
		return nil, err
	}
	store.destructiveResetSQLiteOwner = destructiveReset
	store.destructiveResetSQLiteOwnerMethods = destructiveReset
	apiIdempotency, err := storeapiidempotency.NewSQLite(backend, schema.Path(), store.requireCurrentSchema)
	if err != nil {
		return nil, err
	}
	store.sQLiteOwner = apiIdempotency
	store.apiIdempotencyMethods = apiIdempotency
	operatorChannels, err := storeoperatorchannel.NewSQLite(backend, store.requireCurrentSchema)
	if err != nil {
		return nil, err
	}
	store.operatorChannelSQLiteOwner = operatorChannels
	store.operatorChannelOwnerMethods = operatorChannels
	if err := operatorChannels.BindChannelReconciliation(&store.channelChanges); err != nil {
		return nil, err
	}
	channelOnboarding, err := storechannelonboarding.NewSQLite(backend, store.requireCurrentSchema)
	if err != nil {
		return nil, err
	}
	store.channelOnboardingSQLiteOwner = channelOnboarding
	store.channelOnboardingOwnerMethods = channelOnboarding
	if err := channelOnboarding.BindChannelReconciliation(&store.channelChanges); err != nil {
		return nil, err
	}
	sourceArtifacts, err := storesourceartifact.NewSQLite(backend, store.requireCurrentSchema, store.now)
	if err != nil {
		return nil, err
	}
	store.sourceArtifactOwner = sourceArtifacts
	durableData, err := storedurabledata.NewSQLite(backend, store.requireCurrentSchema, store.now)
	if err != nil {
		return nil, err
	}
	store.durableDataOwner = durableData
	operatorObservability, err := storeoperatorsurface.NewObservabilitySQLite(backend, store.requireCurrentSchema)
	if err != nil {
		return nil, err
	}
	store.operatorObservabilitySQLite = operatorObservability
	store.operatorObservabilityMethods = operatorObservability
	operatorConversation, err := storeoperatorsurface.NewConversationSQLite(backend, store.requireCurrentSchema)
	if err != nil {
		return nil, err
	}
	store.operatorConversationSQLite = operatorConversation
	store.operatorConversationMethods = operatorConversation
	operatorEntity, err := storeoperatorsurface.NewEntitySQLite(backend, store.requireCurrentSchema)
	if err != nil {
		return nil, err
	}
	store.operatorEntitySQLite = operatorEntity
	store.operatorEntityMethods = operatorEntity
	activityJournal, err := storeactivityjournal.NewSQLite(backend, store.requireCurrentSchema)
	if err != nil {
		return nil, err
	}
	store.activitySQLiteOwner = activityJournal
	store.activityOwnerMethods = activityJournal
	if err := activityJournal.BindChannelReconciliation(&store.channelChanges); err != nil {
		return nil, err
	}
	activityResult, err := storeactivityresult.NewSQLite(backend, store.requireCurrentSchema)
	if err != nil {
		return nil, err
	}
	store.activityResultSQLiteOwner = activityResult
	store.activityResultOwnerMethods = activityResult
	replyContexts, err := storereplycontext.NewSQLite(backend)
	if err != nil {
		return nil, err
	}
	store.replySQLiteOwner = replyContexts
	store.replyOwnerMethods = replyContexts
	managedCapabilities, err := storemanagedcapability.NewSQLite(backend)
	if err != nil {
		return nil, err
	}
	store.managedCapabilitySQLiteOwner = managedCapabilities
	store.managedCapabilityOwnerMethods = managedCapabilities
	mailbox, err := storemailbox.NewSQLite(backend, store.requireCurrentSchema, apiIdempotency)
	if err != nil {
		return nil, err
	}
	store.mailboxSQLiteOwner = mailbox
	store.mailboxOwnerMethods = mailbox
	if err := mailbox.BindChannelReconciliation(&store.channelChanges); err != nil {
		return nil, err
	}
	budgetSpend, err := storebudgetspend.NewSQLite(backend)
	if err != nil {
		return nil, err
	}
	store.budgetSQLiteOwner = budgetSpend
	store.budgetOwnerMethods = budgetSpend
	agentOwner, err := storeagent.NewSQLite(backend, store.requireCurrentSchema, store)
	if err != nil {
		return nil, err
	}
	store.agentSQLiteOwner = agentOwner
	store.agentSQLiteOwnerMethods = agentOwner
	entityOwner, err := storeentity.NewSQLite(backend)
	if err != nil {
		return nil, err
	}
	store.entitySQLiteOwner = entityOwner
	store.entityOwnerMethods = entityOwner
	deadLetterOwner, err := storedelivery.NewDeadLetterSQLiteOwner(backend, store.requireCurrentSchema)
	if err != nil {
		return nil, err
	}
	operatorAgent, err := storeoperatorsurface.NewAgentSQLite(backend, store.requireCurrentSchema, operatorObservability)
	if err != nil {
		return nil, err
	}
	store.operatorAgentSQLite = operatorAgent
	store.operatorAgentSQLiteMethods = operatorAgent
	runLifecycle, err := storerunlifecycle.NewSQLite(backend, store.requireCurrentSchema, candidates, store.now)
	if err != nil {
		return nil, err
	}
	llmOwner, err := storellm.NewSQLite(backend, store.requireCurrentSchema, runLifecycle, candidates, store.now)
	if err != nil {
		return nil, err
	}
	store.lLMSQLiteOwner = llmOwner
	store.llmMethods = llmOwner
	effectOwner, err := storeeffect.NewSQLite(backend, store.requireCurrentSchema, runLifecycle, candidates, llmOwner)
	if err != nil {
		return nil, err
	}
	store.effectSQLiteOwner = effectOwner
	store.effectOwnerMethods = effectOwner
	if err := effectOwner.BindChannelReconciliation(&store.channelChanges); err != nil {
		return nil, err
	}
	if err := runLifecycle.BindExecutionAuthority(effectOwner); err != nil {
		return nil, err
	}
	genericSchedules, err := storegenericschedule.NewSQLite(backend, store.requireCurrentSchema, runLifecycle)
	if err != nil {
		return nil, err
	}
	store.genericScheduleSQLiteOwner = genericSchedules
	deliveryOwner, err := storedelivery.NewDeliverySQLiteOwner(deadLetterOwner, runLifecycle, candidates, agentOwner, store.now)
	if err != nil {
		return nil, err
	}
	if err := effectOwner.BindProviderDrainDelivery(deliveryOwner); err != nil {
		return nil, err
	}
	if err := agentOwner.BindProviderAttemptDrains(effectOwner); err != nil {
		return nil, err
	}
	decisionOwner, err := storedecision.NewSQLite(backend, store.requireCurrentSchema, runLifecycle, candidates, store.now)
	if err != nil {
		return nil, err
	}
	eventOwner, err := storeevent.NewSQLite(backend, store.requireCurrentSchema, activityJournal, runLifecycle, deliveryOwner, replyContexts, apiIdempotency, durableData, store.now)
	if err != nil {
		return nil, err
	}
	pipelineOwner, err := storepipeline.NewSQLite(backend, store.requireCurrentSchema, runLifecycle, candidates, decisionOwner, deliveryOwner, replyContexts, workflowEntityQueries, workflowRoutes, eventOwner, apiIdempotency, durableData, store.now)
	if err != nil {
		return nil, err
	}
	if err := pipelineOwner.BindInstanceLifeLifecycle(agentOwner); err != nil {
		return nil, err
	}
	if err := pipelineOwner.BindChannelReconciliation(&store.channelChanges); err != nil {
		return nil, err
	}
	startupOwner, err := storestartupownership.NewSQLiteWithBackendIdentity(backend, schema.Path(), backendIdentity, store.requireCurrentSchema, schema.CatalogEmpty, agentOwner, pipelineOwner)
	if err != nil {
		return nil, err
	}
	if err := startupOwner.BindLifecycleCandidates(candidates); err != nil {
		return nil, err
	}
	if err := startupOwner.BindActivityJournalRole(activityJournal); err != nil {
		return nil, err
	}
	store.startupSQLiteOwner = startupOwner
	store.startupOwnerMethods = startupOwner
	if err := startupOwner.BindChannelReconciliation(&store.channelChanges); err != nil {
		return nil, err
	}
	if err := pipelineOwner.BindGenericScheduleTxOwner(genericSchedules); err != nil {
		return nil, err
	}
	if err := eventOwner.BindPipeline(pipelineOwner); err != nil {
		return nil, err
	}
	if err := eventOwner.BindOperatorChannelClaims(operatorChannels); err != nil {
		return nil, err
	}
	if err := runLifecycle.BindDelivery(deliveryOwner); err != nil {
		return nil, err
	}
	if err := runLifecycle.BindPipeline(pipelineOwner); err != nil {
		return nil, err
	}
	if err := runLifecycle.BindDecisionCards(decisionOwner); err != nil {
		return nil, err
	}
	store.deliverySQLiteOwner = deliveryOwner
	store.deliveryOwnerMethods = deliveryOwner
	store.pipelineSQLiteOwner = pipelineOwner
	store.pipelineOwnerMethods = pipelineOwner
	if err := pipelineOwner.BindWorkflowTurnTermination(effectOwner); err != nil {
		return nil, err
	}
	store.eventSQLiteOwner = eventOwner
	store.eventSQLiteOwnerMethods = eventOwner
	if err := eventOwner.BindChannelReconciliation(&store.channelChanges); err != nil {
		return nil, err
	}
	store.decisionSQLiteOwner = decisionOwner
	store.decisionOwnerMethods = decisionOwner
	if err := decisionOwner.BindChannelReconciliation(&store.channelChanges); err != nil {
		return nil, err
	}
	store.runLifecycleSQLiteOwner = runLifecycle
	store.runLifecycleSQLiteOwnerMethods = runLifecycle
	if err := runLifecycle.BindChannelReconciliation(&store.channelChanges); err != nil {
		return nil, err
	}
	operatorRun, err := storeoperatorsurface.NewRunSQLite(backend, store.requireCurrentSchema, store.now, pipelineOwner, timerObligations, operatorObservability, sourceArtifacts)
	if err != nil {
		return nil, err
	}
	store.operatorRunSQLite = operatorRun
	store.operatorRunSQLiteMethods = operatorRun
	if err := agentOwner.BindDirectiveDependencies(eventOwner, pipelineOwner); err != nil {
		return nil, err
	}
	if err := deliveryOwner.BindReceiverTargetPersistence(pipelineOwner); err != nil {
		return nil, err
	}
	if err := effectOwner.BindProviderDrainDirectives(agentOwner); err != nil {
		return nil, err
	}
	if err := effectOwner.BindCanceledTurnPublication(eventOwner); err != nil {
		return nil, err
	}
	if err := pipelineOwner.BindSelectedForkWriter(eventOwner); err != nil {
		return nil, err
	}
	runForkOwner, err := storerunfork.NewSQLite(backend, store.requireCurrentSchema, runLifecycle, decisionOwner, deliveryOwner, effectOwner, pipelineOwner, eventOwner, operatorConversation, replyContexts, durableData, apiIdempotency, candidates, store.now)
	if err != nil {
		return nil, err
	}
	store.runForkSQLiteOwner = runForkOwner
	store.runForkOwnerMethods = runForkOwner
	if err := effectOwner.BindPreparedProcessOwner(startupOwner); err != nil {
		return nil, err
	}
	if err := runForkOwner.BindPreparedProcessOwner(startupOwner); err != nil {
		return nil, err
	}
	if err := runForkOwner.BindChannelReconciliation(&store.channelChanges); err != nil {
		return nil, err
	}
	if err := pipelineOwner.BindHistoricalFlowInstanceReader(runForkOwner); err != nil {
		return nil, err
	}
	if err := agentOwner.BindLifecycleDiagnostics(eventOwner, runForkOwner); err != nil {
		return nil, err
	}
	if err := runForkOwner.BindLifecycleDiagnostics(eventOwner); err != nil {
		return nil, err
	}
	if err := eventOwner.BindRunFork(runForkOwner); err != nil {
		return nil, err
	}
	ingressOwner, err := storeingress.NewSQLite(backend, store.requireCurrentSchema)
	if err != nil {
		return nil, err
	}
	store.runtimeIngressSQLiteOwner = ingressOwner
	store.runtimeIngressOwnerMethods = ingressOwner
	return store, nil
}

// ComposeSQLiteRuntimeStore is reserved for read-only inspection and explicit
// test construction. Mutable process construction must bind the backend file
// identity through ComposeSQLiteRuntimeStoreWithBackendIdentity.
func ComposeSQLiteRuntimeStore(schema *SQLiteSchemaStore, backend *sqlitebackend.Backend) (*SQLiteRuntimeStore, error) {
	return newSQLiteStoreComposition(schema, backend, nil)
}

// ComposeSQLiteRuntimeStoreWithBackendIdentity is the exact mutable process-
// construction entrypoint. Runtime consumers receive only the typed facade.
func ComposeSQLiteRuntimeStoreWithBackendIdentity(schema *SQLiteSchemaStore, backend *sqlitebackend.Backend, identity *storestartupownership.SQLiteBackendIdentity) (*SQLiteRuntimeStore, error) {
	if identity == nil {
		return nil, fmt.Errorf("SQLite backend identity is required")
	}
	return newSQLiteStoreComposition(schema, backend, identity)
}
