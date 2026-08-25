package runtimepersistence

import (
	"fmt"
	"time"

	storeadmin "github.com/division-sh/swarm/internal/store/internal/adminpersistence"
	storeapiidempotency "github.com/division-sh/swarm/internal/store/internal/apiidempotency"
	storeactivityjournal "github.com/division-sh/swarm/internal/store/internal/backend/activityjournal"
	storeactivityresult "github.com/division-sh/swarm/internal/store/internal/backend/activityresult"
	storeagent "github.com/division-sh/swarm/internal/store/internal/backend/agentpersistence"
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
	storebundlecatalog "github.com/division-sh/swarm/internal/store/internal/bundlecatalog"
	storeingress "github.com/division-sh/swarm/internal/store/internal/ingresspersistence"
	storemailbox "github.com/division-sh/swarm/internal/store/internal/mailboxpersistence"
	storeoperatorsurface "github.com/division-sh/swarm/internal/store/internal/operatorsurface"
	storepreservation "github.com/division-sh/swarm/internal/store/internal/preservationpersistence"
	storeroutingrules "github.com/division-sh/swarm/internal/store/internal/routingrules"
	storerunbundle "github.com/division-sh/swarm/internal/store/internal/runbundle"
	storerunhandoff "github.com/division-sh/swarm/internal/store/internal/runhandoff"
	storeschema "github.com/division-sh/swarm/internal/store/internal/schemastore"
	storestartupownership "github.com/division-sh/swarm/internal/store/internal/startupownership"
	storeworkflowentityquery "github.com/division-sh/swarm/internal/store/internal/workflowentityquery"
	storeworkflowroute "github.com/division-sh/swarm/internal/store/internal/workflowroute"
	storeworkspace "github.com/division-sh/swarm/internal/store/internal/workspace"
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
	workspaceLookups, err := storeworkspace.NewPostgres(backend)
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
		workspaceLookups:       workspaceLookups,
		schemaOwner:            schemaOwner,
	}
	store.timerObligationPostgresReader = timerObligations
	genericSchedules, err := storegenericschedule.NewPostgres(backend, store.requireCurrentSchema)
	if err != nil {
		return nil, err
	}
	store.genericSchedulePostgresOwner = genericSchedules
	apiIdempotency, err := storeapiidempotency.NewPostgres(backend, store.requireCurrentSchema)
	if err != nil {
		return nil, err
	}
	store.postgresOwner = apiIdempotency
	operatorChannels, err := storeoperatorchannel.NewPostgres(backend, store.requireCurrentSchema)
	if err != nil {
		return nil, err
	}
	store.operatorChannelPostgresOwner = operatorChannels
	bundleCatalog, err := storebundlecatalog.NewPostgres(backend, store.requireCurrentSchema)
	if err != nil {
		return nil, err
	}
	store.postgres = bundleCatalog
	operatorObservability, err := storeoperatorsurface.NewObservabilityPostgres(backend, store.requireCurrentSchema)
	if err != nil {
		return nil, err
	}
	store.operatorObservabilityPostgres = operatorObservability
	operatorConversation, err := storeoperatorsurface.NewConversationPostgres(backend, store.requireCurrentSchema)
	if err != nil {
		return nil, err
	}
	store.operatorConversationPostgres = operatorConversation
	operatorEntity, err := storeoperatorsurface.NewEntityPostgres(backend, store.requireCurrentSchema)
	if err != nil {
		return nil, err
	}
	store.operatorEntityPostgres = operatorEntity
	routingRules, err := storeroutingrules.NewPostgres(backend)
	if err != nil {
		return nil, err
	}
	store.routingPostgresOwner = routingRules
	replyContexts, err := storereplycontext.NewPostgres(backend)
	if err != nil {
		return nil, err
	}
	store.replyPostgresOwner = replyContexts
	managedCapabilities, err := storemanagedcapability.NewPostgres(backend)
	if err != nil {
		return nil, err
	}
	store.managedCapabilityPostgresOwner = managedCapabilities
	mailbox, err := storemailbox.NewPostgres(backend, store.requireCurrentSchema)
	if err != nil {
		return nil, err
	}
	store.mailboxPostgresOwner = mailbox
	bundleDeleteOwner, err := storeadmin.NewBundleDeletePostgres(backend, store.requireCurrentSchema)
	if err != nil {
		return nil, err
	}
	store.bundleDeletePostgresOwner = bundleDeleteOwner
	destructiveResetOwner, err := storeadmin.NewDestructiveResetPostgres(backend, store.requireCurrentSchema)
	if err != nil {
		return nil, err
	}
	store.destructiveResetPostgresOwner = destructiveResetOwner
	activityJournal, err := storeactivityjournal.NewPostgres(backend, store.requireCurrentSchema)
	if err != nil {
		return nil, err
	}
	store.activityPostgresOwner = activityJournal
	activityResult, err := storeactivityresult.NewPostgres(backend, store.requireCurrentSchema)
	if err != nil {
		return nil, err
	}
	store.activityResultPostgresOwner = activityResult
	budgetSpend, err := storebudgetspend.NewPostgres(backend)
	if err != nil {
		return nil, err
	}
	store.budgetPostgresOwner = budgetSpend
	agentOwner, err := storeagent.NewPostgres(backend, store.requireCurrentSchema, store)
	if err != nil {
		return nil, err
	}
	store.agentPostgresOwner = agentOwner
	entityOwner, err := storeentity.NewPostgres(backend, store.requireCurrentSchema)
	if err != nil {
		return nil, err
	}
	store.entityPostgresOwner = entityOwner
	deadLetterOwner, err := storedelivery.NewDeadLetterPostgresOwner(backend, store.requireCurrentSchema)
	if err != nil {
		return nil, err
	}
	operatorAgent, err := storeoperatorsurface.NewAgentPostgres(backend, store.requireCurrentSchema, operatorObservability)
	if err != nil {
		return nil, err
	}
	store.operatorAgentPostgres = operatorAgent
	runLifecycle, err := storerunlifecycle.NewPostgres(backend, store.requireCurrentSchema, candidates)
	if err != nil {
		return nil, err
	}
	llmOwner, err := storellm.NewPostgres(backend, store.requireCurrentSchema, runLifecycle, candidates)
	if err != nil {
		return nil, err
	}
	store.lLMPostgresOwner = llmOwner
	effectOwner, err := storeeffect.NewPostgres(backend, store.requireCurrentSchema, runLifecycle, llmOwner)
	if err != nil {
		return nil, err
	}
	store.effectPostgresOwner = effectOwner
	deliveryOwner, err := storedelivery.NewDeliveryPostgresOwner(deadLetterOwner, runLifecycle)
	if err != nil {
		return nil, err
	}
	if err := effectOwner.BindProviderDrainDelivery(deliveryOwner); err != nil {
		return nil, err
	}
	if err := agentOwner.BindProviderAttemptDrains(effectOwner); err != nil {
		return nil, err
	}
	decisionOwner, err := storedecision.NewPostgres(backend, store.requireCurrentSchema, runLifecycle)
	if err != nil {
		return nil, err
	}
	eventOwner, err := storeevent.NewPostgres(backend, store.requireCurrentSchema, activityJournal, runLifecycle, deliveryOwner, replyContexts, apiIdempotency)
	if err != nil {
		return nil, err
	}
	pipelineOwner, err := storepipeline.NewPostgres(backend, store.requireCurrentSchema, runLifecycle, candidates, decisionOwner, deliveryOwner, replyContexts, workflowEntityQueries, workflowRoutes, eventOwner)
	if err != nil {
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
	store.pipelinePostgresOwner = pipelineOwner
	store.eventPostgresOwner = eventOwner
	store.decisionPostgresOwner = decisionOwner
	store.runLifecyclePostgresOwner = runLifecycle
	operatorRun, err := storeoperatorsurface.NewRunPostgres(backend, store.requireCurrentSchema, pipelineOwner, timerObligations, operatorObservability)
	if err != nil {
		return nil, err
	}
	store.operatorRunPostgres = operatorRun
	if err := agentOwner.BindDirectiveDependencies(eventOwner, pipelineOwner); err != nil {
		return nil, err
	}
	if err := effectOwner.BindProviderDrainDirectives(agentOwner); err != nil {
		return nil, err
	}
	if err := pipelineOwner.BindSelectedForkWriter(eventOwner); err != nil {
		return nil, err
	}
	runForkOwner, err := storerunfork.NewPostgres(backend, store.requireCurrentSchema, runLifecycle, decisionOwner, deliveryOwner, effectOwner, pipelineOwner, eventOwner, operatorConversation)
	if err != nil {
		return nil, err
	}
	store.runForkPostgresOwner = runForkOwner
	if err := eventOwner.BindRunFork(runForkOwner); err != nil {
		return nil, err
	}
	preservationOwner, err := storepreservation.NewPostgres(backend, store.requireCurrentSchema, deliveryOwner, pipelineOwner, runLifecycle, runForkOwner)
	if err != nil {
		return nil, err
	}
	store.preservationPostgresOwner = preservationOwner
	startupOwner, err := storestartupownership.NewPostgres(backend, store.requireCurrentSchema, schemaOwner.CatalogEmpty, agentOwner, bundleDeleteOwner, destructiveResetOwner)
	if err != nil {
		return nil, err
	}
	store.startupPostgresOwner = startupOwner
	ingressOwner, err := storeingress.NewPostgres(backend, store.requireCurrentSchema)
	if err != nil {
		return nil, err
	}
	store.runtimeIngressPostgresOwner = ingressOwner
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
	workspaceLookups, err := storeworkspace.NewSQLite(backend)
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
		workspaceLookups:       workspaceLookups,
		nowFn:                  time.Now,
	}
	store.timerObligationSQLiteReader = timerObligations
	genericSchedules, err := storegenericschedule.NewSQLite(backend, store.requireCurrentSchema)
	if err != nil {
		return nil, err
	}
	store.genericScheduleSQLiteOwner = genericSchedules
	apiIdempotency, err := storeapiidempotency.NewSQLite(backend, schema.Path(), store.requireCurrentSchema)
	if err != nil {
		return nil, err
	}
	store.sQLiteOwner = apiIdempotency
	operatorChannels, err := storeoperatorchannel.NewSQLite(backend, store.requireCurrentSchema)
	if err != nil {
		return nil, err
	}
	store.operatorChannelSQLiteOwner = operatorChannels
	bundleCatalog, err := storebundlecatalog.NewSQLite(backend, store.requireCurrentSchema)
	if err != nil {
		return nil, err
	}
	store.sQLite = bundleCatalog
	operatorObservability, err := storeoperatorsurface.NewObservabilitySQLite(backend, store.requireCurrentSchema)
	if err != nil {
		return nil, err
	}
	store.operatorObservabilitySQLite = operatorObservability
	operatorConversation, err := storeoperatorsurface.NewConversationSQLite(backend, store.requireCurrentSchema)
	if err != nil {
		return nil, err
	}
	store.operatorConversationSQLite = operatorConversation
	operatorEntity, err := storeoperatorsurface.NewEntitySQLite(backend, store.requireCurrentSchema)
	if err != nil {
		return nil, err
	}
	store.operatorEntitySQLite = operatorEntity
	activityJournal, err := storeactivityjournal.NewSQLite(backend, store.requireCurrentSchema)
	if err != nil {
		return nil, err
	}
	store.activitySQLiteOwner = activityJournal
	activityResult, err := storeactivityresult.NewSQLite(backend, store.requireCurrentSchema)
	if err != nil {
		return nil, err
	}
	store.activityResultSQLiteOwner = activityResult
	replyContexts, err := storereplycontext.NewSQLite(backend)
	if err != nil {
		return nil, err
	}
	store.replySQLiteOwner = replyContexts
	managedCapabilities, err := storemanagedcapability.NewSQLite(backend)
	if err != nil {
		return nil, err
	}
	store.managedCapabilitySQLiteOwner = managedCapabilities
	mailbox, err := storemailbox.NewSQLite(backend, store.requireCurrentSchema)
	if err != nil {
		return nil, err
	}
	store.mailboxSQLiteOwner = mailbox
	budgetSpend, err := storebudgetspend.NewSQLite(backend)
	if err != nil {
		return nil, err
	}
	store.budgetSQLiteOwner = budgetSpend
	agentOwner, err := storeagent.NewSQLite(backend, store.requireCurrentSchema, store)
	if err != nil {
		return nil, err
	}
	store.agentSQLiteOwner = agentOwner
	entityOwner, err := storeentity.NewSQLite(backend, store.requireCurrentSchema, store.now)
	if err != nil {
		return nil, err
	}
	store.entitySQLiteOwner = entityOwner
	deadLetterOwner, err := storedelivery.NewDeadLetterSQLiteOwner(backend, store.requireCurrentSchema)
	if err != nil {
		return nil, err
	}
	operatorAgent, err := storeoperatorsurface.NewAgentSQLite(backend, store.requireCurrentSchema, operatorObservability)
	if err != nil {
		return nil, err
	}
	store.operatorAgentSQLite = operatorAgent
	runLifecycle, err := storerunlifecycle.NewSQLite(backend, store.requireCurrentSchema, candidates, store.now)
	if err != nil {
		return nil, err
	}
	llmOwner, err := storellm.NewSQLite(backend, store.requireCurrentSchema, runLifecycle, candidates, store.now)
	if err != nil {
		return nil, err
	}
	store.lLMSQLiteOwner = llmOwner
	startupOwner, err := storestartupownership.NewSQLiteWithBackendIdentity(backend, schema.Path(), backendIdentity, store.requireCurrentSchema, schema.CatalogEmpty, agentOwner)
	if err != nil {
		return nil, err
	}
	store.startupSQLiteOwner = startupOwner
	effectOwner, err := storeeffect.NewSQLite(backend, store.requireCurrentSchema, runLifecycle, llmOwner)
	if err != nil {
		return nil, err
	}
	store.effectSQLiteOwner = effectOwner
	deliveryOwner, err := storedelivery.NewDeliverySQLiteOwner(deadLetterOwner, runLifecycle, store.now)
	if err != nil {
		return nil, err
	}
	if err := effectOwner.BindProviderDrainDelivery(deliveryOwner); err != nil {
		return nil, err
	}
	if err := agentOwner.BindProviderAttemptDrains(effectOwner); err != nil {
		return nil, err
	}
	decisionOwner, err := storedecision.NewSQLite(backend, store.requireCurrentSchema, runLifecycle, store.now)
	if err != nil {
		return nil, err
	}
	eventOwner, err := storeevent.NewSQLite(backend, store.requireCurrentSchema, activityJournal, runLifecycle, deliveryOwner, replyContexts, apiIdempotency, store.now)
	if err != nil {
		return nil, err
	}
	pipelineOwner, err := storepipeline.NewSQLite(backend, store.requireCurrentSchema, runLifecycle, candidates, decisionOwner, deliveryOwner, replyContexts, workflowEntityQueries, workflowRoutes, eventOwner, store.now)
	if err != nil {
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
	store.pipelineSQLiteOwner = pipelineOwner
	store.eventSQLiteOwner = eventOwner
	store.decisionSQLiteOwner = decisionOwner
	store.runLifecycleSQLiteOwner = runLifecycle
	operatorRun, err := storeoperatorsurface.NewRunSQLite(backend, store.requireCurrentSchema, store.now, pipelineOwner, timerObligations, operatorObservability)
	if err != nil {
		return nil, err
	}
	store.operatorRunSQLite = operatorRun
	if err := agentOwner.BindDirectiveDependencies(eventOwner, pipelineOwner); err != nil {
		return nil, err
	}
	if err := effectOwner.BindProviderDrainDirectives(agentOwner); err != nil {
		return nil, err
	}
	if err := pipelineOwner.BindSelectedForkWriter(eventOwner); err != nil {
		return nil, err
	}
	runForkOwner, err := storerunfork.NewSQLite(backend, store.requireCurrentSchema, runLifecycle, decisionOwner, deliveryOwner, effectOwner, pipelineOwner, eventOwner, operatorConversation, store.now)
	if err != nil {
		return nil, err
	}
	store.runForkSQLiteOwner = runForkOwner
	if err := eventOwner.BindRunFork(runForkOwner); err != nil {
		return nil, err
	}
	ingressOwner, err := storeingress.NewSQLite(backend, store.requireCurrentSchema)
	if err != nil {
		return nil, err
	}
	store.runtimeIngressSQLiteOwner = ingressOwner
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
