package serveapp

import (
	"fmt"
	"reflect"
	"sort"
	"strings"

	apiv1 "github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/budgetspend"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimeingress "github.com/division-sh/swarm/internal/runtime/ingress"
	runtimellm "github.com/division-sh/swarm/internal/runtime/llm"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimerunquiescence "github.com/division-sh/swarm/internal/runtime/runquiescence"
	runtimestartupownership "github.com/division-sh/swarm/internal/runtime/startupownership"
	runtimetools "github.com/division-sh/swarm/internal/runtime/tools"
	"github.com/division-sh/swarm/internal/store"
	storebackend "github.com/division-sh/swarm/internal/store/backendselection"
)

// selectedConcreteRuntimeStore is the compile-complete shared role set that the
// selected Postgres and SQLite runtime stores must both implement. Selected
// construction still wraps non-store roles such as pipeline and Postgres
// sessions, but those wrappers are validated by selectedStoreBundleRoleLedger.
type selectedConcreteRuntimeStore interface {
	apiv1.Pinger
	runtime.RuntimeLogPersistence
	store.SchemaBootstrapper
	runtimebus.EventStore
	runtimedelivery.Store
	runtimellm.ConversationPersistence
	runtimemanager.ManagerPersistence
	runtimepipeline.SchedulePersistence
	runtimepipeline.MailboxWriteMaterializationStore
	runtimetools.MailboxPersistence
	runtimetools.EntityPersistence
	runtimetools.HumanTaskCardStore
	budgetspend.Store
	runtime.InboundPersistence
	runtimeingress.Store
	runtimestartupownership.Store
	runtimeeffects.Store
	runtimeeffects.CompletionStore
	runtimeeffects.CompletionHeartbeatStore
	runtimeeffects.RecoveryStore

	apiv1.MailboxAPIStore
	apiv1.ObservabilityReadStore
	apiv1.AgentUsageReadStore
	apiv1.AgentDeliveryLifecycleReadStore
	apiv1.APIIdempotencyStore
	apiv1.RunReadStore
	apiv1.EntityReadStore
	apiv1.AgentConversationReadStore
	apiv1.RunBundleContextStore
	apiv1.BundleCatalogReadStore
	apiv1.ConversationForkReadStore
	apiv1.ConversationForkLifecycleStore
	apiv1.TestSetupStore

	runtimerunquiescence.ServeAbandonStore
	runStalledReadStore
}

var _ selectedConcreteRuntimeStore = (*store.PostgresStore)(nil)
var _ selectedConcreteRuntimeStore = (*store.SQLiteRuntimeStore)(nil)

type selectedStoreRoleBackends uint8

const (
	selectedStoreRolePostgres selectedStoreRoleBackends = 1 << iota
	selectedStoreRoleSQLite
	selectedStoreRoleBoth = selectedStoreRolePostgres | selectedStoreRoleSQLite
)

func (b selectedStoreRoleBackends) includes(backend storebackend.Backend) bool {
	switch backend {
	case storebackend.BackendPostgres:
		return b&selectedStoreRolePostgres != 0
	case storebackend.BackendSQLite:
		return b&selectedStoreRoleSQLite != 0
	default:
		return false
	}
}

type selectedStoreRoleClassification string

const (
	selectedStoreRoleConstructionOwner         selectedStoreRoleClassification = "construction_only_backend_owner"
	selectedStoreRoleWorkspaceProcessDB        selectedStoreRoleClassification = "workspace_and_process_db_capability"
	selectedStoreRoleRawRuntimeSQLException    selectedStoreRoleClassification = "raw_runtime_sql_exception"
	selectedStoreRoleLegacyConstructionBlocker selectedStoreRoleClassification = "legacy_construction_blocker"
	selectedStoreRoleCoreRuntime               selectedStoreRoleClassification = "core_runtime_store"
	selectedStoreRolePublicAPIReadControl      selectedStoreRoleClassification = "public_api_read_and_control_capability"
	selectedStoreRoleServeControl              selectedStoreRoleClassification = "serve_control_capability"
	selectedStoreRoleOptionalClassifier        selectedStoreRoleClassification = "optional_capability_classifier"
	selectedStoreRoleOptionalProductWiredBoth  selectedStoreRoleClassification = "optional_product_capability_wired_both"
	selectedStoreRoleOptionalProductPostgres   selectedStoreRoleClassification = "optional_product_capability_postgres_only"
)

type selectedStoreBundleRoleEntry struct {
	Name           string
	Classification selectedStoreRoleClassification
	RequiredOn     selectedStoreRoleBackends
	ForbiddenOn    selectedStoreRoleBackends
	Issue          int
	SpecRef        string
	Reason         string
}

func selectedStoreBundleRoleLedger() []selectedStoreBundleRoleEntry {
	const selectedFacadeSpec = "platform-spec.yaml#runtime_storage.runtime_core_persistence_store_contracts.selected_contracts[name=selected_runtime_store_facade]"
	const rawSQLSpec = "platform-spec.yaml#runtime_storage.runtime_core_persistence_store_contracts.selected_contracts[name=selected_runtime_store_facade].raw_sql_policy"
	const backendSpec = "platform-spec.yaml#runtime_storage.runtime_store_backend_selection"
	return []selectedStoreBundleRoleEntry{
		{Name: "Postgres", Classification: selectedStoreRoleConstructionOwner, RequiredOn: selectedStoreRolePostgres, ForbiddenOn: selectedStoreRoleSQLite, SpecRef: selectedFacadeSpec, Reason: "concrete Postgres owner is construction-only and must not represent selected capability authority"},
		{Name: "SQLDB", Classification: selectedStoreRoleWorkspaceProcessDB, RequiredOn: selectedStoreRoleBoth, SpecRef: rawSQLSpec, Reason: "raw process/workspace DB handle is allowed only for close/health/workspace lifecycle capability"},
		{Name: "RuntimeSQLDB", Classification: selectedStoreRoleRawRuntimeSQLException, RequiredOn: selectedStoreRolePostgres, ForbiddenOn: selectedStoreRoleSQLite, Issue: 1783, SpecRef: rawSQLSpec, Reason: "raw runtime SQL remains a tracked Postgres-only exception and must stay omitted for SQLite"},
		{Name: "Database", Classification: selectedStoreRolePublicAPIReadControl, RequiredOn: selectedStoreRoleBoth, SpecRef: selectedFacadeSpec, Reason: "health/readiness pinger is a selected public control capability"},
		{Name: "RuntimeLogStore", Classification: selectedStoreRoleCoreRuntime, RequiredOn: selectedStoreRoleBoth, SpecRef: selectedFacadeSpec, Reason: "runtime diagnostics/log persistence is required for selected runtime construction"},
		{Name: "RuntimeBlocker", Classification: selectedStoreRoleLegacyConstructionBlocker, ForbiddenOn: selectedStoreRoleBoth, Issue: 1087, SpecRef: backendSpec, Reason: "successful selected construction must not carry a residual fail-closed construction blocker"},
		{Name: "SchemaBootstrapper", Classification: selectedStoreRoleCoreRuntime, RequiredOn: selectedStoreRoleBoth, SpecRef: selectedFacadeSpec, Reason: "schema/bootstrap capability is required before selected runtime persistence executes"},
		{Name: "EventStore", Classification: selectedStoreRoleCoreRuntime, RequiredOn: selectedStoreRoleBoth, SpecRef: selectedFacadeSpec, Reason: "event append/read persistence is a required selected runtime capability"},
		{Name: "PipelineStore", Classification: selectedStoreRoleCoreRuntime, RequiredOn: selectedStoreRoleBoth, SpecRef: selectedFacadeSpec, Reason: "workflow instance persistence wrapper is required for runtime construction"},
		{Name: "SessionRegistry", Classification: selectedStoreRoleCoreRuntime, RequiredOn: selectedStoreRoleBoth, SpecRef: selectedFacadeSpec, Reason: "LLM session registry is required for runtime construction"},
		{Name: "ConversationStore", Classification: selectedStoreRoleCoreRuntime, RequiredOn: selectedStoreRoleBoth, SpecRef: selectedFacadeSpec, Reason: "conversation persistence is required for runtime construction"},
		{Name: "ManagerStore", Classification: selectedStoreRoleCoreRuntime, RequiredOn: selectedStoreRoleBoth, SpecRef: selectedFacadeSpec, Reason: "manager/agent/entity schema persistence is required for runtime construction"},
		{Name: "DeliveryStore", Classification: selectedStoreRoleCoreRuntime, RequiredOn: selectedStoreRoleBoth, SpecRef: selectedFacadeSpec, Reason: "executable delivery construction, claim, settlement, and recovery require the selected canonical lifecycle owner"},
		{Name: "PipelineObligations", Classification: selectedStoreRoleCoreRuntime, RequiredOn: selectedStoreRoleBoth, Issue: 2106, SpecRef: selectedFacadeSpec, Reason: "durable platform-pipeline exclusion, claim, disposition, and summary require the selected canonical owner"},
		{Name: "ScheduleStore", Classification: selectedStoreRoleCoreRuntime, RequiredOn: selectedStoreRoleBoth, SpecRef: selectedFacadeSpec, Reason: "timer/schedule persistence is required for runtime construction"},
		{Name: "MailboxMaterializer", Classification: selectedStoreRoleCoreRuntime, RequiredOn: selectedStoreRoleBoth, SpecRef: selectedFacadeSpec, Reason: "mailbox_write materialization is required for runtime construction"},
		{Name: "MailboxStore", Classification: selectedStoreRoleCoreRuntime, RequiredOn: selectedStoreRoleBoth, SpecRef: selectedFacadeSpec, Reason: "tool mailbox persistence is required for runtime construction"},
		{Name: "ToolEntityStore", Classification: selectedStoreRoleCoreRuntime, RequiredOn: selectedStoreRoleBoth, SpecRef: selectedFacadeSpec, Reason: "tool entity persistence is required for runtime construction"},
		{Name: "HumanTaskStore", Classification: selectedStoreRoleCoreRuntime, RequiredOn: selectedStoreRoleBoth, SpecRef: selectedFacadeSpec, Reason: "human task persistence is required for runtime construction"},
		{Name: "BudgetSpendStore", Classification: selectedStoreRoleCoreRuntime, RequiredOn: selectedStoreRoleBoth, SpecRef: selectedFacadeSpec, Reason: "budget/spend persistence is required for runtime construction"},
		{Name: "InboundStore", Classification: selectedStoreRoleCoreRuntime, RequiredOn: selectedStoreRoleBoth, SpecRef: selectedFacadeSpec, Reason: "inbound webhook persistence is required for runtime construction"},
		{Name: "MailboxAPIStore", Classification: selectedStoreRolePublicAPIReadControl, RequiredOn: selectedStoreRoleBoth, SpecRef: selectedFacadeSpec, Reason: "mailbox API owner is a selected public control capability"},
		{Name: "DecisionCards", Classification: selectedStoreRolePublicAPIReadControl, RequiredOn: selectedStoreRoleBoth, Issue: 1986, SpecRef: selectedFacadeSpec, Reason: "typed decision cards are the selected mutable human-decision resource owner"},
		{Name: "ObservabilityStore", Classification: selectedStoreRolePublicAPIReadControl, RequiredOn: selectedStoreRoleBoth, SpecRef: selectedFacadeSpec, Reason: "observability/event/log/trace reads are selected public read capabilities"},
		{Name: "AgentUsageStore", Classification: selectedStoreRolePublicAPIReadControl, RequiredOn: selectedStoreRoleBoth, SpecRef: selectedFacadeSpec, Reason: "agent.usage reads are selected public read capabilities"},
		{Name: "AgentDeliveryLifecycleStore", Classification: selectedStoreRolePublicAPIReadControl, RequiredOn: selectedStoreRoleBoth, SpecRef: selectedFacadeSpec, Reason: "agent delivery lifecycle reads are selected public read capabilities"},
		{Name: "RuntimeIngressStore", Classification: selectedStoreRoleCoreRuntime, RequiredOn: selectedStoreRoleBoth, SpecRef: selectedFacadeSpec, Reason: "runtime ingress persistence/control is required for runtime construction"},
		{Name: "IdempotencyStore", Classification: selectedStoreRolePublicAPIReadControl, RequiredOn: selectedStoreRoleBoth, SpecRef: selectedFacadeSpec, Reason: "API idempotency is a selected public control capability"},
		{Name: "StartupOwnership", Classification: selectedStoreRoleCoreRuntime, RequiredOn: selectedStoreRoleBoth, SpecRef: selectedFacadeSpec, Reason: "startup ownership persistence is required for runtime construction"},
		{Name: "RunQuiescenceStore", Classification: selectedStoreRoleServeControl, RequiredOn: selectedStoreRoleBoth, SpecRef: selectedFacadeSpec, Reason: "serve abandon-active-runs quiescence is a selected control capability"},
		{Name: "RunReadStore", Classification: selectedStoreRolePublicAPIReadControl, RequiredOn: selectedStoreRoleBoth, SpecRef: selectedFacadeSpec, Reason: "run.get/list/diagnose reads are selected public read capabilities"},
		{Name: "EntityReadStore", Classification: selectedStoreRolePublicAPIReadControl, RequiredOn: selectedStoreRoleBoth, SpecRef: selectedFacadeSpec, Reason: "entity.get/list/aggregate reads are selected public read capabilities"},
		{Name: "AgentConversationReadStore", Classification: selectedStoreRolePublicAPIReadControl, RequiredOn: selectedStoreRoleBoth, Issue: 1782, SpecRef: selectedFacadeSpec, Reason: "agent/conversation reads were promoted as pure operator-read capabilities"},
		{Name: "RunBundleContextStore", Classification: selectedStoreRolePublicAPIReadControl, RequiredOn: selectedStoreRoleBoth, SpecRef: selectedFacadeSpec, Reason: "served event.publish run-bundle context reads are selected public read capabilities"},
		{Name: "BundleRuntimeCatalogStore", Classification: selectedStoreRoleOptionalProductPostgres, RequiredOn: selectedStoreRolePostgres, ForbiddenOn: selectedStoreRoleSQLite, SpecRef: selectedFacadeSpec, Reason: "DB-loaded bundle runtime catalog remains a spec-classified Postgres-only product capability"},
		{Name: "BundleSourceCatalogStore", Classification: selectedStoreRoleOptionalProductPostgres, RequiredOn: selectedStoreRolePostgres, ForbiddenOn: selectedStoreRoleSQLite, SpecRef: selectedFacadeSpec, Reason: "bundle source catalog writes/register/delete are spec-classified optional product lifecycle capabilities"},
		{Name: "RunBundleAvailabilityStore", Classification: selectedStoreRoleOptionalProductPostgres, RequiredOn: selectedStoreRolePostgres, ForbiddenOn: selectedStoreRoleSQLite, SpecRef: selectedFacadeSpec, Reason: "active bundle availability admission is a spec-classified Postgres-backed optional product capability"},
		{Name: "StartupRecoveryStore", Classification: selectedStoreRoleOptionalProductPostgres, RequiredOn: selectedStoreRolePostgres, ForbiddenOn: selectedStoreRoleSQLite, SpecRef: selectedFacadeSpec, Reason: "startup recovery is a spec-classified Postgres-only product capability outside local-dev SQLite runtime construction"},
		{Name: "RunStalledReader", Classification: selectedStoreRoleOptionalProductWiredBoth, RequiredOn: selectedStoreRoleBoth, SpecRef: selectedFacadeSpec, Reason: "run-stalled monitor reads are wired on both selected stores and must not silently disappear"},
		{Name: "APIOptionalCapabilityBuilder", Classification: selectedStoreRoleOptionalClassifier, RequiredOn: selectedStoreRoleBoth, Issue: 1810, SpecRef: selectedFacadeSpec, Reason: "optional public capabilities are produced through the explicit selected classifier"},
		{Name: "RunForkRuntimeOwner", Classification: selectedStoreRoleOptionalProductPostgres, RequiredOn: selectedStoreRolePostgres, ForbiddenOn: selectedStoreRoleSQLite, SpecRef: selectedFacadeSpec, Reason: "selected-contract run.fork runtime lifecycle is a spec-classified Postgres-only optional product capability"},
	}
}

func validateSelectedStoreBundleRoles(backend storebackend.Backend, stores storeBundle) error {
	if backend != storebackend.BackendPostgres && backend != storebackend.BackendSQLite {
		return fmt.Errorf("selected store role validation requires postgres or sqlite backend, got %q", backend)
	}
	value := reflect.ValueOf(stores)
	var missing []string
	var forbidden []string
	for _, entry := range selectedStoreBundleRoleLedger() {
		field := value.FieldByName(entry.Name)
		if !field.IsValid() {
			return fmt.Errorf("selected store role ledger names unknown storeBundle field %q", entry.Name)
		}
		configured := selectedStoreBundleRoleConfigured(field)
		if entry.RequiredOn.includes(backend) && !configured {
			missing = append(missing, entry.Name)
		}
		if entry.ForbiddenOn.includes(backend) && configured {
			forbidden = append(forbidden, entry.Name)
		}
	}
	if len(missing) == 0 && len(forbidden) == 0 {
		return nil
	}
	sort.Strings(missing)
	sort.Strings(forbidden)
	parts := make([]string, 0, 2)
	if len(missing) > 0 {
		parts = append(parts, "missing required roles: "+strings.Join(missing, ", "))
	}
	if len(forbidden) > 0 {
		parts = append(parts, "forbidden roles configured: "+strings.Join(forbidden, ", "))
	}
	return fmt.Errorf("selected %s store bundle failed role validation: %s", backend, strings.Join(parts, "; "))
}

func selectedStoreBundleRoleConfigured(value reflect.Value) bool {
	if !value.IsValid() {
		return false
	}
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return !value.IsNil()
	default:
		return !value.IsZero()
	}
}
