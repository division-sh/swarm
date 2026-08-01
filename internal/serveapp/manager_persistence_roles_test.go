package serveapp

import (
	runtimeagentcontrol "github.com/division-sh/swarm/internal/runtime/agentcontrol"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
)

func selectedStoreManagerPersistenceRoles(selected any, eventBus *runtimebus.EventBus) runtimemanager.PersistenceRoles {
	roles := runtimemanager.PersistenceRoles{
		AgentRoutes: eventBus, RouteInstaller: eventBus, RouteVerifier: eventBus,
		RouteRestorer: eventBus, RouteRetirer: eventBus, RouteRemover: eventBus,
		CreationPublisher: eventBus, DeliveryRuntime: eventBus,
	}
	roles.LifecycleState, _ = selected.(runtimemanager.AgentLifecycleStateReader)
	roles.LifecycleEffects, _ = selected.(runtimeeffects.Store)
	roles.LifecycleDiagnostics, _ = selected.(runtimemanager.AgentLifecycleDiagnosticPersistence)
	roles.EffectsRecovery, _ = selected.(runtimeeffects.RecoveryStore)
	roles.DeliveryQuiescence, _ = selected.(runtimemanager.ActiveRunDeliveryQuiescenceReader)
	roles.EventExistence, _ = selected.(runtimemanager.EventExistenceReader)
	roles.DirectiveOperations, _ = selected.(runtimeagentcontrol.DirectiveOperationStore)
	roles.DirectiveTargets, _ = selected.(runtimemanager.AgentDirectiveRunTargetResolver)
	roles.FlowRoutes, _ = selected.(runtimebus.FlowInstanceRoutePersistence)
	return roles
}
