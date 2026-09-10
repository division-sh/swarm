package runforkpersistence

// This allowlist guards semantic construction and the named write handoff, not
// just the file containing each owner. Runtime relation/rollback tests remain
// required: reference counts are not proof of transaction or lineage correctness.
func inheritedFanOutBoundaryAllowances() map[string]historicalBoundaryAllowance {
	allowed := map[string]historicalBoundaryAllowance{}
	edge := func(caller, callee, reason string) {
		allowed[caller+"/reference:"+callee] = historicalBoundaryAllowance{1, reason}
	}
	edge("runtime/fanoutobligation::ordinalEmission", "events::NewInheritedFanOutOrigin", "one exact ordinal origin projection")
	edge("runtime/fanoutobligation::OrdinalEmission.NewEvent", "events::NewInheritedFanOutEvent", "only the semantic ordinal projection creates this event variant")
	edge("events::BindManagerOutputIdentity", "events::NewInheritedFanOutEvent", "event owner preserves previously admitted origin during identity binding")
	edge("events::RestoreAdmittedEvent", "events::NewInheritedFanOutEvent", "canonical durable restoration requires the exact decoded origin")
	edge("store/internal/backend/fanoutorigin::ValidateCommitted", "runtime/fanoutobligation::ValidateCommittedOrdinalEvent", "live exact outcome readback consumes ordinal semantics")
	edge("store/internal/backend/runforkpersistence::admitRunForkInheritedFanOutHistory", "runtime/fanoutobligation::ValidateCommittedOrdinalEvent", "fixed-revision exact outcome readback consumes ordinal semantics")
	edge("store/internal/backend/eventrecord::Record.decodeInheritedFanOutOrigin", "events::NewInheritedFanOutOrigin", "strict private durable codec")
	edge("runtime/engine::Executor.EvaluateFanOutOrdinal", "runtime/fanoutobligation::PrepareOrdinalEmission", "evaluator consumes immutable intent relation")
	edge("store/internal/backend/pipelinepersistence::commitFanOutChunk", "runtime/fanoutobligation::PrepareOrdinalEmission", "locked intent and trigger select exact ordinal")
	edge("runtime/bus::EventBus.PrepareEnginePublications", "runtime/bus::EventBus.admitEnginePublishEvent", "only engine planning can prepare inherited origin")
	edge("runtime/bus::EnginePublicationPlan.ValidateDurablePublicationPlan", "runtime/bus::PublicationCommand.ValidateFanOut", "pure prepared-plan shape, not write authority")
	edge("store/internal/backend/eventpersistence::commitFanOutPublicationTx", "runtime/bus::PublicationCommand.ValidateFanOut", "named transaction validates plan shape")
	edge("store/internal/backend/eventpersistence::commitPublicationTx", "store/internal/backend/eventpersistence::commitValidatedPublicationTx", "generic publication rejects inherited class before private mutation")
	edge("store/internal/backend/eventpersistence::commitFanOutPublicationTx", "store/internal/backend/eventpersistence::commitValidatedPublicationTx", "named publication validates locked ordinal projection")
	edge("store/internal/backend/pipelinepersistence::commitFanOutChunk", "store/internal/backend/pipelinepersistence::eventCommitTxStore.commitFanOutPublicationTx", "only named atomic chunk may submit this write")
	for _, backend := range []string{"Postgres", "SQLite"} {
		edge("store/internal/backend/pipelinepersistence::Pipeline"+backend+"Owner.commitFanOutPublicationTx", "store/internal/backend/pipelinepersistence::EventCommitOwner.CommitFanOutPublicationTx", "thin selected-store handoff")
		edge("store/internal/backend/eventpersistence::Event"+backend+"Owner.CommitFanOutPublicationTx", "store/internal/backend/eventpersistence::commitFanOutPublicationTx", "thin selected-store handoff")
	}
	edge("store/internal/backend/eventrecord::Record.ValidateInheritedFanOutOwner", "store/internal/backend/fanoutorigin::ValidateCommitted", "durable readback consumes exact committed origin relation")
	for _, backend := range []string{"postgres", "sqlite"} {
		for _, caller := range []string{"Load", "LoadMany"} {
			edge("store/internal/backend/eventrecord/"+backend+"::"+caller, "store/internal/backend/eventrecord::Record.ValidateInheritedFanOutOwner", "all canonical hydration validates origin ownership after closing rows")
		}
	}
	return allowed
}

func inheritedFanOutBoundaryReference(callee string) bool {
	for _, guarded := range []string{
		"events::NewInheritedFanOutOrigin",
		"events::NewInheritedFanOutEvent",
		"runtime/fanoutobligation::ValidateCommittedOrdinalEvent",
		"runtime/fanoutobligation::PrepareOrdinalEmission",
		"runtime/bus::EventBus.admitEnginePublishEvent",
		"runtime/bus::PublicationCommand.ValidateFanOut",
		"store/internal/backend/eventpersistence::commitValidatedPublicationTx",
		"store/internal/backend/eventpersistence::commitFanOutPublicationTx",
		"store/internal/backend/pipelinepersistence::eventCommitTxStore.commitFanOutPublicationTx",
		"store/internal/backend/pipelinepersistence::EventCommitOwner.CommitFanOutPublicationTx",
		"store/internal/backend/eventrecord::Record.ValidateInheritedFanOutOwner",
		"store/internal/backend/fanoutorigin::ValidateCommitted",
	} {
		if callee == guarded {
			return true
		}
	}
	return false
}
