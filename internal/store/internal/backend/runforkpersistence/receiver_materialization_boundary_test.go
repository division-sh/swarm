package runforkpersistence

func receiverMaterializationBoundaryAllowances() map[string]historicalBoundaryAllowance {
	return map[string]historicalBoundaryAllowance{
		"store/internal/backend/delivery::decodeRoute/reference:events::RestoreReceiverMaterializationRecord": {
			1, "canonical selected-store delivery hydration",
		},
		"runtime/deliverylifecycle::DecodeHistoricalSnapshot/reference:events::RestoreReceiverMaterializationRecord": {
			1, "historical delivery hydration retains exact construction evidence",
		},
		"events::RestoreReceiverMaterializationRecord/reference:events::decodeReceiverInitialization": {1, "the strict durable record consumes the same construction receipt decoder"},
		"events::ReceiverInitialization.UnmarshalJSON/reference:events::decodeReceiverInitialization": {1, "the standalone wire consumes the same construction receipt decoder"},
		"runtime/bus::flowReceiverInitialization/reference:events::AdmitFlowReceiverInitialization":                                    {1, "canonical activation plan supplies exact flow initialization"},
	}
}

func receiverMaterializationBoundaryReference(callee string) bool {
	return callee == "events::AdmitReceiverMaterializationPlan" || callee == "events::RestoreDeliveryMaterialization" || callee == "events::restoreDeliveryMaterialization" || callee == "events::RestoreReceiverMaterializationRecord" || callee == "events::decodeReceiverInitialization" || callee == "events::AdmitNodeReceiverInitialization" || callee == "events::AdmitFlowReceiverInitialization"
}
