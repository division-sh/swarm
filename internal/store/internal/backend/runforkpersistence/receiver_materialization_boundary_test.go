package runforkpersistence

func receiverMaterializationBoundaryAllowances() map[string]historicalBoundaryAllowance {
	return map[string]historicalBoundaryAllowance{
		"runtime/bus::selectedRunTargetOwnerProjection.bindReceiverMaterializations/reference:events::AdmitReceiverMaterializationPlan": {
			1, "the complete publication is bound after canonical per-handler target classification",
		},
		"events::ReceiverMaterializationPlan.ValidatePublication/reference:events::AdmitReceiverMaterializationPlan": {
			1, "re-admission consumes the same exact receiver relation, not decoded strings",
		},
		"events::DeliveryRoute.UnmarshalJSON/reference:events::RestoreDeliveryMaterialization": {
			1, "strict route wire codec; aggregate admission remains mandatory",
		},
		"store/internal/backend/delivery::decodeRoute/reference:events::RestoreReceiverMaterializationRecord": {
			1, "canonical selected-store delivery hydration",
		},
		"runtime/deliverylifecycle::DecodeHistoricalSnapshot/reference:events::RestoreReceiverMaterializationRecord": {
			1, "historical delivery hydration retains exact dependency evidence",
		},
		"events::RestoreReceiverMaterializationRecord/reference:events::RestoreDeliveryMaterialization":                                {1, "the strict durable supplier record delegates dependency decoding to the existing owner"},
		"runtime/bus::selectedRunTargetOwnerProjection.bindReceiverMaterializations/reference:events::AdmitNodeReceiverInitialization": {1, "canonical handler classification supplies the exact initializing node"},
		"runtime/bus::flowReceiverInitialization/reference:events::AdmitFlowReceiverInitialization":                                    {1, "canonical activation plan supplies exact flow initialization"},
	}
}

func receiverMaterializationBoundaryReference(callee string) bool {
	return callee == "events::AdmitReceiverMaterializationPlan" || callee == "events::RestoreDeliveryMaterialization" || callee == "events::RestoreReceiverMaterializationRecord" || callee == "events::AdmitNodeReceiverInitialization" || callee == "events::AdmitFlowReceiverInitialization"
}
