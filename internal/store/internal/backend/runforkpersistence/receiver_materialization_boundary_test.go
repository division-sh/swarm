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
		"events::RestoreReceiverMaterializationRecord/reference:events::restoreDeliveryMaterialization":                                {1, "the strict supplier record passes its already decoded dependency to the same private owner"},
		"events::RestoreDeliveryMaterialization/reference:events::restoreDeliveryMaterialization":                                      {1, "standalone wire admission consumes the same private dependency restoration"},
		"runtime/bus::selectedRunTargetOwnerProjection.bindReceiverMaterializations/reference:events::AdmitNodeReceiverInitialization": {1, "canonical handler classification supplies the exact initializing node"},
		"runtime/bus::flowReceiverInitialization/reference:events::AdmitFlowReceiverInitialization":                                    {1, "canonical activation plan supplies exact flow initialization"},
	}
}

func receiverMaterializationBoundaryReference(callee string) bool {
	return callee == "events::AdmitReceiverMaterializationPlan" || callee == "events::RestoreDeliveryMaterialization" || callee == "events::restoreDeliveryMaterialization" || callee == "events::RestoreReceiverMaterializationRecord" || callee == "events::AdmitNodeReceiverInitialization" || callee == "events::AdmitFlowReceiverInitialization"
}
