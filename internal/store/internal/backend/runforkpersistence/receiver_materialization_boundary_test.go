package runforkpersistence

func receiverMaterializationBoundaryAllowances() map[string]historicalBoundaryAllowance {
	return map[string]historicalBoundaryAllowance{
		"events::ReceiverMaterializationPlan.ValidatePublication/reference:events::AdmitReceiverMaterializationPlan": {
			1, "re-admission consumes the same exact receiver relation, not decoded strings",
		},
	}
}

func receiverMaterializationBoundaryReference(callee string) bool {
	return callee == "events::AdmitReceiverMaterializationPlan"
}
