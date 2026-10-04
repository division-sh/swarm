package startupownership

// SuccessorAcquisitionKind is the durable-head rule used after independent
// possession succeeds. Inspection can apply it without recording a successor.
func SuccessorAcquisitionKind(state State) (AcquisitionKind, error) {
	switch state {
	case StateReleased:
		return AcquisitionCleanHandoff, nil
	case StateActive:
		return AcquisitionCrashTakeover, nil
	default:
		return "", &AcquisitionError{Failure: AcquisitionPriorOwnerAmbiguous,
			Detail: "durable process authority head is terminal without a current successor"}
	}
}

func AdmitAuthorityInspection(inspection AuthorityInspection) error {
	if err := inspection.Validate(); err != nil {
		return err
	}
	switch inspection.Status {
	case AuthorityInspectionEmpty:
		return nil
	case AuthorityInspectionValid:
		_, err := SuccessorAcquisitionKind(inspection.State)
		return err
	default:
		return &AcquisitionError{Failure: AcquisitionPriorOwnerAmbiguous, Detail: inspection.Detail}
	}
}
