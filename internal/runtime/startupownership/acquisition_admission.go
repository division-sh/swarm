package startupownership

func AdmitAuthorityInspection(inspection AuthorityInspection) error {
	return inspection.Validate()
}
