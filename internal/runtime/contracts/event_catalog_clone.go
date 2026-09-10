package contracts

// CloneEventCatalogEntry isolates schema projections using the contract owner's
// existing clone semantics, including field refinements and admission provenance.
func CloneEventCatalogEntry(entry EventCatalogEntry) EventCatalogEntry {
	return cloneEventCatalogEntry(entry)
}
