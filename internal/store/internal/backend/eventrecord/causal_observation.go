package eventrecord

// CausalObservationRow is a physical witness. PayloadEntityID is a payload
// reference used by catalog assertions, not canonical envelope authority.
type CausalObservationRow struct {
	ID, Name, SourceEventID, PayloadEntityID string
}

type GlobalEventChronologyRow struct {
	Name, EntityID, FlowInstance string
}

type SourceRouteSettlementStorage struct {
	Events, DistinctLedgers, Bytes int
}
