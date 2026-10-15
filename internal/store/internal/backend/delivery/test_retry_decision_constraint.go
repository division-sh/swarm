package delivery

// RetryDecisionConstraintProbe supplies invalid column combinations only inside
// the caller's rolled-back native proof transaction.
type RetryDecisionConstraintProbe struct {
	ClosureKind string
	Outcome     *string
	CompletedAt *string
	EligibleAt  *string
}
