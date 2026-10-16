package pipeline

// ActivityRecoverySummary carries results out of their owning recovery mutation,
// not an ACK, work queue or execution grant. Publication consumes the durable
// journal/readback obligation only after native transaction acknowledgement.
type ActivityRecoverySummary struct {
	Results []ActivityAttemptRecord
}
