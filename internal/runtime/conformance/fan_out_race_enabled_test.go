//go:build race

package conformance

// Race qualification retains the complete workload. Only the two disputed waits
// in Gate A 5749347759 use a 60-second race watchdog; normal bounds are unchanged.
const fanOutRaceBuild = true
