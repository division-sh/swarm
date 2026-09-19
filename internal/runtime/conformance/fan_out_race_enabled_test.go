//go:build race

package conformance

// Race qualification retains the complete workload and its existing correctness
// deadline; the Gate A throughput target applies to normal builds.
const fanOutRaceBuild = true
