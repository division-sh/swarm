package storetest

import "github.com/division-sh/swarm/internal/store/construction"

type ReleaseProcessReadOnlyInspection = construction.ReleaseProcessReadOnlyInspection

// The location belongs to the already-started release-process fixture. Opening
// this independent reader never bootstraps, activates, or recovers that process.
func OpenReleaseProcessReadOnlyInspection(backend, location string) (ReleaseProcessReadOnlyInspection, error) {
	return construction.OpenReleaseProcessReadOnlyInspectionForTest(backend, location)
}
