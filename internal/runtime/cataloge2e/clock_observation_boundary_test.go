package cataloge2e

import (
	"testing"
	"time"
)

func TestCatalogSQLiteObservationBoundaryKeepsApplicationClockCut(t *testing.T) {
	before := time.Now().UTC()
	boundary := catalogHarnessStartBoundary(t, nil, catalogBackendSQLite)
	after := time.Now().UTC()
	if at := boundary.Add(time.Second); at.Before(before) || at.After(after) {
		t.Fatalf("SQLite boundary did not retain the exact application-clock cut: before=%v boundary=%v after=%v", before, boundary, after)
	}
}
