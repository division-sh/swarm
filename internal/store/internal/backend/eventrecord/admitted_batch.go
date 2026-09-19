package eventrecord

import (
	"sync"

	"github.com/division-sh/swarm/internal/events"
)

// AdmittedRecord retains both projections from one full canonical admission.
type AdmittedRecord struct {
	Event      events.AdmittedEvent
	Settlement events.RouteSettlement
}

// DecodedRecord keeps an error at its original position. The adapter must still
// check missing rows and inherited owners in request order before exposing any
// admitted records; pure decoding cannot establish database ownership.
type DecodedRecord struct {
	AdmittedRecord
	Err error
}

// DecodeLoadedRecords only schedules the canonical decoder over the adapter's
// already-read bounded physical batch. Inputs must remain immutable until it
// returns. All helper goroutines join here, even when individual records fail;
// no query, authority, cancellation lifetime, or work survives this operation.
func DecodeLoadedRecords(records []Record) []DecodedRecord {
	results := make([]DecodedRecord, len(records))
	workers := min(4, len(records))
	decode := func(start, stride int) {
		for i := start; i < len(records); i += stride {
			results[i].Event, results[i].Settlement, results[i].Err = records[i].DecodeWithSettlement()
		}
	}
	if workers <= 1 {
		decode(0, 1)
		return results
	}
	var joined sync.WaitGroup
	for worker := 1; worker < workers; worker++ {
		joined.Add(1)
		go func(start int) {
			defer joined.Done()
			decode(start, workers)
		}(worker)
	}
	decode(0, workers)
	joined.Wait()
	return results
}
