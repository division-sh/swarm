package bus

import (
	"errors"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
)

func (s *targetRouteMemoryStore) requireConstructionUnchanged(t testing.TB) {
	t.Helper()
	type header struct {
		Owner    flowidentity.RunScopedFlowInstance
		Instance pipeline.WorkflowInstance
	}
	snapshot := func() ([]header, map[flowidentity.RunScopedFlowInstance]pipeline.FlowConstructionPublicationEvidence) {
		t.Helper()
		s.mu.Lock()
		defer s.mu.Unlock()
		headers := make([]header, 0, len(s.instanceObservations))
		for _, observed := range s.instanceObservations {
			instance, err := observed.WorkflowInstance()
			if err != nil {
				t.Fatal(err)
			}
			headers = append(headers, header{Owner: observed.Owner(), Instance: instance})
		}
		receipts := make(map[flowidentity.RunScopedFlowInstance]pipeline.FlowConstructionPublicationEvidence, len(s.constructions))
		for owner, receipt := range s.constructions {
			if receipt.Fields != nil {
				fields, err := canonicaljson.CloneRuntimeValue(receipt.Fields)
				if err != nil {
					t.Fatal(err)
				}
				receipt.Fields = fields.(map[string]any)
			}
			receipts[owner] = receipt
		}
		return headers, receipts
	}
	headers, receipts := snapshot()
	t.Cleanup(func() {
		afterHeaders, afterReceipts := snapshot()
		if !reflect.DeepEqual(headers, afterHeaders) || !reflect.DeepEqual(receipts, afterReceipts) {
			t.Error("publication changed exact native instance or immutable construction evidence")
		}
	})
}

func (s *targetRouteMemoryStore) refuseInstanceIndexReads(t testing.TB) {
	t.Helper()
	s.mu.Lock()
	s.instanceIndexReads = 0
	s.instanceIndexErr = errors.New("persisted replay must not select a new receiver")
	s.mu.Unlock()
	t.Cleanup(func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		if s.instanceIndexReads != 0 {
			t.Errorf("persisted replay performed %d fresh instance-index reads", s.instanceIndexReads)
		}
	})
}
