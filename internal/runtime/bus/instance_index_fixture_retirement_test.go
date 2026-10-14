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

func TestFlowInstanceFixturesHaveNoRetiredDescriptorReader(t *testing.T) {
	for _, owner := range []reflect.Type{
		reflect.TypeOf((*targetRouteMemoryStore)(nil)),
		reflect.TypeOf((*connectRoutePlanLifecycleStore)(nil)),
		reflect.TypeOf((*connectRoutePlanConcurrentLifecycleStore)(nil)),
		reflect.TypeOf((*finalFlowInstanceAuthoringLifecycleStore)(nil)),
	} {
		for _, name := range []string{"ListActiveFlowInstanceDescriptors", "UpsertFlowInstanceRoute", "DeleteFlowInstanceRoute", "ListFlowInstanceRoutes"} {
			if _, present := owner.MethodByName(name); present {
				t.Errorf("%s retains the retired instance mirror API %s", owner, name)
			}
		}
		for _, name := range []string{"flowRoutes", "flowInstances"} {
			if _, present := owner.Elem().FieldByName(name); present {
				t.Errorf("%s retains retired mirror %s beside its native index", owner, name)
			}
		}
	}
}

func TestFinalAuthoringFixtureHasNoDescriptorMirror(t *testing.T) {
	owner := reflect.TypeOf(finalFlowInstanceAuthoringLifecycleStore{})
	if _, present := owner.FieldByName("flowInstances"); present {
		t.Fatal("authoring fixture retains a descriptor mirror beside its native index")
	}
}
