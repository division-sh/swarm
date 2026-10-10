package bus

import (
	"errors"
	"reflect"
	"testing"
)

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
		reflect.TypeOf((*connectRoutePlanDescriptorStore)(nil)),
		reflect.TypeOf((*connectRoutePlanConcurrentLifecycleStore)(nil)),
		reflect.TypeOf((*finalFlowInstanceAuthoringLifecycleStore)(nil)),
	} {
		if _, present := owner.MethodByName("ListActiveFlowInstanceDescriptors"); present {
			t.Errorf("%s retains the retired descriptor reader instead of the native index", owner)
		}
	}
}

func TestFinalAuthoringFixtureHasNoDescriptorMirror(t *testing.T) {
	owner := reflect.TypeOf(finalFlowInstanceAuthoringLifecycleStore{})
	if _, present := owner.FieldByName("flowInstances"); present {
		t.Fatal("authoring fixture retains a descriptor mirror beside its native index")
	}
}
