package deliverycontinuation

import "testing"

func TestCoordinatorCreatingPredecessorSettlementSignalsOnce(t *testing.T) {
	authority, owner, cleanup := coordinatorTestAuthorityAndOwner(t)
	defer cleanup()
	coordinator, err := New(&coordinatorTestStore{}, coordinatorTestRestarts{}, authority, owner, &coordinatorTestDispatcher{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	before := coordinator.wakeVersion.Load()
	if err := coordinator.Release("creating-turn"); err != nil {
		t.Fatal(err)
	}
	if coordinator.wakeVersion.Load() != before+1 {
		t.Fatal("committed predecessor settlement did not signal deferred followers")
	}
	select {
	case <-coordinator.wake:
	default:
		t.Fatal("predecessor settlement did not wake the existing control lane")
	}
	if err := coordinator.releaseTerminal("creating-turn"); err != nil {
		t.Fatal(err)
	}
	if coordinator.wakeVersion.Load() != before+1 {
		t.Fatal("duplicate terminal evidence repeated the wake")
	}
	if state, found := coordinatorOwnershipState(coordinator, "creating-turn"); !found || state != ownershipTerminal {
		t.Fatal("waking followers revived the predecessor")
	}
}
