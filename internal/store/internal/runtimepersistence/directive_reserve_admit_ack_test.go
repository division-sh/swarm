package runtimepersistence

import (
	"errors"
	"testing"

	runtimeagentcontrol "github.com/division-sh/swarm/internal/runtime/agentcontrol"
)

func TestDirectiveReserveAndAdmitAcknowledgmentAcrossRestart(t *testing.T) {
	forEachDirectiveAmbiguityBackend(t, func(t *testing.T, backend directiveAmbiguityBackend) {
		for _, fault := range []directivePersistenceFault{directiveFaultReserve, directiveFaultAdmit} {
			for _, mode := range []directiveFaultMode{directiveFaultBeforeCommit, directiveFaultAfterCommit} {
				t.Run(string(fault)+"/"+string(mode), func(t *testing.T) {
					baseOperations, baseEvents := countDirectiveReservationRows(t, backend)
					h := newDirectiveAmbiguityHarness(t, backend, &directiveAmbiguityAgent{id: "reserve-admit-agent", response: "accepted"})
					h.faults.setFault(fault, mode)
					result, err := h.manager.SendDirective(h.workContext(t), h.request)
					if mode == directiveFaultBeforeCommit {
						if !errors.Is(err, errInjectedDirectivePersistence) {
							t.Fatalf("unacknowledged %s error = %v", fault, err)
						}
						if got := h.agent.calls.Load(); got != 0 {
							t.Fatalf("BoardStep calls before retry = %d, want 0", got)
						}
						operations, events := countDirectiveReservationRows(t, backend)
						if fault == directiveFaultReserve {
							if operations != baseOperations || events != baseEvents {
								t.Fatalf("unacknowledged reservation rows = %d/%d, want %d/%d", operations, events, baseOperations, baseEvents)
							}
						} else {
							if operations != baseOperations+1 || events != baseEvents+1 {
								t.Fatalf("unacknowledged admission rows = %d/%d, want %d/%d", operations, events, baseOperations+1, baseEvents+1)
							}
							op := h.loadOperation(t)
							assertDirectiveOperationEvidence(t, op, runtimeagentcontrol.DirectiveOperationPrepared, false, false)
							assertDirectiveReceipt(t, backend.db, op.DirectiveEventID, "", nil)
						}
						h.restartManager(t)
						result, err = h.manager.SendDirective(h.workContext(t), h.request)
					}
					if err != nil || !result.OK || result.Response != "accepted" {
						t.Fatalf("acknowledged %s result = %#v err=%v", fault, result, err)
					}
					if got := h.agent.calls.Load(); got != 1 {
						t.Fatalf("BoardStep calls = %d, want 1", got)
					}
					op := h.loadOperation(t)
					assertDirectiveOperationEvidence(t, op, runtimeagentcontrol.DirectiveOperationSucceeded, true, false)
					assertDirectiveSuccessSettlement(t, backend.db, op)
					assertDirectiveReservationRowCounts(t, backend, baseOperations+1, baseEvents+1)

					h.restartManager(t)
					replayed, err := h.manager.SendDirective(h.workContext(t), h.request)
					if err != nil || !replayed.OK || replayed.Response != "accepted" {
						t.Fatalf("restarted same-key result = %#v err=%v", replayed, err)
					}
					if replayed.OperationID != result.OperationID || replayed.DirectiveEventID != result.DirectiveEventID {
						t.Fatalf("restarted directive identity = %s/%s, want %s/%s", replayed.OperationID, replayed.DirectiveEventID, result.OperationID, result.DirectiveEventID)
					}
					if got := h.agent.calls.Load(); got != 1 {
						t.Fatalf("BoardStep calls after restart = %d, want 1", got)
					}
					assertDirectiveReservationRowCounts(t, backend, baseOperations+1, baseEvents+1)
				})
			}
		}
	})
}

func countDirectiveReservationRows(t *testing.T, backend directiveAmbiguityBackend) (int, int) {
	t.Helper()
	var operations, events int
	if err := backend.db.QueryRow(`SELECT COUNT(*) FROM agent_directive_operations`).Scan(&operations); err != nil {
		t.Fatalf("count directive operations: %v", err)
	}
	if err := backend.db.QueryRow(`SELECT COUNT(*) FROM events WHERE event_name = 'platform.agent_directive'`).Scan(&events); err != nil {
		t.Fatalf("count directive events: %v", err)
	}
	return operations, events
}

func assertDirectiveReservationRowCounts(t *testing.T, backend directiveAmbiguityBackend, wantOperations, wantEvents int) {
	t.Helper()
	operations, events := countDirectiveReservationRows(t, backend)
	if operations != wantOperations || events != wantEvents {
		t.Fatalf("directive operation/event counts = %d/%d, want %d/%d", operations, events, wantOperations, wantEvents)
	}
}
