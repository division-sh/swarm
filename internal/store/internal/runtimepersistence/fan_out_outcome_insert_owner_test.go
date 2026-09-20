package runtimepersistence

import (
	"errors"
	"fmt"
	"strings"
	"testing"
)

func TestFanOutOutcomeInsertOwnerExecutionBothStores(t *testing.T) {
	for _, backend := range []string{"postgres", "sqlite"} {
		for _, phase := range []string{"success", "before_exec", "after_exec", "close", "execute_and_close"} {
			t.Run(backend+"/"+phase, func(t *testing.T) {
				f := newGroupProofFixture(t, backend, 3)
				f.prepare(t)
				f.seal(t)
				before := f.snapshot(t)
				fault := errors.New("outcome insertion refused")
				closeFault := errors.New("outcome statement close refused")
				entered, completed, closed, commits, rollbacks := 0, 0, 0, 0, 0
				f.probe.set(func(at, query string) error {
					if at == "before_commit" {
						commits++
						if closed != 1 {
							return fmt.Errorf("commit before outcome statement close: %d", closed)
						}
					}
					if at == "after_rollback" {
						rollbacks++
					}
					if !strings.HasPrefix(query, "INSERT INTO fan_out_outcomes ") {
						return nil
					}
					if at == "before_exec" {
						entered++
					}
					if at == "after_exec" {
						completed++
					}
					if at == "after_stmt_close" {
						closed++
						if phase == "close" || phase == "execute_and_close" {
							return closeFault
						}
					}
					if (at == phase || phase == "execute_and_close" && at == "after_exec") && entered == 2 {
						return fault
					}
					return nil
				})
				result, err := f.owner.CommitFanOutChunk(f.ctx, f.command)
				f.probe.set(nil)
				if closed != 1 {
					t.Fatalf("native statement closes=%d want=1", closed)
				}
				if phase != "success" {
					want := fmt.Sprintf("insert fan-out outcome ordinal 1: %v", fault)
					wantFault, wantEntered := fault, 2
					if phase == "close" {
						want, wantFault, wantEntered = closeFault.Error(), closeFault, 3
					}
					if !errors.Is(err, wantFault) || err.Error() != want || entered != wantEntered || len(result.Publications) != 0 || commits != 0 || rollbacks != 1 {
						t.Fatalf("result=%+v err=%v entered=%d completed=%d", result, err, entered, completed)
					}
					wantCompleted := 1
					if phase == "after_exec" || phase == "execute_and_close" {
						wantCompleted = 2
					} else if phase == "close" {
						wantCompleted = 3
					}
					if completed != wantCompleted {
						t.Fatalf("native writes=%d want=%d", completed, wantCompleted)
					}
					f.unchanged(t, before)
					return
				}
				if err != nil || entered != 3 || completed != 3 || len(result.Publications) != 3 || result.Intent.Cursor != 3 || commits != 1 || rollbacks != 0 {
					t.Fatalf("result=%+v err=%v entered=%d completed=%d", result, err, entered, completed)
				}
				if err := f.group.ValidateCommitted(f.ctx, f.claims); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}
