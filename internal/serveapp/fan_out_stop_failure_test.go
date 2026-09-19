package serveapp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"

	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/runtime/runcontrol"
	"github.com/division-sh/swarm/internal/servedparity"
	"github.com/google/uuid"
)

// M22 validation and statement failures execute the actual selected stop owner.
// Commit uncertainty is separately proved at the real driver Commit boundary in
// runtimepersistence; statement rollback is not offered as its substitute.
func TestIssue2394StopFailureEnvelopeBothStores(t *testing.T) {
	for _, backend := range []servedparity.Backend{servedparity.BackendDefaultSQLite, servedparity.BackendExplicitPostgres} {
		for _, fault := range []string{"invalid_capsule", "cancel_statement"} {
			t.Run(string(backend)+"/"+fault, func(t *testing.T) {
				rig := newIssue2394StopRig(t, backend)
				rt := rig.start(t)
				runID, eventID, _ := createServedControlWaitingRun(t, rt, "stop-failure-"+uuid.NewString())
				requireServedOKJSONRPC(t, rt.Endpoint, "run.pause", map[string]any{"run_id": runID})
				seedIssue2394StopIntents(t, rt, runID, eventID)
				assertIssue2394StopMatrix(t, readIssue2394StopSnapshot(t, rt.DB, runID), false)
				cause := "fan-out capsule requires exact node"
				removeFault := func() {}
				if fault == "invalid_capsule" {
					var raw []byte
					if err := rt.DB.QueryRow(`SELECT capsule FROM fan_out_intents WHERE run_id=$1 AND semantic_path='stop-fixture-19'`, runID).Scan(&raw); err != nil {
						t.Fatal(err)
					}
					var capsule fanoutobligation.Capsule
					if err := json.Unmarshal(raw, &capsule); err != nil {
						t.Fatal(err)
					}
					capsule.NodeKey = ""
					raw, err := json.Marshal(capsule)
					if err != nil {
						t.Fatal(err)
					}
					if _, err := rt.DB.Exec(`UPDATE fan_out_intents SET capsule=$2 WHERE run_id=$1 AND semantic_path='stop-fixture-19'`, runID, string(raw)); err != nil {
						t.Fatal(err)
					}
				} else {
					cause = "issue2394_cancel_statement"
					removeFault = installIssue2394StopStatementFault(t, rt, runID, cause)
				}
				before := readIssue2394StopSnapshot(t, rt.DB, runID)
				key := uuid.NewString()
				response := requestServedJSONRPC(t, rt.Endpoint, "run.stop", map[string]any{"run_id": runID, "idempotency_key": key})
				if response.Error == nil {
					t.Fatal("faulted cancellation returned success")
				}
				// Check rollback before envelope assertions, so an untyped failure
				// cannot conceal a partial stop or falsely completed idempotency.
				assertIssue2394StopUnchanged(t, before, rt.DB, runID)
				requireServedControlAPIIdempotencyRows(t, rt.DB, rt.Backend, "run.stop", key, 0)
				_, ownerErr := rt.Runtime.RunControl.Stop(servedControlProofAuthorActivityContext(t, rt), runcontrol.TransitionRequest{RunID: runID})
				if ownerErr == nil || !issue2394ErrorChainContains(ownerErr, cause) {
					t.Errorf("selected stop owner lost %s cause: %v", fault, ownerErr)
				}
				assertIssue2394StopUnchanged(t, before, rt.DB, runID)
				ownerFailure, typed := runtimefailures.EnvelopeFromError(ownerErr)
				if !typed {
					t.Errorf("selected stop owner returned no typed stage envelope: %v", ownerErr)
				}
				details, ok := response.Error.Data["details"].(map[string]any)
				if !ok {
					t.Fatalf("missing HTTP error details: %+v", response.Error)
				}
				failure := decodeServedFailureEnvelope(t, details["failure"])
				if failure.Class != runtimefailures.ClassInternalFailure || failure.Retryable || failure.Detail.Code == "unclassified_runtime_error" {
					t.Errorf("M22 requires a classified nonretryable stop failure, got %+v", failure)
				}
				stage, _ := failure.Detail.Attributes["stage"].(string)
				wantStage := "decode_intent"
				if fault == "cancel_statement" {
					wantStage = "cancel_intents"
				}
				if stage != wantStage || failure.Detail.Code != "fan_out_cancellation_failed" || failure.Component != "runtime.fan_out" || failure.Operation != "cancel_run" {
					t.Errorf("M22 requires the selected owner's stable operation/stage, got %+v", failure)
				}
				if typed && !reflect.DeepEqual(failure, ownerFailure) {
					t.Errorf("HTTP changed the owner's failure: owner=%+v HTTP=%+v", ownerFailure, failure)
				}
				removeFault()
				rig.stop(t)
			})
		}
	}
}

func installIssue2394StopStatementFault(t *testing.T, rt servedControlProofRuntime, runID, cause string) func() {
	t.Helper()
	// A real database statement fails inside fan-out cancellation after run
	// delivery quiescence. The outer transaction must roll back both families.
	name := "issue2394_stop_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if rt.Backend == "sqlite" {
		_, err := rt.DB.Exec(fmt.Sprintf(`CREATE TRIGGER %s BEFORE UPDATE OF status ON fan_out_intents WHEN NEW.run_id='%s' AND NEW.status='canceled' BEGIN SELECT RAISE(ABORT,'%s'); END`, name, runID, cause))
		if err != nil {
			t.Fatal(err)
		}
		var once sync.Once
		remove := func() {
			once.Do(func() {
				// The process cleanup closes its SQLite DB later in LIFO order.
				if _, err := rt.DB.Exec(`DROP TRIGGER IF EXISTS ` + name); err != nil {
					t.Error(err)
				}
			})
		}
		t.Cleanup(remove)
		return remove
	}
	_, err := rt.DB.Exec(fmt.Sprintf(`CREATE FUNCTION %s() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION '%s'; END $$`, name, cause))
	if err != nil {
		t.Fatal(err)
	}
	_, err = rt.DB.Exec(fmt.Sprintf(`CREATE TRIGGER %s BEFORE UPDATE OF status ON fan_out_intents FOR EACH ROW WHEN (NEW.run_id='%s'::uuid AND NEW.status='canceled') EXECUTE FUNCTION %s()`, name, runID, name))
	if err != nil {
		t.Fatal(err)
	}
	var once sync.Once
	remove := func() {
		once.Do(func() {
			if _, err := rt.DB.ExecContext(context.Background(), `DROP TRIGGER IF EXISTS `+name+` ON fan_out_intents`); err != nil {
				t.Error(err)
			}
			if _, err := rt.DB.ExecContext(context.Background(), `DROP FUNCTION IF EXISTS `+name+`() `); err != nil {
				t.Error(err)
			}
		})
	}
	t.Cleanup(remove)
	return remove
}

func issue2394ErrorChainContains(err error, text string) bool {
	if err == nil {
		return false
	}
	if strings.Contains(err.Error(), text) {
		return true
	}
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		for _, cause := range joined.Unwrap() {
			if issue2394ErrorChainContains(cause, text) {
				return true
			}
		}
	}
	return issue2394ErrorChainContains(errors.Unwrap(err), text)
}
