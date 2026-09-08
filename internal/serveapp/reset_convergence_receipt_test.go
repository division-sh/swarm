package serveapp

import (
	"context"
	"testing"

	"github.com/google/uuid"
)

func TestRunServeResetFinalReceiptFailureRetryPreservesLiveSuccessorPostgres(t *testing.T) {
	proof := startServedSessionCleanupProof(t)
	if _, err := proof.DB.Exec(`CREATE FUNCTION reset_final_receipt_fault() RETURNS trigger LANGUAGE plpgsql AS $$
	BEGIN IF NEW.phase = 'completed' THEN RAISE EXCEPTION 'reset final receipt fault'; END IF; RETURN NEW; END $$;
	CREATE TRIGGER reset_final_receipt_fault BEFORE UPDATE ON runtime_reset_operations FOR EACH ROW EXECUTE FUNCTION reset_final_receipt_fault()`); err != nil {
		t.Fatal(err)
	}
	params := map[string]any{"include_source_artifacts": false, "idempotency_key": "final-receipt-" + uuid.NewString()}
	response := requestServedSessionCleanupMutation(t, proof, "runtime.nuke", params)
	if response.Error == nil {
		t.Fatal("final receipt failure was reported as success")
	}
	var phase string
	if err := proof.DB.QueryRow("SELECT phase FROM runtime_reset_operations").Scan(&phase); err != nil || phase != "containers_settled" {
		t.Fatalf("pending phase = %s, %v", phase, err)
	}
	if _, err := proof.DB.Exec("DROP TRIGGER reset_final_receipt_fault ON runtime_reset_operations; DROP FUNCTION reset_final_receipt_fault()"); err != nil {
		t.Fatal(err)
	}
	use, _, err := proof.Contexts.AcquireBundleHash(context.Background(), proof.BundleHash)
	if err != nil || use == nil {
		t.Fatalf("converged successor unavailable: %v", err)
	}
	successor := use.Runtime()
	grant, err := successor.CurrentStartupGrantEvidence()
	if err != nil {
		_ = use.Done()
		t.Fatal(err)
	}
	if err := use.Done(); err != nil {
		t.Fatal(err)
	}
	later := requireServedEventPublishRPCResult(t, proof.Endpoint, map[string]any{
		"event_name": "item.received", "bundle_hash": proof.BundleHash,
		"payload": map[string]any{"item_id": "after-uncertain-final-receipt"}, "idempotency_key": uuid.NewString(),
	})
	waitForServedEventPublishNodeDeliveryLifecycle(t, proof.DB, "postgres", later.RunID, later.EventID, proof.Probe)
	for i := 0; i < 2; i++ {
		retry := requestServedJSONRPC(t, proof.Endpoint, "runtime.nuke", params)
		if retry.Error != nil {
			t.Fatalf("retry %d: %+v", i, retry.Error)
		}
		use, _, err := proof.Contexts.AcquireBundleHash(context.Background(), proof.BundleHash)
		if err != nil || use == nil {
			t.Fatalf("retry withdrew successor: %v", err)
		}
		current := use.Runtime()
		currentGrant, grantErr := current.CurrentStartupGrantEvidence()
		if err := use.Done(); err != nil {
			t.Fatal(err)
		}
		if grantErr != nil || current != successor || currentGrant.GrantID != grant.GrantID {
			t.Fatalf("retry reconstructed live successor: %v", grantErr)
		}
		var count int
		if err := proof.DB.QueryRow("SELECT COUNT(*) FROM runs WHERE run_id = $1", later.RunID).Scan(&count); err != nil || count != 1 {
			t.Fatalf("retry changed later work: count=%d err=%v", count, err)
		}
	}
	if err := proof.DB.QueryRow("SELECT phase FROM runtime_reset_operations").Scan(&phase); err != nil || phase != "completed" {
		t.Fatalf("final receipt did not converge: %s, %v", phase, err)
	}
}
