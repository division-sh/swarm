package runidentity

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"

	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/runphysical"
)

const Predecessor = "9f90349d64445231416b81103e2daea6f2f4133d"
const Path = "tools/fixture-codemod/pipeline-observations/family145-run-identity-preimages.json"
const Digest = "23c82ec70c3225c2d92dd639d51ce592b0655cb9caa18856240810474dabef1f"

type Pin = runphysical.Pin

func Decode(raw []byte) ([]Pin, error) {
	if fmt.Sprintf("%x", sha256.Sum256(raw)) != Digest {
		return nil, fmt.Errorf("run identity complete source manifest changed")
	}
	var pins []Pin
	if err := json.Unmarshal(raw, &pins); err != nil {
		return nil, err
	}
	if len(pins) != 5 {
		return nil, fmt.Errorf("run identity declaration inventory changed")
	}
	return pins, nil
}

var Rules = []runphysical.Rule{
	{"internal/serveapp/reset_served_parity_test.go", "TestServedResetRetainClearAndHistoricalReplayBothStores", []runphysical.Edit{
		{"var beforeJoin int", ""},
		count("proof.DB", "proof.ReceiverStateReader", "initial.RunID", "beforeJoin", false),
		{"var oldRuns, artifacts int", "var artifacts int"},
		count("proof.DB", "proof.ReceiverStateReader", "initial.RunID", "oldRuns", false),
		{"var count int\n\t\t\t\t\t\tif err := proof.DB.QueryRowContext(context.Background(), \"SELECT COUNT(*) FROM runs WHERE run_id = $1\", laterRun).Scan(&count); err", "count, err := readServedRunPhysicalIdentity(proof.ReceiverStateReader, laterRun)\nif err"},
	}, 3},
	{"internal/serveapp/reset_startup_recovery_test.go", "TestServedResetPendingStartupUsesAdmittedSourcesBothStores", []runphysical.Edit{
		{"proof.DB, proof.Postgres, proof.SQLite = selectedRuntimeStoreForTest(t, persistence)", "proof.DB, proof.Postgres, proof.SQLite = selectedRuntimeStoreForTest(t, persistence)\nproof.ReceiverStateReader = requireReceiverProofStateReader(t, persistence.deps.EventStore)"},
		{"var predecessor int", ""},
		count("proof.DB", "proof.ReceiverStateReader", "initial.RunID", "predecessor", false),
	}, 1},
	{"internal/serveapp/lifecycle_diagnostic_recovery_test.go", "TestLifecycleDiagnosticServedCrashRecoveryAndResetBothStores", []runphysical.Edit{
		{"proof := servedControlProofRuntime{Endpoint: first.endpoint(t) + \"/v1/rpc\", DB: db, Backend: backendName, BundleHash: hash}", "proof := servedControlProofRuntime{Endpoint: first.endpoint(t) + \"/v1/rpc\", DB: db, Backend: backendName, BundleHash: hash, ReceiverStateReader: reader}"},
		{"var oldRuns, laterRuns int", ""},
		count("db", "reader", "initial.RunID", "oldRuns", false),
		count("db", "reader", "later.RunID", "laterRuns", false),
	}, 2},
	{"internal/serveapp/reset_convergence_receipt_test.go", "proveServedResetFinalReceiptFailure", []runphysical.Edit{
		{"var count int", ""},
		count("proof.DB", "proof.ReceiverStateReader", "later.RunID", "count", false),
	}, 1},
	{"internal/serveapp/selected_reset_admission_test.go", "TestSelectedResetPreparationAdmissionOrderBothStores", []runphysical.Edit{
		{"var count int", ""},
		count("rt.DB", "rt.ReceiverStateReader", "seed.RunID", "count", true),
	}, 1},
}

func count(db, reader, run, destination string, tight bool) runphysical.Edit {
	query := "SELECT COUNT(*) FROM runs WHERE run_id = $1"
	if tight {
		query = "SELECT COUNT(*) FROM runs WHERE run_id=$1"
	}
	return runphysical.Edit{
		Before: "if err := " + db + ".QueryRow(\"" + query + "\", " + run + ").Scan(&" + destination + "); err",
		After:  destination + ", err := readServedRunPhysicalIdentity(" + reader + ", " + run + ")\nif err",
	}
}
