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
	{File: "internal/serveapp/reset_served_parity_test.go", Function: "TestServedResetRetainClearAndHistoricalReplayBothStores", Edits: []runphysical.Edit{
		{Before: "var beforeJoin int", After: ""},
		count("proof.DB", "proof.ReceiverStateReader", "initial.RunID", "beforeJoin", false),
		{Before: "var oldRuns, artifacts int", After: "var artifacts int"},
		count("proof.DB", "proof.ReceiverStateReader", "initial.RunID", "oldRuns", false),
		{Before: "var count int\n\t\t\t\t\t\tif err := proof.DB.QueryRowContext(context.Background(), \"SELECT COUNT(*) FROM runs WHERE run_id = $1\", laterRun).Scan(&count); err", After: "count, err := readServedRunPhysicalIdentity(proof.ReceiverStateReader, laterRun)\nif err"},
	}, Calls: 3},
	{File: "internal/serveapp/reset_startup_recovery_test.go", Function: "TestServedResetPendingStartupUsesAdmittedSourcesBothStores", Edits: []runphysical.Edit{
		{Before: "proof.DB, proof.Postgres, proof.SQLite = selectedRuntimeStoreForTest(t, persistence)", After: "proof.DB, proof.Postgres, proof.SQLite = selectedRuntimeStoreForTest(t, persistence)\nproof.ReceiverStateReader = requireReceiverProofStateReader(t, persistence.deps.EventStore)"},
		{Before: "var predecessor int", After: ""},
		count("proof.DB", "proof.ReceiverStateReader", "initial.RunID", "predecessor", false),
	}, Calls: 1},
	{File: "internal/serveapp/lifecycle_diagnostic_recovery_test.go", Function: "TestLifecycleDiagnosticServedCrashRecoveryAndResetBothStores", Edits: []runphysical.Edit{
		{Before: "proof := servedControlProofRuntime{Endpoint: first.endpoint(t) + \"/v1/rpc\", DB: db, Backend: backendName, BundleHash: hash}", After: "proof := servedControlProofRuntime{Endpoint: first.endpoint(t) + \"/v1/rpc\", DB: db, Backend: backendName, BundleHash: hash, ReceiverStateReader: reader}"},
		{Before: "var oldRuns, laterRuns int", After: ""},
		count("db", "reader", "initial.RunID", "oldRuns", false),
		count("db", "reader", "later.RunID", "laterRuns", false),
	}, Calls: 2},
	{File: "internal/serveapp/reset_convergence_receipt_test.go", Function: "proveServedResetFinalReceiptFailure", Edits: []runphysical.Edit{
		{Before: "var count int", After: ""},
		count("proof.DB", "proof.ReceiverStateReader", "later.RunID", "count", false),
	}, Calls: 1},
	{File: "internal/serveapp/selected_reset_admission_test.go", Function: "TestSelectedResetPreparationAdmissionOrderBothStores", Edits: []runphysical.Edit{
		{Before: "var count int", After: ""},
		count("rt.DB", "rt.ReceiverStateReader", "seed.RunID", "count", true),
	}, Calls: 1},
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
