package sourceblob

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/runphysical"
)

const Predecessor = "0ebf6b0862a1e168450d1b91df86d4ad218edb6f"
const Path = "tools/fixture-codemod/pipeline-observations/family145-source-blob-preimages.json"
const Digest = "3b865a7023939158cf390d9f0119480fef4d5b61c49f1d7223bc373159268c1c"

type Pin = runphysical.Pin

func Decode(raw []byte) ([]Pin, error) {
	if fmt.Sprintf("%x", sha256.Sum256(raw)) != Digest {
		return nil, fmt.Errorf("source blob complete manifest changed")
	}
	var pins []Pin
	if err := json.Unmarshal(raw, &pins); err != nil {
		return nil, err
	}
	if len(pins) != 4 {
		return nil, fmt.Errorf("source blob whole declaration inventory changed")
	}
	return pins, nil
}

var Rules = []runphysical.Rule{
	{"internal/serveapp/main_runtime_test.go", "startServedControlProofRuntimeWithFixture", []runphysical.Edit{
		{"var deliveryReader servedRunDeliveryReader", "var sourceReader sourceArtifactReader\nvar deliveryReader servedRunDeliveryReader"},
		{"deliveryReader = p.deps.DeliveryStore.SummarizeRun", "sourceReader = p.sourceReader\ndeliveryReader = p.deps.DeliveryStore.SummarizeRun"},
		{"return servedControlProofRuntime{ReadApplicationSnapshot: snapshotReader, ReadRunDeliveries: deliveryReader, ReceiverStateReader: receiverReader, Endpoint: endpoint, DB: servedDB", "return servedControlProofRuntime{SourceArtifacts: sourceReader, ReadApplicationSnapshot: snapshotReader, ReadRunDeliveries: deliveryReader, ReceiverStateReader: receiverReader, Endpoint: endpoint, DB: servedDB"},
		{"return servedControlProofRuntime{ReadApplicationSnapshot: snapshotReader, ReadRunDeliveries: deliveryReader, ReceiverStateReader: receiverReader, Endpoint: endpoint, DB: db", "return servedControlProofRuntime{SourceArtifacts: sourceReader, ReadApplicationSnapshot: snapshotReader, ReadRunDeliveries: deliveryReader, ReceiverStateReader: receiverReader, Endpoint: endpoint, DB: db"},
	}, 0},
	{"internal/serveapp/source_admission_fork_proof_test.go", "Test2376VerifyServeArtifactParityBothStores", []runphysical.Edit{
		expression("blob", "rt.DB.QueryRow(\"SELECT source_blob FROM source_artifacts WHERE bundle_hash=$1\", verified.Hash)", "selected.SourceArtifactStore()", "verified.Hash"),
	}, 1},
	{"internal/serveapp/reset_served_parity_test.go", "TestServedResetRetainClearAndHistoricalReplayBothStores", []runphysical.Edit{
		expression("originalSource", "proof.DB.QueryRow(\"SELECT source_blob FROM source_artifacts WHERE bundle_hash = $1\", proof.BundleHash)", "proof.SourceArtifacts", "proof.BundleHash"),
		expression("retainedSource", "proof.DB.QueryRow(\"SELECT source_blob FROM source_artifacts WHERE bundle_hash = $1\", proof.BundleHash)", "proof.SourceArtifacts", "proof.BundleHash"),
	}, 2},
	{"internal/serveapp/channel_source_lifecycle_supported_surface_test.go", "TestChannelSourceLifecyclePublicJourney", []runphysical.Edit{
		{"var deliveryReader servedRunDeliveryReader", "var sourceReader sourceArtifactReader\nvar deliveryReader servedRunDeliveryReader"},
		{"deliveryReader = p.deps.DeliveryStore.SummarizeRun", "sourceReader = p.sourceReader\ndeliveryReader = p.deps.DeliveryStore.SummarizeRun"},
		expression("originalSource", "db.QueryRow(`SELECT source_blob FROM source_artifacts WHERE bundle_hash=$1`, hash)", "sourceReader", "hash"),
		retained("changed process replaced retained source: %v"),
		retained("retained reset changed source bytes: %v"),
	}, 3},
}

func retained(failure string) runphysical.Edit {
	edit := expression("retained", "db.QueryRow(`SELECT source_blob FROM source_artifacts WHERE bundle_hash=$1`, hash)", "sourceReader", "hash")
	suffix := "; err != nil || !bytes.Equal(retained, originalSource) {\n" + strings.Repeat("\t", 7) + "t.Fatalf(\"" + failure + "\", err)\n" + strings.Repeat("\t", 6) + "}"
	edit.Before += suffix
	edit.After += suffix
	return edit
}

func expression(destination, query, reader, hash string) runphysical.Edit {
	return runphysical.Edit{Before: query + ".Scan(&" + destination + ")", After: "func() error { var err error; " + destination + ",err=readServedPersistedSourceBlob(context.Background(), " + reader + ", " + hash + "); return err }()"}
}
