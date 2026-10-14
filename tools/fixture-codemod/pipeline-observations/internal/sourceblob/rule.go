package sourceblob

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"strings"

	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/requestkey"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/runphysical"
)

const Predecessor = "0ebf6b0862a1e168450d1b91df86d4ad218edb6f"
const Path = "tools/fixture-codemod/pipeline-observations/family145-source-blob-preimages.json"
const Digest = "3b865a7023939158cf390d9f0119480fef4d5b61c49f1d7223bc373159268c1c"
const FixtureTypeDigest = "04263600f4049181bb4f60cb8a1166a58f400cf57389a096ecfd016aa92e1be7"

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

func ValidateFixtureRole(raw []byte) error {
	file, err := parser.ParseFile(token.NewFileSet(), "fixture.go", raw, parser.AllErrors)
	if err != nil {
		return err
	}
	count := 0
	for _, declaration := range file.Decls {
		group, ok := declaration.(*ast.GenDecl)
		if !ok {
			continue
		}
		for _, spec := range group.Specs {
			if typed, ok := spec.(*ast.TypeSpec); ok && typed.Name.Name == "servedControlProofRuntime" {
				count++
				if fmt.Sprintf("%x", sha256.Sum256([]byte(requestkey.Node(typed)))) != FixtureTypeDigest {
					return fmt.Errorf("source blob shared fixture role/fields changed")
				}
			}
		}
	}
	if count != 1 {
		return fmt.Errorf("source blob shared fixture type count=%d want1", count)
	}
	return nil
}

func ValidateCurrentSource(pin Pin, raw []byte) error {
	if err := validatePinRule(pin); err != nil {
		return err
	}
	file, err := parser.ParseFile(token.NewFileSet(), pin.File, raw, parser.AllErrors)
	if err != nil {
		return err
	}
	count := 0
	for _, declaration := range file.Decls {
		if fn, ok := declaration.(*ast.FuncDecl); ok && fn.Recv == nil && fn.Name.Name == pin.Function {
			count++
			if requestkey.Node(fn) != pin.After {
				return fmt.Errorf("source blob current complete body changed: %s", pin.Function)
			}
		}
	}
	if count != 1 {
		return fmt.Errorf("source blob current declaration count=%d want1: %s", count, pin.Function)
	}
	return nil
}

func validatePinRule(pin Pin) error {
	for _, rule := range Rules {
		if rule.File != pin.File || rule.Function != pin.Function {
			continue
		}
		after, err := runphysical.Transform(rule, pin.Before)
		if err != nil || after != pin.After || pin.Calls != rule.Calls || fmt.Sprintf("%x", sha256.Sum256([]byte(pin.Before))) != pin.SHA256 {
			return fmt.Errorf("source blob complete pin/six substitutions changed: %s", pin.Function)
		}
		return nil
	}
	return fmt.Errorf("foreign source blob pin: %s/%s", pin.File, pin.Function)
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
