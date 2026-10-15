package store_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const debtSourceBlobManifest = "tools/fixture-codemod/pipeline-observations/family145-source-blob-preimages.json"
const debtSourceBlobManifestSHA = "3b865a7023939158cf390d9f0119480fef4d5b61c49f1d7223bc373159268c1c"

var debtSourceBlobPins = [4]debtRunIdentitySourcePin{
	{"internal/serveapp/main_runtime_test.go", "startServedControlProofRuntimeWithFixture", "b035c358d021cd3ecd15df307a334d83dc4d7373524abf6f44cd79ac4f64dcdb", "93aa87b9bcfee31bb86ad438275df0321fe25acb30606c39e6f1b62bcba11b69"},
	{"internal/serveapp/source_admission_fork_proof_test.go", "Test2376VerifyServeArtifactParityBothStores", "b65e5cc5168c5bee266d68a67f89c5d4cfa4a36ed6d1b8f6d9e50e9866074145", "85e59a3c73dca6acb15c544be7765726d57d9e691b83870c88f74fc3c53bd17a"},
	{"internal/serveapp/reset_served_parity_test.go", "TestServedResetRetainClearAndHistoricalReplayBothStores", "c822417546a88e925daa1c3c5f687a94197332dcd2d156cde3004a4882666813", "2679c1c9cf8c79a7a74d0ca1f7f362b0578b472cf2321dd93184f8689d20be7d"},
	{"internal/serveapp/channel_source_lifecycle_supported_surface_test.go", "TestChannelSourceLifecyclePublicJourney", "c4cb9bafbd0aaf286bea5fea11d967d195b8d4409278056eb5221e778d5f4f29", "06537487a7f93891181c544c1b3135eecb89d1e56b2eb9606a6a4be40b26f73d"},
}

var debtSourceBlobNativeFiles = [6][2]string{
	{"internal/serveapp/store_runtime.go", "70ba418b94dc41f32d5d8c47fe9e0d8651a3314a1c53d17c909ce2dc8c133d89"},
	{"internal/store/selected/accessors.go", "f73fc7bcfe9a4bbbad1badfce927ce425ee9410a6bdbe872fb6d62ee4a5a89e9"},
	{"internal/store/internal/runtimepersistence/source_artifact.go", "20d2bf865b5c9c980ecbbfea3d7e15f8cef7983e96775582a59bb88c62d51ceb"},
	{"internal/store/internal/sourceartifact/postgres.go", "7c4f1bb6032fb47cb8d6a4c76a25bcc8950dc59cbd37a98b129123348bdbe6c2"},
	{"internal/store/internal/sourceartifact/sqlite.go", "1b5e1a24dcaec7a9ebddeaf4e00431ba15a24c1b0ef23c89ea6e449450662b31"},
	{"internal/serveapp/persisted_source_blob_observation_test.go", "0bc04f7674d104ce6fe186bf0d1aea68c783948b3b8dcd5446c579a4f2d7fec4"},
}

// Ruling6159906599: source evidence only, for this reset body only. No census
// site, occurrence, signature, or physical source artifact is transformed.
func debtSourceBlobResetPrevious(root, file, function, current string) (string, error) {
	pin := debtSourceBlobPins[2]
	if file != pin.File || function != pin.Function {
		return current, nil
	}
	bodies, err := debtSourceBlobLiveSources(root)
	if err != nil {
		return "", err
	}
	return debtRunIdentityUniqueBodyInverse(bodies, pin, current)
}

func debtSourceBlobLiveSources(root string) ([]debtRunIdentitySourceBody, error) {
	raw, err := os.ReadFile(filepath.Join(root, debtSourceBlobManifest))
	if err != nil || debtBaselineChecksum(raw) != debtSourceBlobManifestSHA {
		return nil, fmt.Errorf("source blob complete manifest/provenance changed: %v", err)
	}
	var bodies []debtRunIdentitySourceBody
	if err := json.Unmarshal(raw, &bodies); err != nil || len(bodies) != len(debtSourceBlobPins) {
		return nil, fmt.Errorf("source blob complete inventory changed: %v", err)
	}
	for index, pin := range debtSourceBlobPins {
		body := bodies[index]
		if body.File != pin.File || body.Function != pin.Function || body.Calls != index {
			return nil, fmt.Errorf("source blob exact key/order/multiplicity changed")
		}
		declarations, err := debtCatchUpLaneDeclarations(root, pin.File)
		if err != nil {
			return nil, err
		}
		current, err := debtServedObservationSourcePrevious(root, pin.File, pin.Function, declarations[pin.Function])
		if err != nil {
			return nil, err
		}
		if _, err := debtRunIdentityUniqueBodyInverse(bodies, pin, current); err != nil {
			return nil, err
		}
	}
	if err := debtSourceBlobNativePins(root); err != nil {
		return nil, err
	}
	return bodies, nil
}

func debtSourceBlobNativePins(root string) error {
	for _, pin := range debtSourceBlobNativeFiles {
		raw, err := os.ReadFile(filepath.Join(root, pin[0]))
		if err != nil || debtBaselineChecksum(raw) != pin[1] {
			return fmt.Errorf("source blob original native owner/projection/bridge changed: %s: %v", pin[0], err)
		}
	}
	path := debtSourceBlobPins[0].File
	raw, err := os.ReadFile(filepath.Join(root, path))
	if err == nil {
		raw, err = debtCatchUpLaneSource(root, path, raw)
	}
	if err != nil {
		return err
	}
	file, err := parser.ParseFile(token.NewFileSet(), path, raw, parser.AllErrors)
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
				var out bytes.Buffer
				if err := format.Node(&out, token.NewFileSet(), typed); err != nil {
					return err
				}
				if debtBaselineChecksum(out.Bytes()) != "04263600f4049181bb4f60cb8a1166a58f400cf57389a096ecfd016aa92e1be7" {
					return fmt.Errorf("source blob exact selected fixture role/type changed")
				}
			}
		}
	}
	if count != 1 {
		return fmt.Errorf("source blob requires one exact shared fixture type")
	}
	return nil
}

func debtSourceBlobCopySourceEvidence(t *testing.T, root, copy string) {
	t.Helper()
	paths := []string{debtSourceBlobManifest}
	for _, pin := range debtSourceBlobNativeFiles {
		paths = append(paths, pin[0])
	}
	for _, path := range paths {
		raw, err := os.ReadFile(filepath.Join(root, path))
		if err != nil {
			t.Fatal(err)
		}
		debtWriteModuleSource(t, copy, path, string(raw))
	}
	bodies, err := debtSourceBlobLiveSources(root)
	if err != nil {
		t.Fatal(err)
	}
	for _, body := range bodies {
		debtSourceBlobCopyDeclaration(t, copy, body.File, body.Function, body.After)
	}
	path := debtSourceBlobPins[0].File
	raw, err := os.ReadFile(filepath.Join(root, path))
	if err == nil {
		raw, err = debtCatchUpLaneSource(root, path, raw)
	}
	if err != nil {
		t.Fatal(err)
	}
	file, err := parser.ParseFile(token.NewFileSet(), path, raw, parser.AllErrors)
	if err != nil {
		t.Fatal(err)
	}
	for _, declaration := range file.Decls {
		group, ok := declaration.(*ast.GenDecl)
		if !ok {
			continue
		}
		for _, spec := range group.Specs {
			if typed, ok := spec.(*ast.TypeSpec); ok && typed.Name.Name == "servedControlProofRuntime" {
				var out bytes.Buffer
				if err := format.Node(&out, token.NewFileSet(), typed); err != nil {
					t.Fatal(err)
				}
				raw, err := os.ReadFile(filepath.Join(copy, debtSourceBlobPins[0].File))
				if err != nil {
					t.Fatal(err)
				}
				debtWriteModuleSource(t, copy, debtSourceBlobPins[0].File, string(raw)+"\ntype "+out.String()+"\n")
			}
		}
	}
	debtServedObservationCopySourceEvidence(t, root, copy)
}

// Synthetic history controls retain every unrelated older declaration; copying
// the entire current file would introduce unrequested later composition layers.
func debtSourceBlobCopyDeclaration(t *testing.T, copy, path, name, after string) {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(copy, path))
	if os.IsNotExist(err) {
		raw = []byte("package serveapp\n")
	} else if err != nil {
		t.Fatal(err)
	}
	set := token.NewFileSet()
	file, err := parser.ParseFile(set, path, raw, parser.AllErrors)
	if err != nil {
		t.Fatal(err)
	}
	for _, declaration := range file.Decls {
		if fn, ok := declaration.(*ast.FuncDecl); ok && fn.Recv == nil && fn.Name.Name == name {
			start, end := set.Position(fn.Pos()).Offset, set.Position(fn.End()).Offset
			debtWriteModuleSource(t, copy, path, string(raw[:start])+after+string(raw[end:]))
			return
		}
	}
	debtWriteModuleSource(t, copy, path, string(raw)+"\n"+after+"\n")
}

func TestPersistenceAuthorityDebtSourceBlobResetInverseChecksLiveSourceAndChangesNoAccounting(t *testing.T) {
	root := persistenceAuthorityRepoRoot(t)
	bodies, err := debtSourceBlobLiveSources(root)
	if err != nil {
		t.Fatal(err)
	}
	pin, body := debtSourceBlobPins[2], bodies[2]
	previous, err := debtSourceBlobResetPrevious(root, pin.File, pin.Function, body.After)
	if err != nil || previous != body.Before {
		t.Fatal("exact reset inverse refused:", err)
	}
	for _, key := range [][2]string{{"foreign.go", pin.Function}, {pin.File, "foreignFunction"}, {debtSourceBlobPins[0].File, debtSourceBlobPins[0].Function}} {
		if got, err := debtSourceBlobResetPrevious(root, key[0], key[1], body.After); err != nil || got != body.After {
			t.Fatal("foreign declaration received reset inverse:", err)
		}
	}
	for _, cut := range []string{"readServedPersistedSourceBlob", "proof.SourceArtifacts", "context.Background()", "BundleHash", "bytes.Equal", "later.RunID", "lease.Done", "requestServedJSONRPC", "t.Fatalf"} {
		mutant := strings.Replace(body.After, cut, "foreignSourceBlobCut", 1)
		if mutant == body.After {
			t.Fatal("source blob hostile cut absent:", cut)
		}
		if _, err := debtSourceBlobResetPrevious(root, pin.File, pin.Function, mutant); err == nil {
			t.Fatal("changed reset owner/hash/context/bytes/assertion/replay/lease accepted:", cut)
		}
	}
	for _, native := range debtSourceBlobNativeFiles {
		copy := t.TempDir()
		debtSourceBlobCopySourceEvidence(t, root, copy)
		raw, err := os.ReadFile(filepath.Join(copy, native[0]))
		if err != nil {
			t.Fatal(err)
		}
		debtWriteModuleSource(t, copy, native[0], string(raw)+"\n// foreign native source\n")
		if _, err := debtSourceBlobResetPrevious(copy, pin.File, pin.Function, body.After); err == nil {
			t.Fatal("supplied exact historical body bypassed a changed live native source:", native[0])
		}
	}
	debtSourceBlobRejectChangedLiveSources(t, root, bodies)
}

func debtSourceBlobRejectChangedLiveSources(t *testing.T, root string, bodies []debtRunIdentitySourceBody) {
	t.Helper()
	reset := debtSourceBlobPins[2]
	for _, body := range bodies {
		copy := t.TempDir()
		debtSourceBlobCopySourceEvidence(t, root, copy)
		declarations, err := debtServedDebugDeclarations(filepath.Join(copy, body.File))
		current := declarations[body.Function]
		if err != nil || current == "" {
			t.Fatal("source blob live hostility lost exact declaration:", body.Function, err)
		}
		mutant := strings.Replace(current, "{", "{foreignLiveSource();", 1)
		debtSourceBlobCopyDeclaration(t, copy, body.File, body.Function, mutant)
		changed, err := debtServedDebugDeclarations(filepath.Join(copy, body.File))
		if err != nil || changed[body.Function] == current {
			t.Fatal("source blob hostile mutation did not execute:", body.Function, err)
		}
		if _, err := debtSourceBlobResetPrevious(copy, reset.File, reset.Function, bodies[2].After); err == nil {
			t.Fatal("supplied exact result bypassed changed live capture/assertion:", body.Function)
		}
	}
	if _, err := debtSourceBlobResetPrevious(root, reset.File, reset.Function, bodies[2].Before); err == nil {
		t.Fatal("supplied old history substituted for actual current reset source")
	}
	copy := t.TempDir()
	debtSourceBlobCopySourceEvidence(t, root, copy)
	raw, err := os.ReadFile(filepath.Join(copy, debtSourceBlobPins[0].File))
	if err != nil {
		t.Fatal(err)
	}
	debtWriteModuleSource(t, copy, debtSourceBlobPins[0].File, strings.ReplaceAll(string(raw), "sourceArtifactReader", "foreignSourceArtifactRole"))
	if _, err := debtSourceBlobResetPrevious(copy, reset.File, reset.Function, bodies[2].After); err == nil {
		t.Fatal("actual shared fixture role was not checked before reset inversion")
	}
}
