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
	"sync"
	"testing"

	"github.com/division-sh/swarm/internal/checkoutsource"
)

const debtStandingControlManifest = "tools/fixture-codemod/pipeline-observations/family145-standing-ingress-control-preimages.json"
const debtStandingControlDigest = "ea16d7efc8ff4471bebc5b5e2af94a833c10a4650586e60264b9d057e5edb625"
const debtStandingControlPredecessor = "366ef25c98c0ec629354038246e7a30b214d7971"

var debtStandingControlCache sync.Map

// Rulings6186092411/6186734979: five exact inverses, never census/debt credit.
var debtStandingControlKeys = [5][2]string{
	{"internal/serveapp/main_runtime_test.go", "requireServedStoppedPendingDelivery"},
	{"internal/serveapp/main_runtime_test.go", "runServedRunControlLifecycleProof"},
	{"internal/serveapp/main_runtime_test.go", "runServedRuntimeIngressControlLifecycleProof"},
	{"internal/serveapp/standing_ingress_supported_surface_test.go", "runServedStandingServiceLifecycleBackendProof"},
	{"internal/serveapp/standing_ingress_dormancy_supported_surface_test.go", "TestDormantIngressProvisionRestartSignedInputBothStores"},
}

func debtStandingControlSourcePrevious(root, file, function, current string) (string, error) {
	selected := false
	for _, key := range debtStandingControlKeys {
		selected = selected || key == [2]string{file, function}
	}
	if !selected {
		return current, nil
	}
	pins, err := debtStandingControlLiveSources(root)
	if err != nil {
		return "", err
	}
	for _, pin := range pins {
		if pin.File == file && pin.Function == function {
			if current != pin.After {
				return "", fmt.Errorf("standing-control supplied source is not its actual live endpoint: %s", function)
			}
			return pin.Before, nil
		}
	}
	return "", fmt.Errorf("standing-control exact key disappeared: %s", function)
}

func debtStandingControlLiveSources(root string) ([]debtRunIdentitySourceBody, error) {
	raw, err := os.ReadFile(filepath.Join(root, debtStandingControlManifest))
	if err != nil || debtBaselineChecksum(raw) != debtStandingControlDigest {
		return nil, fmt.Errorf("standing-control complete29-row manifest changed: %v", err)
	}
	var pins []debtRunIdentitySourceBody
	if err := json.Unmarshal(raw, &pins); err != nil || len(pins) != 29 {
		return nil, fmt.Errorf("standing-control declaration inventory changed: %v", err)
	}
	files, key, err := debtStandingControlCurrentFiles(root, pins)
	if err != nil {
		return nil, err
	}
	gitIdentity, err := debtStandingControlGitIdentity(root)
	if err != nil {
		return nil, err
	}
	key = root + "/" + gitIdentity + "/" + key
	if _, ok := debtStandingControlCache.Load(key); ok {
		return pins, nil
	}
	previous := map[string]map[string]string{}
	seen := map[[2]string]bool{}
	for _, pin := range pins {
		identity := [2]string{pin.File, pin.Function}
		if seen[identity] || pin.Before == "" || pin.After == "" || pin.Before == pin.After {
			return nil, fmt.Errorf("standing-control duplicate or malformed declaration: %s", pin.Function)
		}
		seen[identity] = true
		if previous[pin.File] == nil {
			raw, err := debtGit(root, "show", debtStandingControlPredecessor+":"+pin.File)
			if err != nil {
				return nil, err
			}
			previous[pin.File], err = debtStandingControlDeclarations(raw)
			if err != nil {
				return nil, err
			}
		}
		if previous[pin.File][pin.Function] != pin.Before || files[pin.File][pin.Function] != pin.After {
			return nil, fmt.Errorf("standing-control signed predecessor/live sibling changed: %s", pin.Function)
		}
	}
	for _, identity := range debtStandingControlKeys {
		if !seen[identity] {
			return nil, fmt.Errorf("standing-control authorized key is missing: %s", identity[1])
		}
	}
	debtStandingControlCache.Store(key, true)
	return pins, nil
}

func debtStandingControlGitIdentity(root string) (string, error) {
	path := filepath.Join(root, ".git")
	info, err := os.Stat(path)
	if err != nil {
		return "", err
	}
	if info.IsDir() {
		return fmt.Sprintf("directory:%d:%d", info.Size(), info.ModTime().UnixNano()), nil
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	return debtBaselineChecksum(raw), nil
}

func debtStandingControlCurrentFiles(root string, pins []debtRunIdentitySourceBody) (map[string]map[string]string, string, error) {
	files := map[string]map[string]string{}
	var identity strings.Builder
	for _, pin := range pins {
		if files[pin.File] != nil {
			continue
		}
		path := filepath.Join(root, pin.File)
		owned, err := checkoutsource.Member(root, path)
		if err != nil || !owned {
			return nil, "", fmt.Errorf("standing-control source is not owned: %s: %v", pin.File, err)
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return nil, "", err
		}
		files[pin.File], err = debtCatchUpLaneDeclarations(root, pin.File)
		if err != nil {
			return nil, "", err
		}
		fmt.Fprintf(&identity, "%s:%s\n", pin.File, debtBaselineChecksum(raw))
	}
	return files, debtBaselineChecksum([]byte(identity.String())), nil
}

func debtStandingControlDeclarations(raw []byte) (map[string]string, error) {
	file, err := parser.ParseFile(token.NewFileSet(), "signed-source.go", raw, parser.AllErrors)
	if err != nil {
		return nil, err
	}
	declarations := map[string]string{}
	for _, declaration := range file.Decls {
		fn, ok := declaration.(*ast.FuncDecl)
		if !ok || fn.Recv != nil {
			continue
		}
		if _, duplicate := declarations[fn.Name.Name]; duplicate {
			return nil, fmt.Errorf("duplicate signed standing-control source: %s", fn.Name.Name)
		}
		var out bytes.Buffer
		if err := format.Node(&out, token.NewFileSet(), fn); err != nil {
			return nil, err
		}
		declarations[fn.Name.Name] = out.String()
	}
	return declarations, nil
}

func debtStandingControlCopySources(t *testing.T, root, copy string) {
	t.Helper()
	pins, err := debtStandingControlLiveSources(root)
	if err != nil {
		t.Fatal(err)
	}
	paths := map[string]bool{debtStandingControlManifest: true}
	for _, pin := range pins {
		paths[pin.File] = true
	}
	for path := range paths {
		raw, err := os.ReadFile(filepath.Join(root, path))
		if err != nil {
			t.Fatal(err)
		}
		debtWriteModuleSource(t, copy, path, string(raw))
	}
	gitdir, err := debtGit(root, "rev-parse", "--absolute-git-dir")
	if err != nil {
		t.Fatal(err)
	}
	// Read-only test evidence uses the same authenticated object database.
	debtWriteModuleSource(t, copy, ".git", "gitdir: "+strings.TrimSpace(string(gitdir))+"\n")
}

func TestPersistenceAuthorityDebtStandingControlFiveInversesAndHostileEvidence(t *testing.T) {
	root := persistenceAuthorityRepoRoot(t)
	pins, err := debtStandingControlLiveSources(root)
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range debtStandingControlKeys {
		proveDebtStandingControlInverse(t, root, pins, key)
	}
	for _, cut := range []string{"missing manifest", "changed manifest", "duplicate key", "changed sibling", "missing sibling", "missing history"} {
		t.Run(cut, func(t *testing.T) {
			copy := t.TempDir()
			debtStandingControlCopySources(t, root, copy)
			mutateDebtStandingControlEvidence(t, copy, pins, cut)
			pin := pins[0]
			for _, row := range pins {
				if row.Function == debtStandingControlKeys[0][1] {
					pin = row
				}
			}
			if _, err := debtStandingControlSourcePrevious(copy, pin.File, pin.Function, pin.After); err == nil {
				t.Fatal("foreign standing-control evidence was accepted")
			}
		})
	}
}

func TestPersistenceAuthorityDebtStandingControlCollectorDependenciesStayExact(t *testing.T) {
	root := persistenceAuthorityRepoRoot(t)
	for _, check := range []struct {
		name string
		run  func(string) error
	}{
		{"run identity", debtRunIdentityNativeSourcePins},
		{"rollback native", debtPublicRollbackNativePins},
		{"request key", debtRequestKeySourcePins},
		{"served debug", debtServedDebugSourcePins},
	} {
		t.Run(check.name, func(t *testing.T) {
			if err := check.run(root); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func proveDebtStandingControlInverse(t *testing.T, root string, pins []debtRunIdentitySourceBody, key [2]string) {
	t.Helper()
	for _, pin := range pins {
		if [2]string{pin.File, pin.Function} != key {
			continue
		}
		before, err := debtStandingControlSourcePrevious(root, pin.File, pin.Function, pin.After)
		if err != nil || before != pin.Before {
			t.Fatalf("exact five-key inverse: %s %v", pin.Function, err)
		}
		for _, wrong := range []string{pin.Before, pin.After + "\n", ""} {
			if _, err := debtStandingControlSourcePrevious(root, pin.File, pin.Function, wrong); err == nil {
				t.Fatalf("stale/foreign/missing supplied source accepted: %s", pin.Function)
			}
		}
		return
	}
	t.Fatal("exact authorized standing-control source was not tested")
}

func mutateDebtStandingControlEvidence(t *testing.T, root string, pins []debtRunIdentitySourceBody, cut string) {
	t.Helper()
	manifest := filepath.Join(root, debtStandingControlManifest)
	switch cut {
	case "missing manifest":
		if err := os.Remove(manifest); err != nil {
			t.Fatal(err)
		}
	case "changed manifest":
		raw, err := os.ReadFile(manifest)
		if err != nil {
			t.Fatal(err)
		}
		debtWriteModuleSource(t, root, debtStandingControlManifest, string(raw)+"\n")
	case "duplicate key":
		raw, err := json.Marshal(append(pins, pins[0]))
		if err != nil {
			t.Fatal(err)
		}
		debtWriteModuleSource(t, root, debtStandingControlManifest, string(raw))
	case "changed sibling":
		pin := pins[0]
		debtSourceBlobCopyDeclaration(t, root, pin.File, pin.Function, pin.Before)
	case "missing sibling":
		if err := os.Remove(filepath.Join(root, pins[0].File)); err != nil {
			t.Fatal(err)
		}
	case "missing history":
		if err := os.Remove(filepath.Join(root, ".git")); err != nil {
			t.Fatal(err)
		}
	}
}
