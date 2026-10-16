package selectedreadrole

import (
	"bytes"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestCatchUpNewMasterDeclarationStaysLiveAndCannotMaskMalformedHistory(t *testing.T) {
	const current = "func newMasterDeclaration() { actualOwner() }"
	const prior = "package proof\nfunc oldLaneDeclaration() { originalOwner() }\n"
	actual, err := catchUpHistoricalDeclaration([]byte(prior), "newMasterDeclaration", current)
	if err != nil || actual != current {
		t.Fatal("an unrelated new declaration acquired a historical inverse:", err)
	}
	for _, malformed := range []string{
		"package proof\nfunc newMasterDeclaration(",
		"package proof\nfunc newMasterDeclaration(){}\nfunc newMasterDeclaration(){}",
	} {
		if _, err := catchUpHistoricalDeclaration([]byte(malformed), "newMasterDeclaration", current); err == nil {
			t.Fatal("missing-source handling hid malformed or duplicate history")
		}
	}
	previous, err := catchUpHistoricalDeclaration([]byte(prior), "oldLaneDeclaration", current)
	if err != nil || previous == current {
		t.Fatal("the exact historical declaration lost its original inverse:", err)
	}
}

func TestCatchUpNewMasterDeclarationUsesActualCopiedFunctionPath(t *testing.T) {
	root := filepath.Clean("../../../../..")
	const path = "internal/serveapp/dynamic_topology_startup_context_test.go"
	const name = "loadServeDynamicTopologySource"
	raw, err := os.ReadFile(filepath.Join(root, path))
	if err != nil {
		t.Fatal(err)
	}
	current, err := Body(raw, name)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := CatchUp2542HistoricalFunction(root, path, name, current); err != nil || got != current {
		t.Fatal("new master declaration acquired a false lane inverse:", err)
	}
	copy := t.TempDir()
	gitdir, err := exec.Command("git", "-C", root, "rev-parse", "--absolute-git-dir").Output()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(copy, ".git"), []byte("gitdir: "+strings.TrimSpace(string(gitdir))+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	actualPath := filepath.Join(copy, path)
	if err := os.MkdirAll(filepath.Dir(actualPath), 0700); err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, path, raw, parser.AllErrors)
	if err != nil {
		t.Fatal(err)
	}
	var mutant []byte
	for _, decl := range file.Decls {
		if fn, ok := decl.(*ast.FuncDecl); ok && fn.Name.Name == name {
			at := fset.Position(fn.Body.Lbrace).Offset + 1
			mutant = append(append(append([]byte(nil), raw[:at]...), []byte(" unexpectedRawCaller();")...), raw[at:]...)
		}
	}
	if bytes.Equal(mutant, raw) {
		t.Fatal("extra-caller control did not change the actual declaration")
	}
	for _, source := range [][]byte{raw, mutant} {
		assertCatchUpCopiedNewDeclaration(t, copy, path, name, source)
	}
	if err := os.Remove(actualPath); err != nil {
		t.Fatal(err)
	}
	if _, err := CatchUp2542HistoricalFunction(copy, path, name, current); err == nil {
		t.Fatal("warm historical cache hid a missing copied current file")
	}
}

func assertCatchUpCopiedNewDeclaration(t *testing.T, root, path, name string, source []byte) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(root, path), source, 0600); err != nil {
		t.Fatal(err)
	}
	current, err := Body(source, name)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := CatchUp2542HistoricalFunction(root, path, name, current); err != nil || got != current {
		t.Fatal("actual changed new declaration/extra caller was hidden by a historical inverse:", err)
	}
}

func TestCatchUpRetiredHistoricalInputsRemainExactAndNeverCertifyAlteredLiveSource(t *testing.T) {
	const root = "../../../../.."
	for _, pin := range []struct{ path, name string }{
		{"internal/serveapp/issue2277_audit_probe_test.go", "TestAudit2277ForkCompletionLoss"},
		{"internal/runtime/node_delivery_startup_recovery_test.go", "assertExpiredNodeDeliveryAttemptHistory"},
	} {
		t.Run(pin.name, func(t *testing.T) {
			actual, err := os.ReadFile(filepath.Join(root, pin.path))
			if err != nil {
				t.Fatal(err)
			}
			prior, err := CatchUp2542HistoricalSource(root, pin.path, actual)
			if err != nil {
				t.Fatal(err)
			}
			historical, err := Body(prior, pin.name)
			if err != nil {
				t.Fatal(err)
			}
			if got, err := CatchUp2542HistoricalFunction(root, pin.path, pin.name, historical); err != nil || got != historical {
				t.Fatal("exact already-historical declaration was lost:", err)
			}
			for _, changed := range [][]byte{
				append(bytes.Clone(actual), []byte("\nfunc newRawCaller(){ bypassOwner() }\n")...),
				append(bytes.Clone(actual), []byte("\nfunc "+pin.name+"(){}\nfunc "+pin.name+"(){}\n")...),
				[]byte("package broken\nfunc {"),
			} {
				if _, err := catchUpRetiredHistoricalInput(root, pin.path, pin.name, historical, changed, os.ErrNotExist); err == nil {
					t.Fatal("changed/ambiguous actual source was hidden by a retired historical input")
				}
			}
			if _, err := catchUpRetiredHistoricalInput(root, pin.path, pin.name, historical+"\n", actual, os.ErrNotExist); err == nil {
				t.Fatal("changed historical input acquired retired-declaration admission")
			}
		})
	}
}
