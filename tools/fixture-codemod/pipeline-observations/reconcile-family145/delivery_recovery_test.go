package main

import (
	"bytes"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

const deliveryRecoveryAdapter = "internal/store/internal/backend/delivery/adapter.go"

func TestDeliveryRecoveryFiniteCodemodsReproduceCompleteOwnedCuts(t *testing.T) {
	root := filepath.Clean("../../../..")
	actualSource := deliveryRecoveryRead(t, filepath.Join(root, deliveryConformanceFile))
	actualOwner := deliveryRecoveryRead(t, filepath.Join(root, deliveryRecoveryAdapter))
	copy, source, owner := deliveryRecoveryCodemodCheckout(t, root)
	t.Chdir(copy)
	rewriteDeliveryRecoverySchema()
	rewriteDeliveryRecoveryClock()
	generatedSource := deliveryRecoveryRead(t, deliveryConformanceFile)
	generatedOwner := deliveryRecoveryRead(t, deliveryRecoveryAdapter)
	if got, want := deliveryRecoveryProbeCalls(t, generatedSource), deliveryRecoveryProbeCalls(t, actualSource); len(got) != 14 || !reflect.DeepEqual(got, want) {
		t.Fatal("fourteen exact probe calls differ from committed output")
	}
	for _, symbol := range []string{"deliveryConstraintQueries", "deliveryConstraintArguments", "DeliveryConformanceClockInput", "ProveDeliveryConformanceClockForTest", "deliveryConformanceClockAttempt", "validateDeliveryConformanceClockProof"} {
		if got, want := deliveryRecoveryDeclaration(t, generatedOwner, symbol), deliveryRecoveryDeclaration(t, actualOwner, symbol); got != want {
			t.Fatalf("generated native owner differs: %s", symbol)
		}
	}
	for _, run := range []func(){rewriteDeliveryRecoverySchema, rewriteDeliveryRecoveryClock} {
		deliveryRecoveryExpectPanic(t, run)
		if !bytes.Equal(deliveryRecoveryRead(t, deliveryConformanceFile), generatedSource) || !bytes.Equal(deliveryRecoveryRead(t, deliveryRecoveryAdapter), generatedOwner) {
			t.Fatal("reapplying a completed transform changed files")
		}
	}
	deliveryRecoveryWrite(t, deliveryConformanceFile, source)
	deliveryRecoveryExpectPanic(t, rewriteDeliveryRecoverySchema)
	if !bytes.Equal(deliveryRecoveryRead(t, deliveryConformanceFile), source) || !bytes.Equal(deliveryRecoveryRead(t, deliveryRecoveryAdapter), generatedOwner) {
		t.Fatal("existing owner catalog caused partial caller writes")
	}
	set, _, calls := deliverySchemaCalls(source)
	start, end := set.Position(calls[0].Pos()).Offset, set.Position(calls[0].End()).Offset
	duplicate := append(append(append([]byte{}, source[:start]...), []byte(node(calls[1]))...), source[end:]...)
	deliveryRecoveryWrite(t, deliveryConformanceFile, duplicate)
	deliveryRecoveryWrite(t, deliveryRecoveryAdapter, owner)
	deliveryRecoveryExpectPanic(t, rewriteDeliveryRecoverySchema)
	if !bytes.Equal(deliveryRecoveryRead(t, deliveryConformanceFile), duplicate) || !bytes.Equal(deliveryRecoveryRead(t, deliveryRecoveryAdapter), owner) {
		t.Fatal("duplicate schema case caused partial writes")
	}
	deliveryRecoveryHostileTransforms(t, source, owner)
}

func deliveryRecoveryHostileTransforms(t *testing.T, source, owner []byte) {
	t.Helper()
	for _, cut := range []struct {
		name, before, after string
		run                 func()
	}{
		{"label", `"event/run mismatch"`, `"foreign probe"`, rewriteDeliveryRecoverySchema},
		{"argument", "[]any{other.RunID(), proof.DeliveryID()}", "[]any{other.RunID(), other.RunID()}", rewriteDeliveryRecoverySchema},
		{"query", "SET run_id = $1::uuid WHERE delivery_id = $2::uuid", "SET run_id = $1::uuid WHERE delivery_id <> $2::uuid", rewriteDeliveryRecoverySchema},
		{"inventory", "assertDeliverySQLRejected(t, backend, \"event/run mismatch\"", "foreignProbe(t, backend, \"event/run mismatch\"", rewriteDeliveryRecoverySchema},
		{"clock", "1100 * time.Millisecond", "100 * time.Millisecond", rewriteDeliveryRecoveryClock},
	} {
		t.Run(cut.name, func(t *testing.T) {
			changed := bytes.Replace(source, []byte(cut.before), []byte(cut.after), 1)
			if bytes.Equal(changed, source) {
				t.Fatal("hostile cut did not reach signed source")
			}
			deliveryRecoveryWrite(t, deliveryConformanceFile, changed)
			deliveryRecoveryWrite(t, deliveryRecoveryAdapter, owner)
			deliveryRecoveryExpectPanic(t, cut.run)
			if !bytes.Equal(deliveryRecoveryRead(t, deliveryConformanceFile), changed) || !bytes.Equal(deliveryRecoveryRead(t, deliveryRecoveryAdapter), owner) {
				t.Fatal("changed predecessor caused partial transform writes")
			}
		})
	}
}

func deliveryRecoveryCodemodCheckout(t *testing.T, root string) (string, []byte, []byte) {
	t.Helper()
	copy := t.TempDir()
	gitdir, err := exec.Command("git", "-C", root, "rev-parse", "--absolute-git-dir").Output()
	if err != nil {
		t.Fatal(err)
	}
	deliveryRecoveryWrite(t, filepath.Join(copy, ".git"), []byte("gitdir: "+strings.TrimSpace(string(gitdir))+"\n"))
	var originals [][]byte
	for _, path := range []string{deliveryConformanceFile, deliveryRecoveryAdapter} {
		raw, err := exec.Command("git", "-C", root, "show", deliveryRecoveryPredecessor+":"+path).Output()
		if err != nil {
			t.Fatal(err)
		}
		deliveryRecoveryWrite(t, filepath.Join(copy, path), raw)
		originals = append(originals, raw)
	}
	return copy, originals[0], originals[1]
}

func deliveryRecoveryDeclaration(t *testing.T, source []byte, symbol string) string {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), "proof.go", source, 0)
	if err != nil {
		t.Fatal(err)
	}
	var found string
	for _, declaration := range file.Decls {
		ast.Inspect(declaration, func(n ast.Node) bool {
			match := false
			switch value := n.(type) {
			case *ast.FuncDecl:
				match = value.Name.Name == symbol
			case *ast.TypeSpec:
				match = value.Name.Name == symbol
			}
			if match {
				if found != "" {
					t.Fatalf("duplicate owner declaration %s", symbol)
				}
				found = node(declaration)
			}
			return true
		})
	}
	if found == "" {
		t.Fatalf("missing owner declaration %s", symbol)
	}
	return found
}

func deliveryRecoveryProbeCalls(t *testing.T, source []byte) []string {
	t.Helper()
	_, _, calls := deliverySchemaCalls(source)
	var out []string
	for _, call := range calls {
		out = append(out, node(call))
	}
	return out
}

func deliveryRecoveryExpectPanic(t *testing.T, run func()) {
	t.Helper()
	defer func() {
		if recover() == nil {
			t.Fatal("foreign or already migrated source accepted")
		}
	}()
	run()
}

func deliveryRecoveryRead(t *testing.T, path string) []byte {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func deliveryRecoveryWrite(t *testing.T, path string, raw []byte) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
}
