package serveapp

import (
	"crypto/sha256"
	"fmt"
	"go/parser"
	"go/token"
	"os"
	"testing"
)

func TestLifecycleTemplateFutureForkOraclePreserved(t *testing.T) {
	const path = "testdata/future_capabilities/lifecycle_template_sibling_fork_test.go.txt"
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	// #642 must explicitly replace this preserved success oracle when enabling
	// dynamic execution; a current-policy refusal is not its success proof.
	const want = "0dfbf9e98c51a630e0694901b28841b0bfdb43a87570f57fc4b7ac94c3f557c8"
	if got := fmt.Sprintf("%x", sha256.Sum256(content)); got != want {
		t.Fatalf("T17's retained future assertions changed: sha256=%s want=%s", got, want)
	}
	if _, err := parser.ParseFile(token.NewFileSet(), path, content, parser.AllErrors); err != nil {
		t.Fatalf("future success oracle is not executable Go test source: %v", err)
	}
}
