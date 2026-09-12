package serveapp

import (
	"crypto/sha256"
	"fmt"
	"os"
	"testing"
)

func TestRetainedJoinFutureSuccessArtifactPreserved(t *testing.T) {
	raw, err := os.ReadFile("testdata/future_capabilities/fork_retained_join_success_test.go.txt")
	if err != nil {
		t.Fatal(err)
	}
	// Exact pre-split source at d4e77eadb: both fixtures and every original
	// source, generation, child ownership, and duplicate/isolation assertion.
	const want = "a24e4b0a28a643c56bda44ae35805588dae1e6cc1a90438efa9e07c15055ba59"
	if got := fmt.Sprintf("%x", sha256.Sum256(raw)); got != want {
		t.Fatalf("#642 future-success evidence changed: sha256=%s, want %s", got, want)
	}
}
