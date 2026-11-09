package pipelinepersistence

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/division-sh/swarm/tests/support/testutil/sourceartifactfixture"
)

func TestRetainedTurnSourceMemoKeepsOnlyExactCompilerOutput(t *testing.T) {
	artifact := sourceartifactfixture.Artifact()
	rooted, _ := sourceartifactfixture.Rooted(t, "fixture", artifact)
	var memo retainedTurnSourceMemo
	first, err := memo.load(t.Context(), rooted)
	if err != nil {
		t.Fatal(err)
	}
	bundle, _ := semanticview.Bundle(first)
	if !bundle.FanOutPlansArePrepared() {
		t.Fatal("published source still needs mutable preparation")
	}
	again, err := memo.load(t.Context(), rooted)
	if err != nil || again != first {
		t.Fatalf("same rooted source recompiled: %v", err)
	}
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	if got, err := memo.load(canceled, rooted); got != nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("warm memo swallowed cancellation: %v", err)
	}
	if got, err := memo.load(t.Context(), sourceartifact.RootedSource{}); got != nil || err == nil {
		t.Fatal("warm memo accepted absent root evidence")
	}
	other, _ := sourceartifactfixture.Rooted(t, "other", artifact)
	different, err := memo.load(t.Context(), other)
	if err != nil || different == first {
		t.Fatalf("same hash crossed root identity: %v", err)
	}
	otherBundle, _ := semanticview.Bundle(different)
	otherRoot, err := otherBundle.RootedSource()
	if err != nil || !otherRoot.Admission().Equal(other.Admission()) {
		t.Fatalf("compiled source lost exact root: %v", err)
	}
	replaced, err := memo.load(t.Context(), rooted)
	if err != nil || replaced == first || replaced == different {
		t.Fatalf("single-entry memo retained an evicted compilation: %v", err)
	}
	memo.specDigest = "different compiler inputs"
	updated, err := memo.load(t.Context(), rooted)
	if err != nil || updated == replaced {
		t.Fatalf("changed compiler inputs reused old output: %v", err)
	}
	badArtifact, err := sourceartifact.AdmitMembers(map[string][]byte{
		"manifest.yaml": []byte("version: 1.0.0\nplatform_version: '>999.0.0'\n"),
		"schema.yaml":   []byte("stages: []\n"),
	})
	if err != nil {
		t.Fatal(err)
	}
	badRoot, _ := sourceartifactfixture.Rooted(t, "invalid", badArtifact)
	for range 2 {
		if got, err := memo.load(t.Context(), badRoot); err == nil || got != nil || memo.source != updated {
			t.Fatalf("failed compile published or displaced valid output: %v", err)
		}
	}
	changedArtifact, err := sourceartifact.AdmitMembers(map[string][]byte{
		"manifest.yaml": []byte("version: 2.0.0\nplatform_version: '*'\n"),
		"schema.yaml":   []byte("stages: []\n"),
	})
	if err != nil {
		t.Fatal(err)
	}
	changedRoot, _ := sourceartifactfixture.Rooted(t, "fixture", changedArtifact)
	changed, err := memo.load(t.Context(), changedRoot)
	if err != nil || changed == updated || changed.WorkflowVersion() != "2.0.0" {
		t.Fatalf("same root crossed changed source bytes: %v", err)
	}
	var concurrent retainedTurnSourceMemo
	results := make([]semanticview.Source, 16)
	loadErrors := make([]error, len(results))
	var joined sync.WaitGroup
	for i := range results {
		joined.Add(1)
		go func() {
			defer joined.Done()
			results[i], loadErrors[i] = concurrent.load(t.Context(), rooted)
		}()
	}
	joined.Wait()
	for i, got := range results {
		if loadErrors[i] != nil || got != results[0] || got == nil {
			t.Fatalf("concurrent reader %d did not share completed output: %v", i, loadErrors[i])
		}
	}
}
