package fanoutobligation

import (
	"testing"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
)

// Retains reviewer-a's two shared-owner counterexamples.
func TestFanOutSameArtifactCannotHaveTwoInterpretations(t *testing.T) {
	t.Run("persisted_readback", func(t *testing.T) {
		r := a2SourceProjectionRequest(t)
		r.Capsule.SourceProjection = r.Capsule.SourceProjection.Clone()
		r.Capsule.SourceProjection.Emit.Event = "contradictory.original"
		var err error
		r.Capsule.SourcePlanRef.SemanticDigest, err = canonicaljson.Hash(r.Capsule.SourceProjection)
		if err != nil {
			t.Fatal(err)
		}
		if err := r.Validate(); err == nil {
			t.Fatal("accepted two semantic digests for the same artifact and declaration")
		}
	})
	t.Run("fork_back_to_original_artifact", func(t *testing.T) {
		r := a2SourceProjectionRequest(t)
		originalRef := r.PlanRef
		r.PlanRef.BundleHash = "intermediate-selected-bundle"
		semantics := r.Capsule.SourceProjection.Clone()
		semantics.Emit.Event = "contradictory.original"
		var err error
		originalRef.SemanticDigest, err = canonicaljson.Hash(semantics)
		if err != nil {
			t.Fatal(err)
		}
		if err := r.AdmitSelectedPlan(originalRef, semantics); err == nil {
			t.Fatal("fork-back admission accepted contradictory original-artifact interpretation")
		}
	})
	t.Run("exact_return_to_original_artifact", func(t *testing.T) {
		r := a2SourceProjectionRequest(t)
		originalRef := r.PlanRef
		semantics := r.Capsule.SourceProjection.Clone()
		semantics.Emit.Event = "selected.output"
		selectedRef := originalRef
		selectedRef.BundleHash = "intermediate-selected-bundle"
		var err error
		selectedRef.SemanticDigest, err = canonicaljson.Hash(semantics)
		if err != nil {
			t.Fatal(err)
		}
		if err := r.AdmitSelectedPlan(selectedRef, semantics); err != nil {
			t.Fatalf("lawful different-artifact selection: %v", err)
		}
		r.PlanRef = selectedRef
		if err := r.AdmitSelectedPlan(originalRef, r.Capsule.SourceProjection); err != nil {
			t.Fatalf("exact A -> B -> A return: %v", err)
		}
	})
}
