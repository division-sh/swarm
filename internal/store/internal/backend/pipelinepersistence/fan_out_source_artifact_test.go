package pipelinepersistence

import (
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
)

func TestFanOutSameArtifactReadbackBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			db := fanOutReadbackTestDB(t, backend)
			seedFanOutReadbackClaim(t, db)
			original, err := scanFanOutIntent(db.QueryRow(`SELECT ` + fanOutIntentColumns + ` FROM fan_out_intents`))
			if err != nil {
				t.Fatal(err)
			}
			changed := original.Request.Capsule.SourceProjection.Clone()
			changed.Emit.Event = "selected.output"
			digest, err := canonicaljson.Hash(changed)
			if err != nil {
				t.Fatal(err)
			}
			for _, variant := range []string{"same_artifact_source", "different_artifact", "exact_return", "contradictory_return"} {
				t.Run(variant, func(t *testing.T) {
					request := original.Request
					wantRefusal := false
					switch variant {
					case "same_artifact_source":
						request.Capsule.SourceProjection = changed
						request.Capsule.SourcePlanRef.SemanticDigest = digest
						wantRefusal = true
					case "different_artifact":
						request.PlanRef.BundleHash = "bundle-v2:sha256:" + strings.Repeat("2", 64)
						request.PlanRef.SemanticDigest = digest
					case "exact_return", "contradictory_return":
						intermediate := request.PlanRef
						intermediate.BundleHash = "bundle-v2:sha256:" + strings.Repeat("2", 64)
						intermediate.SemanticDigest = digest
						if err := request.AdmitSelectedPlan(intermediate, changed); err != nil {
							t.Fatal(err)
						}
						request.PlanRef = intermediate
						if variant == "exact_return" {
							if err := request.AdmitSelectedPlan(original.Request.PlanRef, request.Capsule.SourceProjection); err != nil {
								t.Fatal(err)
							}
							request.PlanRef = original.Request.PlanRef
						} else {
							request.PlanRef = original.Request.PlanRef
							request.PlanRef.SemanticDigest = digest
							wantRefusal = true
						}
					}
					capsule, err := fanoutobligation.MarshalCapsule(request.Capsule)
					if err != nil {
						t.Fatal(err)
					}
					tx, err := db.Begin()
					if err != nil {
						t.Fatal(err)
					}
					defer tx.Rollback()
					if _, err := tx.Exec(`UPDATE fan_out_intents SET bundle_hash=$1,semantic_digest=$2,capsule=$3`, request.PlanRef.BundleHash, request.PlanRef.SemanticDigest, string(capsule)); err != nil {
						t.Fatal(err)
					}
					actual, err := scanFanOutIntent(tx.QueryRow(`SELECT ` + fanOutIntentColumns + ` FROM fan_out_intents`))
					if wantRefusal {
						if err == nil || !strings.Contains(err.Error(), "same artifact") {
							t.Fatalf("contradictory artifact readback = %v", err)
						}
					} else if err != nil || actual.Request.PlanRef != request.PlanRef || !actual.Request.Capsule.Equal(request.Capsule) {
						t.Fatalf("exact retained readback changed: %+v %v", actual.Request, err)
					}
					var retained string
					if err := tx.QueryRow(`SELECT capsule FROM fan_out_intents`).Scan(&retained); err != nil || retained != string(capsule) {
						t.Fatalf("readback mutated retained evidence: %q %v", retained, err)
					}
				})
			}
		})
	}
}
