package channelonboarding

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestAdmissionResponsibilityCurrentRejectsStaleOwners(t *testing.T) {
	for _, pending := range []bool{false, true} {
		for _, change := range []string{"current", "advanced", "reset", "retired", "coordinate", "target", "provider", "admission", "activation_revision", "activation_operation"} {
			t.Run(change+"/"+map[bool]string{false: "learned", true: "pending"}[pending], func(t *testing.T) {
				candidate := testCandidate(strings.Repeat("a", 64), "support")
				op := testSucceededOperation(candidate, time.Now().UTC())
				op.CredentialAdmissions = []CredentialAdmission{{Role: "provider", StoreKey: "bot", Kind: CredentialAdmissionObserved, ValueSeal: testValueSeal('a')}}
				activation := testCurrentActivation(op, op.CredentialAdmissions, time.Now().UTC())
				if pending {
					op.Phase = PhaseAwaitingExternalIdentity
				}
				expected := AdmissionResponsibility{OperationID: op.OperationID, OperationRevision: op.Revision,
					Coordinate: op.Coordinate, TargetSelector: op.TargetSelector, Provider: op.Provider,
					Credentials: append([]CredentialAdmission(nil), op.CredentialAdmissions...)}
				if !pending {
					expected.ActivationRevision = activation.Revision
				}
				switch change {
				case "advanced":
					op.Revision++
				case "reset":
					op.Phase = PhasePreparing
				case "retired":
					op.Phase = PhaseRetired
				case "coordinate":
					op.Coordinate.ContextPublicationGeneration++
				case "target":
					op.TargetSelector = "ingress:other:telegram"
				case "provider":
					op.Provider = "other"
				case "admission":
					op.CredentialAdmissions = nil
				case "activation_revision":
					activation.Revision++
				case "activation_operation":
					activation.OperationID = "another-operation"
				}
				store := &cancellationTestStore{op: op, activation: activation}
				for _, exact := range []bool{true, false} {
					current, err := AdmissionResponsibilityCurrent(context.Background(), store, expected, exact)
					if err != nil {
						t.Fatal(err)
					}
					want := change == "current" || change == "advanced" && (!pending || !exact) || pending && strings.HasPrefix(change, "activation_")
					if current != want {
						t.Fatalf("exact=%v: current=%v, want %v", exact, current, want)
					}
				}
			})
		}
	}
}
