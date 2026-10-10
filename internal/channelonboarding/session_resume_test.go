package channelonboarding

import (
	"testing"

	"github.com/division-sh/swarm/internal/operatorchannel"
)

func TestSessionBusinessResumeOnlyChangesLiveOccurrence(t *testing.T) {
	op := Operation{OperationID: "original", Revision: 9, Provider: "whatsapp", Coordinate: testCoordinate(),
		TargetSelector: "ingress:.:whatsapp", Phase: PhaseSucceeded, Posture: ActivationSessionConnection, BindingRevision: 3}
	op.SessionAccount = operatorchannel.SessionAccountAdmission{Provider: "whatsapp", ConnectionID: "connection", AccountRef: "account", AdmissionID: "admission", Revision: 1}
	expected := AdmissionResponsibility{OperationID: op.OperationID, OperationRevision: 7, ActivationRevision: 2,
		Coordinate: op.Coordinate, TargetSelector: op.TargetSelector, Provider: op.Provider, SessionAccount: op.SessionAccount}
	activation := ConnectedChannelActivation{ActivationID: "original-activation", OperationID: op.OperationID, OperationRevision: 7, Revision: 2,
		Coordinate: op.Coordinate, TargetSelector: op.TargetSelector, Provider: op.Provider, SessionAccount: op.SessionAccount, Status: ActivationCurrent}
	op.ActivationRevision = activation.Revision
	for _, change := range []string{"unchanged", "live_occurrence", "source", "target_generation", "account", "binding", "parent", "replacement_activation", "activation_revision_backwards", "activation_parent_revision", "retired", "contradictory_coordinate"} {
		t.Run(change, func(t *testing.T) {
			current, active := op, activation
			switch change {
			case "live_occurrence":
				current.Coordinate.RuntimeInstanceID = "successor-runtime"
				current.Coordinate.ContextPublicationGeneration++
				active.Coordinate = current.Coordinate
				active.Revision++
				active.OperationRevision = current.Revision
				current.ActivationRevision = active.Revision
			case "source":
				current.Coordinate.BundleIdentity += "-replacement"
				active.Coordinate = current.Coordinate
			case "target_generation":
				current.Coordinate.TargetGeneration++
				active.Coordinate = current.Coordinate
			case "account":
				current.SessionAccount.Revision++
				active.SessionAccount = current.SessionAccount
			case "binding":
				current.BindingRevision++
			case "parent":
				current.OperationID = "replacement"
				active.OperationID = current.OperationID
			case "replacement_activation":
				active.ActivationID = "replacement-activation"
			case "activation_revision_backwards":
				active.Revision--
				current.ActivationRevision = active.Revision
			case "activation_parent_revision":
				active.OperationRevision++
			case "retired":
				active.Status = ActivationRetired
			case "contradictory_coordinate":
				active.Coordinate.ContextPublicationGeneration++
			}
			resumed, ok := expected.ResumeSessionBusiness(current, active, op.BindingRevision, activation.ActivationID)
			if want := change == "unchanged" || change == "live_occurrence"; ok != want {
				t.Fatalf("resume=%t, want %t", ok, want)
			}
			if ok && (!resumed.MatchesActivation(current, active) || resumed.OperationRevision != active.OperationRevision ||
				resumed.SessionAccount != expected.SessionAccount || !resumed.Coordinate.Matches(current.Coordinate)) {
				t.Fatal("resume changed stable authority or failed current activation admission")
			}
		})
	}
}
