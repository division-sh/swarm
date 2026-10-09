package channelonboarding

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/google/uuid"
)

// This is a responsibility-model control, never a native-input issuer. Actual
// input lifetime and byte authority are exercised in the native SDK package.
func TestSessionActiveResponsibilityUsesFrozenActivationRevision(t *testing.T) {
	for _, change := range []string{"current", "completion_progress", "replaced_activation", "changed_activation_operation", "retired"} {
		t.Run(change, func(t *testing.T) {
			candidate := testCandidate(strings.Repeat("a", 64), "support")
			now := time.Now().UTC().Truncate(time.Microsecond)
			op := testSucceededOperation(candidate, now)
			op.Provider, op.Posture = "whatsapp", ActivationSessionConnection
			op.TargetSelector = "ingress:support:whatsapp"
			op.SessionAccount = operatorchannel.SessionAccountAdmission{Provider: op.Provider, ConnectionID: uuid.NewString(),
				AccountRef: "100000000001@s.whatsapp.net", AdmissionID: uuid.NewString(), Revision: 1}
			activation := testCurrentActivation(op, nil, now)
			activation.SessionAccount = op.SessionAccount
			expected := AdmissionResponsibility{OperationID: op.OperationID, OperationRevision: activation.OperationRevision,
				ActivationRevision: activation.Revision, Coordinate: op.Coordinate, TargetSelector: op.TargetSelector,
				Provider: op.Provider, SessionAccount: op.SessionAccount}
			store := &cancellationTestStore{op: op, activation: activation}
			switch change {
			case "completion_progress":
				store.op.Revision++
			case "replaced_activation":
				store.activation.Revision++
			case "changed_activation_operation":
				store.activation.OperationRevision++
			case "retired":
				store.activation.Status = ActivationRetired
			}
			current, err := AdmissionResponsibilityCurrent(context.Background(), store, expected, true)
			want := change == "current" || change == "completion_progress"
			if current != want || (err != nil && !(change == "retired" && errors.Is(err, ErrNotFound))) {
				t.Fatalf("current=%v want=%v: %v", current, want, err)
			}
		})
	}
}
