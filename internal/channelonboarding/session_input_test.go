package channelonboarding

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/runtime/triggergeneration"
	"github.com/google/uuid"
)

type sessionInputFixtureReader struct {
	input NativeSessionInput
}

func (r sessionInputFixtureReader) ReadAuthenticatedSessionInput(context.Context, SessionInputReference) (NativeSessionInput, error) {
	return r.input, nil
}

// Component-supplied native data exercises selected responsibility semantics,
// not SDK authentication. The WhatsApp both-store proof covers that reader.
func TestSessionInputAdmissionUsesCanonicalActivationResponsibility(t *testing.T) {
	for _, change := range []string{"current", "completion_progress", "replaced_activation", "changed_activation_operation", "retired", "canceled", "released"} {
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
			op.ActivationRevision = activation.Revision
			store := &cancellationTestStore{op: op, activation: activation}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			releases := 0
			reader := sessionInputFixtureReader{NativeSessionInput{Scope: SessionInputBusiness, Account: op.SessionAccount,
				OperationID: op.OperationID, OperationRevision: activation.OperationRevision, ActivationRevision: activation.Revision,
				TargetSelector: op.TargetSelector, PrincipalID: op.PrincipalID, Source: op.Coordinate.DurableIdentity(),
				BindingRevision: op.BindingRevision, Body: []byte(`{"text":"current"}`), ReceivedAt: now,
				Context: ctx, Release: func() { releases++ }}}
			generation := triggergeneration.FromCanonicalBytes([]byte(`{"fixture":"session"}`))
			owner, err := NewSessionInputOwner(store, reader, op.Coordinate, op.OperationID, generation)
			if err != nil {
				t.Fatal(err)
			}
			input, err := owner.Admit(ctx, SessionInputReference{ConnectionID: op.SessionAccount.ConnectionID})
			if err != nil {
				t.Fatal(err)
			}
			defer input.Close()
			switch change {
			case "completion_progress":
				store.op.Revision++
			case "replaced_activation":
				store.activation.Revision++
			case "changed_activation_operation":
				store.activation.OperationRevision++
			case "retired":
				store.activation.Status = ActivationRetired
			case "canceled":
				cancel()
			case "released":
				copy := input
				copy.Close()
			}
			want := change == "current" || change == "completion_progress"
			if err := input.Validate(ctx, op.Provider, generation); (err == nil) != want {
				t.Fatalf("current input=%v, want %v: %v", err == nil, want, err)
			}
			input.Close()
			if releases != 1 {
				t.Fatalf("shared native lease released %d times", releases)
			}
		})
	}
}
