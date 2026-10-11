package channelonboarding

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/operatorchannel"
)

type connectFenceTestIdentities struct {
	cancellationTestIdentities
	fenceReads int
	fenceErr   error
}

func (i *connectFenceTestIdentities) RetainedBindingFence(ctx context.Context, identity operatorchannel.InterfaceIdentity) (operatorchannel.BindingFence, error) {
	i.fenceReads++
	if i.fenceErr != nil {
		return operatorchannel.BindingFence{}, i.fenceErr
	}
	return i.cancellationTestIdentities.RetainedBindingFence(ctx, identity)
}

func TestConnectFreezesPredecessorApartFromConfirmedBindingAndReplaysFirst(t *testing.T) {
	for _, revision := range []int64{0, 3} {
		for _, saveProof := range []bool{false, true} {
			name := "never_bound"
			if revision > 0 {
				name = "unbound"
			}
			if saveProof {
				name += "_proof"
			}
			t.Run(name, func(t *testing.T) {
				now := time.Date(2026, 10, 11, 6, 0, 0, 0, time.UTC)
				candidate := testCandidate(strings.Repeat("a", 64), "support")
				catalog, err := NewCandidateCatalog([]Candidate{candidate})
				if err != nil {
					t.Fatal(err)
				}
				identities := &connectFenceTestIdentities{}
				if revision > 0 {
					identities.binding = operatorchannel.Binding{Revision: revision, Status: operatorchannel.BindingUnbound}
				}
				store := &cancellationTestStore{}
				opts := ServiceOptions{Store: store, Identities: identities, Credentials: testCredentialWriter(t),
					Catalog: func() (*CandidateCatalog, error) { return catalog, nil }, Activations: &cancellationTestActivations{},
					Confirmation: cancellationTestConfirmation{}, Readiness: cancellationTestReadiness{}, Now: func() time.Time { return now }}
				service, err := NewService(opts)
				if err != nil {
					t.Fatal(err)
				}
				input := StartInput{Verb: VerbConnect, Selection: CandidateSelection{Provider: candidate.Provider},
					IdempotencyKey: "exact-connect", ProviderCredential: "new-provider-secret", SaveProof: saveProof}
				first, err := service.Start(context.Background(), input)
				if err != nil || first.Operation.ConnectFenceRevision != revision || first.Operation.BindingRevision != 0 ||
					identities.beginExpectedRevision != revision || identities.beginKind != operatorchannel.OperationConnect ||
					first.Operation.Phase != PhaseAwaitingExternalIdentity || identities.fenceReads != 1 {
					t.Fatal("connect treated a predecessor as confirmed reconnect authority", first, err, identities)
				}
				identities.binding = operatorchannel.Binding{Revision: revision + 10, Status: operatorchannel.BindingCurrent}
				identities.fenceErr = errors.New("mutable fence must not be reread for exact replay")
				service, err = NewService(opts)
				if err != nil {
					t.Fatal(err)
				}
				replay, err := service.Start(context.Background(), input)
				if err != nil || replay.Operation.OperationID != first.Operation.OperationID ||
					replay.Operation.IdentityOperationID != first.Operation.IdentityOperationID || replay.Operation.ConnectFenceRevision != revision ||
					replay.Operation.BindingRevision != 0 || identities.fenceReads != 1 || identities.beginCalls != 1 {
					t.Fatal("exact replay refreshed the fence or repeated identity creation", replay, err, identities)
				}
				input.SaveProof = !input.SaveProof
				if _, err := service.Start(context.Background(), input); !errors.Is(err, ErrConflict) || identities.fenceReads != 1 {
					t.Fatal("changed request bypassed exact replay admission", err)
				}
				raw, err := json.Marshal(replay.Operation)
				if err != nil || strings.Contains(string(raw), "ConnectFenceRevision") || strings.Contains(string(raw), "connect_fence_revision") {
					t.Fatal("server-selected predecessor became a caller-facing revision", string(raw), err)
				}
			})
		}
	}
}
