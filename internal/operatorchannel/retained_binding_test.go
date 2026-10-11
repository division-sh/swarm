package operatorchannel

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	"github.com/division-sh/swarm/internal/sessionprovider/authority"
	"github.com/google/uuid"
)

type retainedBindingTestStore struct {
	Store
	rows      []Binding
	err       error
	afterRead func()
	principal Principal
	bootBinds int
}

func (s *retainedBindingTestStore) EnsureOperatorPrincipal(context.Context, time.Time) (Principal, error) {
	return s.principal, nil
}

func (*retainedBindingTestStore) ListPendingProofResponsibilities(context.Context) ([]ProofResponsibility, error) {
	return nil, nil
}

func (s *retainedBindingTestStore) BindOperatorChannelFromProof(context.Context, BootBindRequest) (Binding, error) {
	s.bootBinds++
	return Binding{}, errors.New("unavailable native proof must not bind")
}

func (s *retainedBindingTestStore) ListOperatorChannelBindings(context.Context, string) ([]Binding, error) {
	if s.afterRead != nil {
		s.afterRead()
	}
	return s.rows, s.err
}

func (*retainedBindingTestStore) ListOperatorChannelOperations(context.Context, string) ([]Operation, error) {
	return nil, nil
}

type retainedBindingTestCredentials struct{ cause error }

func (c retainedBindingTestCredentials) CurrentValueMatchesSeal(context.Context, runtimecredentials.ValueEvidence) (bool, error) {
	return c.cause == nil, c.cause
}

func (c retainedBindingTestCredentials) AdmitSessionAccount(context.Context, SessionAccountAdmission) (authority.Admission, error) {
	if c.cause != nil {
		return authority.Admission{}, c.cause
	}
	return authority.Admission{}, &SessionProviderUnavailableError{Provider: "whatsapp"}
}

func retainedBindingServiceFixture(t *testing.T) (*Service, *retainedBindingTestStore, Binding) {
	t.Helper()
	binding := testProofResponsibility(t).Binding
	binding.ProofID, binding.ProofRevision = "", 0
	binding.ProviderAuthority = ProviderAuthority{Kind: ProviderAuthoritySession,
		Session: SessionAccountAdmission{Provider: "whatsapp", ConnectionID: uuid.NewString(),
			AccountRef: "exact-account@s.whatsapp.net", AdmissionID: uuid.NewString(), Revision: 1}}
	store := &retainedBindingTestStore{rows: []Binding{binding}, principal: Principal{ID: binding.PrincipalID, CreatedAt: binding.UpdatedAt}}
	proofs, err := NewFileProofStore(filepath.Join(t.TempDir(), ".swarm"))
	if err != nil {
		t.Fatal(err)
	}
	service := &Service{store: store, proofs: proofs, credentials: retainedBindingTestCredentials{},
		principal: Principal{ID: binding.PrincipalID, CreatedAt: binding.UpdatedAt}, interfaces: []InterfaceIdentity{binding.Interface}}
	return service, store, binding
}

func TestBootstrapRetiredNativeProofKeepsRecoveryReachableWithoutBinding(t *testing.T) {
	service, selected, binding := retainedBindingServiceFixture(t)
	proof := testVerifiedProof(binding.UpdatedAt)
	proof.Interface, proof.MintingStoreID, proof.ProviderAuthority = binding.Interface, binding.PrincipalID, binding.ProviderAuthority
	if err := service.proofs.Put(context.Background(), proof); err != nil {
		t.Fatal(err)
	}
	_, bound, err := service.Bootstrap(context.Background(), binding.UpdatedAt)
	if err != nil || len(bound) != 0 || selected.bootBinds != 0 {
		t.Fatal("retired native proof either blocked recovery or acquired binding authority", err, bound, selected.bootBinds)
	}
}

func TestBootstrapUnavailableNativeProofDoesNotHideUnknownOrCanceledObservation(t *testing.T) {
	for _, mode := range []string{"unknown", "joined", "canceled"} {
		t.Run(mode, func(t *testing.T) {
			service, selected, binding := retainedBindingServiceFixture(t)
			proof := testVerifiedProof(binding.UpdatedAt)
			proof.Interface, proof.MintingStoreID, proof.ProviderAuthority = binding.Interface, binding.PrincipalID, binding.ProviderAuthority
			if err := service.proofs.Put(context.Background(), proof); err != nil {
				t.Fatal(err)
			}
			cause := errors.New("native proof observation failed")
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if mode == "joined" {
				service.credentials = retainedBindingTestCredentials{cause: errors.Join(cause, &SessionProviderUnavailableError{Provider: "whatsapp"})}
			} else if mode == "canceled" {
				cancel()
				cause = context.Canceled
			} else {
				service.credentials = retainedBindingTestCredentials{cause: cause}
			}
			_, bound, err := service.Bootstrap(ctx, binding.UpdatedAt)
			if !errors.Is(err, cause) || len(bound) != 0 || selected.bootBinds != 0 {
				t.Fatal("unknown/canceled native proof granted or concealed admission", err, bound, selected.bootBinds)
			}
		})
	}
}

func TestRetainedBindingDoesNotRequireOrGrantNativeExecution(t *testing.T) {
	service, _, expected := retainedBindingServiceFixture(t)
	binding, err := service.RetainedCurrentBinding(context.Background(), expected.Interface)
	if err != nil || binding != expected || binding.ProviderAuthority.RequireExecutable() == nil {
		t.Fatal("retained identity was lost or became an SDK permit", binding, err)
	}
	var unavailable *SessionProviderUnavailableError
	if _, err := service.CurrentBinding(context.Background(), expected.Interface); !errors.As(err, &unavailable) {
		t.Fatal("strict binding admitted retired provider execution", err)
	}
	if _, ready, err := service.CurrentBindingReadiness(context.Background(), expected.Interface); ready || !errors.As(err, &unavailable) {
		t.Fatal("retained history became proof reuse readiness", ready, err)
	}
	read, err := service.Readback(context.Background())
	if err != nil || len(read) != 1 || read[0].Status != BindingStale || read[0].BindingRevision != expected.Revision || read[0].Reason == "" {
		t.Fatal("unavailable native execution hid retained identity", read, err)
	}
}

func TestRetainedBindingRejectsContradictorySelection(t *testing.T) {
	for _, test := range []struct {
		name      string
		mutate    func(*Binding)
		duplicate bool
	}{
		{"foreign_principal", func(b *Binding) { b.PrincipalID = uuid.NewString() }, false},
		{"bad_interface", func(b *Binding) { b.Interface.Selector = "guessed" }, false},
		{"unsupported_interface", func(b *Binding) {
			b.Interface.InterfaceRef = "unrelated.interface/v1"
			b.Interface.Selector = b.Interface.CanonicalSelector()
		}, false},
		{"zero_revision", func(b *Binding) { b.Revision = 0 }, false},
		{"missing_origin", func(b *Binding) { b.OperationID = "" }, false},
		{"missing_claimant", func(b *Binding) { b.ExternalAccountRef = "" }, false},
		{"missing_provider_evidence", func(b *Binding) { b.ProviderAuthority = ProviderAuthority{} }, false},
		{"bad_proof_revision", func(b *Binding) { b.ProofRevision = 1 }, false},
		{"unknown_status", func(b *Binding) { b.Status = "ready" }, false},
		{"duplicate", func(*Binding) {}, true},
		{"contradictory_revision", func(b *Binding) { b.Revision++ }, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			service, store, original := retainedBindingServiceFixture(t)
			row := original
			test.mutate(&row)
			store.rows = []Binding{row}
			if test.duplicate {
				store.rows = append(store.rows, original)
			}
			if _, err := service.RetainedCurrentBinding(context.Background(), original.Interface); !errors.Is(err, ErrConflict) {
				t.Fatal("contradictory retained selection was accepted", err)
			}
		})
	}
}

func TestRetainedBindingPreservesAbsenceCancellationAndObservationErrors(t *testing.T) {
	for _, mode := range []string{"absent", "unbound", "foreign_interface", "canceled", "canceled_lookup", "store_error"} {
		t.Run(mode, func(t *testing.T) {
			service, store, original := retainedBindingServiceFixture(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			expected := ErrNotFound
			switch mode {
			case "absent":
				store.rows = nil
			case "unbound":
				store.rows[0].Status = BindingUnbound
			case "foreign_interface":
				store.rows[0].Interface.SemanticGeneration = "different-generation"
				store.rows[0].Interface.Selector = store.rows[0].Interface.CanonicalSelector()
			case "canceled":
				cancel()
				expected = context.Canceled
			case "canceled_lookup":
				store.afterRead = cancel
				expected = context.Canceled
			case "store_error":
				expected = errors.New("selected binding observation failed")
				store.err = expected
			}
			if binding, err := service.RetainedCurrentBinding(ctx, original.Interface); !errors.Is(err, expected) || binding != (Binding{}) {
				t.Fatal("failed lookup manufactured identity or lost its cause", binding, err)
			}
		})
	}
}

func TestNativeBindingHistoryDoesNotSuppressUnknownProviderObservation(t *testing.T) {
	service, _, _ := retainedBindingServiceFixture(t)
	cause := errors.New("private SDK observation failed")
	service.credentials = retainedBindingTestCredentials{cause: cause}
	if _, err := service.Readback(context.Background()); !errors.Is(err, cause) {
		t.Fatal("unknown provider failure became stale/ready readback", err)
	}
	service.credentials = retainedBindingTestCredentials{cause: errors.Join(cause, &SessionProviderUnavailableError{Provider: "whatsapp"})}
	if _, err := service.Readback(context.Background()); !errors.Is(err, cause) {
		t.Fatal("joined unavailable classification hid an independent observation error", err)
	}
}
