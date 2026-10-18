package channelonboarding

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/runtime/credentials"
	"github.com/google/uuid"
)

func TestOnboardingSelectionHasNoCredentialOrExecutionEffects(t *testing.T) {
	service, store, activations, identities, values, candidate := newStartSelectionFixture(t)
	input := StartInput{Verb: VerbReconnect, Selection: CandidateSelection{Provider: candidate.Provider}, SaveProof: true}
	selected, err := service.Select(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	role, err := selected.ProviderCredentialRequirement()
	if err != nil || role != candidate.ProviderCredentialRole || selected.SelectionFence == "" {
		t.Fatal("selection lost the canonical requirement", selected, err)
	}
	if store.op.OperationID != "" || activations.preflights != 0 || activations.refreshes != 0 || identities.beginCalls != 0 {
		t.Fatal("non-executing selection admitted work", store.op, activations, identities)
	}
	old, present, err := values.Get(context.Background(), credentialReservations(candidate)[0].StoreKey)
	if err != nil || !present || old != "old-usable-token" {
		t.Fatal("selection mutated the retained credential", present, err)
	}
	cut := errors.New("stop after replacement admission")
	service.testBarrier = func(boundary TestLifecycleBoundary, _ string) error {
		if boundary == TestAfterCredentialWriteBeforeCheckpoint {
			return cut
		}
		return nil
	}
	input.ExpectedSelection, input.ProviderCredential = selected.SelectionFence, "replacement-token"
	if _, err := service.Start(context.Background(), input); !errors.Is(err, cut) {
		t.Fatal("replacement did not reach its original credential owner", err)
	}
	reservation := store.op.CredentialReservations[0]
	key := operationCredentialStoreKey(reservation.StoreKey, store.op.OperationID, reservation.Role)
	value, present, err := values.Get(context.Background(), key)
	if err != nil || !present || value != "replacement-token" || activations.preflights != 0 || identities.beginCalls != 0 {
		t.Fatal("replacement adopted old bytes or executed before admission", present, err, activations, identities)
	}
	old, present, err = values.Get(context.Background(), reservation.StoreKey)
	if err != nil || !present || old != "old-usable-token" {
		t.Fatal("replacement overwrote the predecessor", present, err)
	}
}

func TestOnboardingSelectedRequirementRefusesChangesBeforeReservation(t *testing.T) {
	for _, cell := range []string{"context", "target", "role", "verb", "proof", "cancel", "ambiguous", "credential_input"} {
		t.Run(cell, func(t *testing.T) {
			service, store, activations, identities, _, candidate := newStartSelectionFixture(t)
			input := StartInput{Verb: VerbReconnect, Selection: CandidateSelection{Provider: candidate.Provider}, SaveProof: true}
			selected, err := service.Select(context.Background(), input)
			if err != nil {
				t.Fatal(err)
			}
			input.ExpectedSelection, input.ProviderCredential = selected.SelectionFence, "replacement-token"
			ctx := context.Background()
			switch cell {
			case "context":
				candidate.Coordinate.ContextPublicationGeneration++
			case "target":
				candidate.Target.ServiceID = "changed-service"
			case "role":
				candidate.ProviderCredentialRole = "changed-role"
			case "verb":
				input.Verb = VerbRebind
			case "proof":
				input.SaveProof = false
			case "cancel":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}
			candidates := []Candidate{candidate}
			if cell == "ambiguous" {
				other := candidate
				other.Coordinate.BundleHash = "bundle-v2:sha256:" + strings.Repeat("b", 64)
				candidates = append(candidates, other)
			}
			catalog, err := NewCandidateCatalog(candidates)
			if err != nil {
				t.Fatal(err)
			}
			service.catalog = func() (*CandidateCatalog, error) { return catalog, nil }
			if cell == "credential_input" {
				_, err = service.Select(ctx, input)
			} else {
				_, err = service.Start(ctx, input)
			}
			if err == nil || store.op.OperationID != "" || activations.preflights != 0 || identities.beginCalls != 0 {
				t.Fatal("changed selection gained reservation or effects", err, store.op, activations, identities)
			}
		})
	}
}

func TestOnboardingRequiredCredentialKeyConsumesCurrentAdmission(t *testing.T) {
	candidate := testCandidate(strings.Repeat("a", 64), "support")
	prior := testSucceededOperation(candidate, time.Now().UTC())
	writer := testCredentialWriter(t)
	admissions := writeTestOperationCredentials(t, writer, prior, "prior")
	var provider CredentialAdmission
	for _, admission := range admissions {
		if admission.Role == candidate.ProviderCredentialRole {
			provider = admission
			if removed, err := writer.Release(context.Background(), admission); err != nil || !removed {
				t.Fatal(removed, err)
			}
		}
	}
	store := &cancellationTestStore{activation: testCurrentActivation(prior, admissions, time.Now().UTC())}
	service := &Service{store: store, credentials: writer}
	op := prior
	op.OperationID, op.Phase, op.CredentialAdmissions = uuid.NewString(), PhasePreparing, nil
	_, err := service.admitCredentials(context.Background(), op, candidate, "", true)
	var required *CredentialRequiredError
	if !errors.As(err, &required) || required.Role != provider.Role || required.StoreKey != provider.StoreKey {
		t.Fatal("missing current admission lost its exact effective key", required, err)
	}
	for _, reservation := range op.CredentialReservations {
		if reservation.Role == required.Role && reservation.StoreKey == required.StoreKey {
			t.Fatal("fixture failed to exercise distinct canonical keys")
		}
		if reservation.Role == required.Role {
			if err := writer.values.Set(context.Background(), reservation.StoreKey, "unsafe-base-fallback"); err != nil {
				t.Fatal(err)
			}
		}
	}
	_, err = service.admitCredentials(context.Background(), op, candidate, "", true)
	if !errors.As(err, &required) || required.StoreKey != provider.StoreKey {
		t.Fatal("missing current admission adopted base-key fallback", err)
	}
}

func newStartSelectionFixture(t *testing.T) (*Service, *cancellationTestStore, *cancellationTestActivations, *cancellationTestIdentities, *credentials.FileStore, Candidate) {
	t.Helper()
	candidate := testCandidate(strings.Repeat("a", 64), "support")
	catalog, err := NewCandidateCatalog([]Candidate{candidate})
	if err != nil {
		t.Fatal(err)
	}
	values, err := credentials.NewFileStore(filepath.Join(t.TempDir(), "credentials.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := values.Set(context.Background(), credentialReservations(candidate)[0].StoreKey, "old-usable-token"); err != nil {
		t.Fatal(err)
	}
	writer, err := NewCredentialWriter(values)
	if err != nil {
		t.Fatal(err)
	}
	store := &cancellationTestStore{}
	activations := &cancellationTestActivations{}
	identities := &cancellationTestIdentities{binding: operatorchannel.Binding{Status: operatorchannel.BindingCurrent, Revision: 1}}
	service, err := NewService(ServiceOptions{Store: store, Identities: identities, Credentials: writer,
		Catalog: func() (*CandidateCatalog, error) { return catalog, nil }, Activations: activations,
		Confirmation: cancellationTestConfirmation{}, Readiness: cancellationTestReadiness{}, Now: time.Now})
	if err != nil {
		t.Fatal(err)
	}
	return service, store, activations, identities, values, candidate
}
