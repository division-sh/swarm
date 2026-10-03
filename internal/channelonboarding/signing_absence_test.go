package channelonboarding

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
)

func TestSigningCredentialFallbackRequiresGenuineAbsence(t *testing.T) {
	for _, previous := range []bool{false, true} {
		for _, state := range []string{"absent", "empty", "whitespace", "read_error", "valid", "rotated"} {
			name := "reserved/" + state
			if previous {
				name = "previous_current/" + state
			}
			t.Run(name, func(t *testing.T) {
				ctx := context.Background()
				file, err := runtimecredentials.NewFileStore(filepath.Join(t.TempDir(), "credentials.json"))
				if err != nil {
					t.Fatal(err)
				}
				writer, err := NewCredentialWriter(file)
				if err != nil {
					t.Fatal(err)
				}
				candidate := testCandidate(strings.Repeat("a", 64), "support")
				op := testSucceededOperation(candidate, time.Now().UTC())
				op.Phase = PhasePreparing
				// Reserve both roles: a signing failure must precede even a valid
				// provider credential write, not merely provider registration.
				op.CredentialReservations = credentialReservations(candidate)
				key := op.CredentialReservations[1].StoreKey
				store := &cancellationTestStore{op: op}
				if previous {
					key = "previous.signing"
					if err := file.Set(ctx, key, "previous-valid-secret"); err != nil {
						t.Fatal(err)
					}
					observed, err := writer.Observe(ctx, key)
					if err != nil {
						t.Fatal(err)
					}
					store.activation = testCurrentActivation(op, []CredentialAdmission{{
						Role: candidate.SigningCredentialRole, StoreKey: key,
						Kind: CredentialAdmissionObserved, ValueSeal: observed.ValueSeal,
					}}, time.Now().UTC())
				}
				switch state {
				case "absent", "read_error":
					if err := file.Delete(ctx, key); err != nil {
						t.Fatal(err)
					}
				case "empty":
					if err := file.Set(ctx, key, ""); err != nil {
						t.Fatal(err)
					}
				case "whitespace":
					if err := file.Set(ctx, key, " \t\n"); err != nil {
						t.Fatal(err)
					}
				case "valid":
					if !previous {
						if err := file.Set(ctx, key, "reserved-valid-secret"); err != nil {
							t.Fatal(err)
						}
					}
				case "rotated":
					if err := file.Set(ctx, key, "replacement-secret"); err != nil {
						t.Fatal(err)
					}
				}
				lookupErr := errors.New("selected credential tier unavailable")
				if state == "read_error" {
					writer.snapshots, err = runtimecredentials.NewSnapshotOwner(&signingLookupErrorStore{FileStore: file, key: key, err: lookupErr})
					if err != nil {
						t.Fatal(err)
					}
				}
				generated := 0
				service := &Service{store: store, credentials: writer, secret: func() (string, error) {
					generated++
					return "new-signing-secret", nil
				}}
				admissions, err := service.admitCredentials(ctx, op, candidate, "valid-provider-token", true)
				wantSuccess := state == "valid" || !previous && (state == "absent" || state == "rotated")
				if wantSuccess {
					if err != nil || len(admissions) != 2 {
						t.Fatalf("admissions = %#v, err = %v", admissions, err)
					}
					wantGenerated := 0
					if state == "absent" {
						wantGenerated = 1
					}
					if generated != wantGenerated {
						t.Fatalf("generated = %d, want %d", generated, wantGenerated)
					}
					return
				}
				if err == nil || generated != 0 {
					t.Fatalf("invalid observation admitted: generated=%d admissions=%#v err=%v", generated, admissions, err)
				}
				if state == "read_error" && !errors.Is(err, lookupErr) {
					t.Fatalf("lookup error lost: %v", err)
				}
				if (state == "empty" || state == "whitespace") && !errors.Is(err, runtimecredentials.ErrCredentialValueUnusable) {
					t.Fatalf("unusable error lost: %v", err)
				}
				for _, reservation := range op.CredentialReservations {
					operationKey := operationCredentialStoreKey(reservation.StoreKey, op.OperationID, reservation.Role)
					if _, found, err := file.Get(ctx, operationKey); err != nil || found {
						t.Fatalf("invalid preflight wrote %s: present=%t err=%v", reservation.Role, found, err)
					}
				}
			})
		}
	}
}

type signingLookupErrorStore struct {
	*runtimecredentials.FileStore
	key string
	err error
}

func (s *signingLookupErrorStore) Snapshot(ctx context.Context, key string) (runtimecredentials.AtomicSnapshot, error) {
	if key == s.key {
		return runtimecredentials.AtomicSnapshot{}, s.err
	}
	return s.FileStore.Snapshot(ctx, key)
}

func TestCredentialObservationFailureDoesNotResetAdmittedResponsibility(t *testing.T) {
	for _, entry := range []string{"local_reconciliation", "explicit_retry"} {
		for _, state := range []string{"empty", "whitespace", "read_error", "stale_plus_invalid"} {
			t.Run(entry+"/"+state, func(t *testing.T) {
				ctx := context.Background()
				candidate := testCandidate(strings.Repeat("a", 64), "support")
				catalog, err := NewCandidateCatalog([]Candidate{candidate})
				if err != nil {
					t.Fatal(err)
				}
				file, err := runtimecredentials.NewFileStore(filepath.Join(t.TempDir(), "credentials.json"))
				if err != nil {
					t.Fatal(err)
				}
				writer, err := NewCredentialWriter(file)
				if err != nil {
					t.Fatal(err)
				}
				op := testSucceededOperation(candidate, time.Now().UTC())
				op.Phase = PhaseCredentialsAdmitted
				op.CredentialAdmissions = writeTestOperationCredentials(t, writer, op, "admitted")
				key := op.CredentialAdmissions[1].StoreKey
				lookupErr := errors.New("credential observation unavailable")
				switch state {
				case "empty":
					err = file.Set(ctx, key, "")
				case "whitespace", "stale_plus_invalid":
					err = file.Set(ctx, key, " \t\n")
				case "read_error":
					writer.snapshots, err = runtimecredentials.NewSnapshotOwner(&signingLookupErrorStore{FileStore: file, key: key, err: lookupErr})
				}
				if err != nil {
					t.Fatal(err)
				}
				if state == "stale_plus_invalid" {
					if err := file.Set(ctx, op.CredentialAdmissions[0].StoreKey, "unadmitted-value"); err != nil {
						t.Fatal(err)
					}
				}
				store := &cancellationTestStore{op: op}
				activations := &cancellationTestActivations{}
				service, err := NewService(ServiceOptions{
					Store: store, Identities: &cancellationTestIdentities{}, Credentials: writer,
					Catalog: func() (*CandidateCatalog, error) { return catalog, nil }, Activations: activations,
					Confirmation: successfulTestConfirmation{}, Readiness: cancellationTestReadiness{},
				})
				if err != nil {
					t.Fatal(err)
				}
				if entry == "local_reconciliation" {
					err = service.ReconcileLocal(ctx)
				} else {
					_, err = service.Retry(ctx, RetryInput{OperationID: op.OperationID})
				}
				want := runtimecredentials.ErrCredentialValueUnusable
				if state == "read_error" {
					want = lookupErr
				}
				if !errors.Is(err, want) {
					t.Fatalf("observation error = %v, want %v", err, want)
				}
				if store.op.Phase != op.Phase || store.op.Revision != op.Revision || len(store.op.CredentialAdmissions) != len(op.CredentialAdmissions) {
					t.Fatalf("observation failure reset responsibility: %#v", store.op)
				}
				if activations.preflights != 0 || activations.publications != 0 || activations.promotions != 0 {
					t.Fatalf("observation failure invoked activation: %#v", activations)
				}
			})
		}
	}
}
