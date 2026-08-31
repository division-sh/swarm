package channelonboarding

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
)

func TestOnboardingCredentialWriterRejectsEnvOnlyBeforeMutation(t *testing.T) {
	for storeName, store := range map[string]runtimecredentials.Store{
		"direct_env":           runtimecredentials.EnvStore{},
		"overlay_without_home": runtimecredentials.NewOverlayStore(runtimecredentials.EnvStore{}, nil),
	} {
		for _, saveProof := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/save_proof_%t", storeName, saveProof), func(t *testing.T) {
				_, err := NewCredentialWriter(store)
				if !errors.Is(err, runtimecredentials.ErrValueSealKeyUnavailable) ||
					!strings.Contains(err.Error(), "swarm channel connect <provider> --credential-stdin") {
					t.Fatalf("env-only onboarding error = %v", err)
				}
				file, err := runtimecredentials.NewFileStore(filepath.Join(t.TempDir(), "credentials.json"))
				if err != nil {
					t.Fatal(err)
				}
				remediated, err := NewCredentialWriter(runtimecredentials.NewOverlayStore(runtimecredentials.EnvStore{}, file))
				if err != nil {
					t.Fatalf("configure writable credential tier: %v", err)
				}
				admitted, err := remediated.Admit(context.Background(), CredentialWriteRequest{
					StoreKey: "channel.telegram.provider", Value: "provider-token", Receipt: "operation/provider",
				})
				if err != nil || admitted.ValueSeal == "" {
					t.Fatalf("follow credential remediation = %#v, %v", admitted, err)
				}
			})
		}
	}
}

func TestOnboardingCredentialWriterRejectsUnusableValuesAcrossAdmissionAndRecovery(t *testing.T) {
	for _, value := range []string{"", " \t\n "} {
		for _, path := range []string{"admit", "observe", "observe_written"} {
			t.Run(fmt.Sprintf("%s/%q", path, value), func(t *testing.T) {
				ctx := context.Background()
				store, err := runtimecredentials.NewFileStore(filepath.Join(t.TempDir(), "credentials.json"))
				if err != nil {
					t.Fatal(err)
				}
				writer, err := NewCredentialWriter(store)
				if err != nil {
					t.Fatal(err)
				}
				const key = "channel.telegram.provider"
				const receipt = "operation/provider"
				switch path {
				case "admit":
					_, err = writer.Admit(ctx, CredentialWriteRequest{StoreKey: key, Value: value, Receipt: receipt})
				case "observe":
					if err = store.Set(ctx, key, value); err == nil {
						_, err = writer.Observe(ctx, key)
					}
				case "observe_written":
					if _, writeErr := store.AdmitWithReceipt(ctx, key, value, receipt); writeErr != nil {
						t.Fatal(writeErr)
					}
					_, _, err = writer.ObserveWritten(ctx, key, receipt)
				}
				if !errors.Is(err, runtimecredentials.ErrCredentialValueUnusable) {
					t.Fatalf("%s unusable value error = %v", path, err)
				}
			})
		}
	}
}

func TestOnboardingCredentialWriterCrashConvergence(t *testing.T) {
	path := filepath.Join(t.TempDir(), "credentials.json")
	store, err := runtimecredentials.NewFileStore(path)
	if err != nil {
		t.Fatal(err)
	}
	writer, err := NewCredentialWriter(store)
	if err != nil {
		t.Fatal(err)
	}
	first, err := writer.Admit(context.Background(), CredentialWriteRequest{StoreKey: "channel.telegram.provider", Value: "super-secret-token", Receipt: "operation-a/provider"})
	if err != nil {
		t.Fatal(err)
	}
	replayed, err := writer.Admit(context.Background(), CredentialWriteRequest{StoreKey: "channel.telegram.provider", Value: "different-retry-bytes", Receipt: "operation-a/provider"})
	if err != nil {
		t.Fatal(err)
	}
	if replayed != first || first.ValueSeal == "" || first.Receipt != "operation-a/provider" {
		t.Fatalf("receipt replay = first:%#v replay:%#v", first, replayed)
	}
	value, found, err := store.Get(context.Background(), "channel.telegram.provider")
	if err != nil || !found || value != "super-secret-token" {
		t.Fatalf("replay rewrote credential: value=%q found=%v err=%v", value, found, err)
	}
	rotated, err := writer.Admit(context.Background(), CredentialWriteRequest{StoreKey: "channel.telegram.provider", Value: "rotated-token", Receipt: "operation-b/provider"})
	if err != nil {
		t.Fatal(err)
	}
	if rotated.ValueSeal == first.ValueSeal || rotated.Receipt == first.Receipt {
		t.Fatalf("explicit new receipt did not rotate occurrence: old=%#v new=%#v", first, rotated)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "super-secret-token") && strings.Contains(first.ValueSeal.String(), "super-secret-token") {
		t.Fatal("credential value seal exposed secret bytes")
	}
}

func TestCredentialAdmissionJSONNeverContainsValue(t *testing.T) {
	admission := CredentialAdmission{Role: "provider", StoreKey: "channel.telegram.provider", Kind: CredentialAdmissionWritten, Receipt: "receipt-a", ValueSeal: testValueSeal('a')}
	raw, err := json.Marshal(admission)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "value_seal") || strings.Contains(string(raw), admission.ValueSeal.String()) {
		t.Fatalf("public admission JSON exposed the value seal: %s", raw)
	}
}

func TestOnboardingCredentialOccurrenceSurvivesSnapshotOwnerRestart(t *testing.T) {
	store, err := runtimecredentials.NewFileStore(filepath.Join(t.TempDir(), "credentials.json"))
	if err != nil {
		t.Fatal(err)
	}
	writer, err := NewCredentialWriter(store)
	if err != nil {
		t.Fatal(err)
	}
	written, err := writer.Admit(context.Background(), CredentialWriteRequest{StoreKey: "channel.telegram.provider", Value: "token", Receipt: "operation/provider"})
	if err != nil {
		t.Fatal(err)
	}
	restarted, err := NewCredentialWriter(store)
	if err != nil {
		t.Fatal(err)
	}
	observed, err := restarted.Observe(context.Background(), written.StoreKey)
	if err != nil {
		t.Fatal(err)
	}
	if observed.ValueSeal != written.ValueSeal {
		t.Fatalf("restarted credential value seal = %q, want %q", observed.ValueSeal, written.ValueSeal)
	}
}

func TestPreEpochCredentialFileBootsWithoutRemediationLoop(t *testing.T) {
	path := filepath.Join(t.TempDir(), "credentials.json")
	original := `{"version":1,"entries":{"channel.telegram.provider":{"value":"provider-token","updated_at":"2026-08-28T00:00:00Z"},"unrelated-a":{"value":"leave-a","updated_at":"2026-08-27T00:00:00Z"},"unrelated-b":{"value":"leave-b","updated_at":"2026-08-26T00:00:00Z"}}}`
	if err := os.WriteFile(path, []byte(original), 0o600); err != nil {
		t.Fatal(err)
	}
	store, err := runtimecredentials.NewFileStore(path)
	if err != nil {
		t.Fatal(err)
	}
	writer, err := NewCredentialWriter(store)
	if err != nil {
		t.Fatal(err)
	}
	observed, err := writer.Observe(context.Background(), "channel.telegram.provider")
	if err != nil || observed.ValueSeal == "" {
		t.Fatalf("Observe = %#v, %v", observed, err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), `"value_seal_key"`) || strings.Count(string(raw), `"value_seal`) != 1 ||
		!strings.Contains(string(raw), `"unrelated-a"`) || !strings.Contains(string(raw), `"leave-a"`) ||
		!strings.Contains(string(raw), `"unrelated-b"`) || !strings.Contains(string(raw), `"leave-b"`) ||
		strings.Contains(string(raw), `"epoch"`) {
		t.Fatalf("credential file did not gain only root seal custody:\n%s", raw)
	}
}

func TestOnboardingCredentialReleaseOwnsOnlyWrittenCurrentOccurrence(t *testing.T) {
	store, err := runtimecredentials.NewFileStore(filepath.Join(t.TempDir(), "credentials.json"))
	if err != nil {
		t.Fatal(err)
	}
	writer, err := NewCredentialWriter(store)
	if err != nil {
		t.Fatal(err)
	}
	written, err := writer.Admit(context.Background(), CredentialWriteRequest{StoreKey: "channel.telegram.provider", Value: "token-a", Receipt: "operation-a/provider"})
	if err != nil {
		t.Fatal(err)
	}
	admission := CredentialAdmission{Role: "provider", StoreKey: written.StoreKey, Kind: CredentialAdmissionWritten, Receipt: written.Receipt, ValueSeal: written.ValueSeal}
	if released, err := writer.Release(context.Background(), admission); err != nil || !released {
		t.Fatalf("release = %v, %v; want true, nil", released, err)
	}

	written, err = writer.Admit(context.Background(), CredentialWriteRequest{StoreKey: "channel.telegram.provider", Value: "token-b", Receipt: "operation-b/provider"})
	if err != nil {
		t.Fatal(err)
	}
	observed := CredentialAdmission{Role: "provider", StoreKey: written.StoreKey, Kind: CredentialAdmissionObserved, ValueSeal: written.ValueSeal}
	if released, err := writer.Release(context.Background(), observed); err != nil || released {
		t.Fatalf("observed release = %v, %v; want false, nil", released, err)
	}
	if value, found, err := store.Get(context.Background(), written.StoreKey); err != nil || !found || value != "token-b" {
		t.Fatalf("observed credential = %q, %v, %v", value, found, err)
	}
}

func TestCredentialWriterReleaseOperationReconcilesPhysicalAndCheckpointedWrites(t *testing.T) {
	for _, test := range []struct {
		name             string
		writtenRoles     int
		checkpointWrites bool
	}{
		{name: "partial physical write before checkpoint", writtenRoles: 1},
		{name: "complete physical writes before checkpoint", writtenRoles: 2},
		{name: "complete checkpointed writes", writtenRoles: 2, checkpointWrites: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			store, err := runtimecredentials.NewFileStore(filepath.Join(t.TempDir(), "credentials.json"))
			if err != nil {
				t.Fatal(err)
			}
			writer, err := NewCredentialWriter(store)
			if err != nil {
				t.Fatal(err)
			}
			op := Operation{
				OperationID: "operation-a",
				CredentialReservations: []CredentialReservation{
					{Role: "provider", StoreKey: "channel.telegram.provider"},
					{Role: "signing", StoreKey: "channel.telegram.signing"},
				},
			}
			written := make([]CredentialAdmission, 0, test.writtenRoles)
			for _, reservation := range op.CredentialReservations[:test.writtenRoles] {
				result, err := writer.Admit(context.Background(), CredentialWriteRequest{
					StoreKey: operationCredentialStoreKey(reservation.StoreKey, op.OperationID, reservation.Role),
					Value:    "operation-secret-" + reservation.Role, Receipt: credentialReceipt(op.OperationID, reservation.Role),
				})
				if err != nil {
					t.Fatal(err)
				}
				written = append(written, CredentialAdmission{
					Role: reservation.Role, StoreKey: result.StoreKey, Kind: CredentialAdmissionWritten,
					Receipt: result.Receipt, ValueSeal: result.ValueSeal,
				})
			}
			if test.checkpointWrites {
				op.CredentialAdmissions = append([]CredentialAdmission(nil), written...)
			}

			predecessor, err := writer.Admit(context.Background(), CredentialWriteRequest{
				StoreKey: "channel.telegram.predecessor", Value: "predecessor-secret", Receipt: "predecessor-receipt",
			})
			if err != nil {
				t.Fatal(err)
			}
			op.CredentialAdmissions = append(op.CredentialAdmissions, CredentialAdmission{
				Role: "provider", StoreKey: predecessor.StoreKey, Kind: CredentialAdmissionObserved,
				ValueSeal: predecessor.ValueSeal,
			})
			successorReservation := op.CredentialReservations[0]
			successor, err := writer.Admit(context.Background(), CredentialWriteRequest{
				StoreKey: operationCredentialStoreKey(successorReservation.StoreKey, "operation-b", successorReservation.Role),
				Value:    "successor-secret", Receipt: credentialReceipt("operation-b", successorReservation.Role),
			})
			if err != nil {
				t.Fatal(err)
			}

			if err := writer.ReleaseOperation(context.Background(), op); err != nil {
				t.Fatal(err)
			}
			for _, admission := range written {
				if _, found, err := store.Get(context.Background(), admission.StoreKey); err != nil || found {
					t.Fatalf("operation credential %q found=%v err=%v", admission.StoreKey, found, err)
				}
			}
			for _, retained := range []CredentialWriteResult{predecessor, successor} {
				if _, found, err := store.Get(context.Background(), retained.StoreKey); err != nil || !found {
					t.Fatalf("foreign credential %q found=%v err=%v", retained.StoreKey, found, err)
				}
			}
		})
	}
}

func TestCredentialWriterReleaseOperationRetainsExactBindingEvidence(t *testing.T) {
	store, err := runtimecredentials.NewFileStore(filepath.Join(t.TempDir(), "credentials.json"))
	if err != nil {
		t.Fatal(err)
	}
	writer, err := NewCredentialWriter(store)
	if err != nil {
		t.Fatal(err)
	}
	op := Operation{
		OperationID: "operation-a",
		CredentialReservations: []CredentialReservation{
			{Role: "provider", StoreKey: "channel.telegram.provider"},
			{Role: "signing", StoreKey: "channel.telegram.signing"},
		},
	}
	for _, reservation := range op.CredentialReservations {
		written, err := writer.Admit(context.Background(), CredentialWriteRequest{
			StoreKey: operationCredentialStoreKey(reservation.StoreKey, op.OperationID, reservation.Role),
			Value:    reservation.Role + "-secret", Receipt: credentialReceipt(op.OperationID, reservation.Role),
		})
		if err != nil {
			t.Fatal(err)
		}
		op.CredentialAdmissions = append(op.CredentialAdmissions, CredentialAdmission{
			Role: reservation.Role, StoreKey: written.StoreKey, Kind: CredentialAdmissionWritten,
			Receipt: written.Receipt, ValueSeal: written.ValueSeal,
		})
	}
	provider := op.CredentialAdmissions[0]
	if err := writer.ReleaseOperation(context.Background(), op, runtimecredentials.ValueEvidence{Key: provider.StoreKey, Seal: provider.ValueSeal}); err != nil {
		t.Fatal(err)
	}
	if value, found, err := store.Get(context.Background(), provider.StoreKey); err != nil || !found || value != "provider-secret" {
		t.Fatalf("retained provider credential = %q, found=%v, err=%v", value, found, err)
	}
	signing := op.CredentialAdmissions[1]
	if _, found, err := store.Get(context.Background(), signing.StoreKey); err != nil || found {
		t.Fatalf("retired signing credential found=%v, err=%v", found, err)
	}
}

func testValueSeal(digit byte) runtimecredentials.ValueSeal {
	return runtimecredentials.ValueSeal("credential-value-seal-v1:" + strings.Repeat(string(digit), 64))
}

func TestCredentialWriterReleaseOperationRejectsForeignWrittenAdmission(t *testing.T) {
	store, err := runtimecredentials.NewFileStore(filepath.Join(t.TempDir(), "credentials.json"))
	if err != nil {
		t.Fatal(err)
	}
	writer, err := NewCredentialWriter(store)
	if err != nil {
		t.Fatal(err)
	}
	reservation := CredentialReservation{Role: "provider", StoreKey: "channel.telegram.provider"}
	foreign, err := writer.Admit(context.Background(), CredentialWriteRequest{
		StoreKey: operationCredentialStoreKey(reservation.StoreKey, "operation-b", reservation.Role),
		Value:    "successor-secret", Receipt: credentialReceipt("operation-b", reservation.Role),
	})
	if err != nil {
		t.Fatal(err)
	}
	op := Operation{OperationID: "operation-a", CredentialReservations: []CredentialReservation{reservation}, CredentialAdmissions: []CredentialAdmission{{
		Role: reservation.Role, StoreKey: foreign.StoreKey, Kind: CredentialAdmissionWritten,
		Receipt: foreign.Receipt, ValueSeal: foreign.ValueSeal,
	}}}
	if err := writer.ReleaseOperation(context.Background(), op); err == nil || !strings.Contains(err.Error(), "not owned") {
		t.Fatalf("foreign written cleanup error = %v", err)
	}
	if _, found, err := store.Get(context.Background(), foreign.StoreKey); err != nil || !found {
		t.Fatalf("foreign credential found=%v err=%v", found, err)
	}
}
