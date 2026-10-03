package serveapp

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/runtime/credentials"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/servedparity"
)

func TestChannelSigningFallbackPublicRefusalBothStores(t *testing.T) {
	for _, backend := range servedparity.RequiredBackends {
		for _, previous := range []bool{false, true} {
			for _, entry := range []string{"start", "retry"} {
				for _, state := range []string{"absent", "valid", "empty", "whitespace", "corrupt_file", "read_error"} {
					keyKind := "reserved"
					if previous {
						keyKind = "previous_current"
					}
					t.Run(string(backend)+"/"+keyKind+"/"+entry+"/"+state, func(t *testing.T) {
						h := newChannelOnboardingE2EHarness(t, backend, true)
						h.start(t)
						verb, key := channelonboarding.VerbConnect, ""
						operationID := ""
						if previous {
							var begun channelonboarding.Result
							requireServedJSONRPCResult(t, h.rpcEndpoint(), "channel.onboarding_start", map[string]any{
								"provider": "telegram", "verb": string(verb), "provider_credential": "original-token",
							}, &begun)
							claimed := claimPendingResetRPC(t, h, begun, 78001)
							var confirmed map[string]any
							requireServedJSONRPCResult(t, h.rpcEndpoint(), "channel.confirm", map[string]any{
								"operation_id": claimed.OperationID, "expected_revision": claimed.Revision, "approve": true,
							}, &confirmed)
							ready := retryChannelOnboardingRPC(t, h, begun.Operation.OperationID, "")
							if ready.Operation.Phase != channelonboarding.PhaseSucceeded {
								t.Fatalf("predecessor ceremony did not complete: %#v", ready)
							}
							for _, admission := range ready.Operation.CredentialAdmissions {
								if admission.Role == ready.Candidate.SigningCredentialRole {
									key = admission.StoreKey
								}
							}
							verb = channelonboarding.VerbReconnect
							if entry == "retry" {
								// A real public missing-provider attempt reserves the reconnect
								// without registration or a fabricated operation checkpoint.
								file, err := credentials.NewFileStore(h.credentialPath)
								if err != nil {
									t.Fatal(err)
								}
								for _, admission := range ready.Operation.CredentialAdmissions {
									if admission.Role == ready.Candidate.ProviderCredentialRole {
										if err := file.Delete(context.Background(), admission.StoreKey); err != nil {
											t.Fatal(err)
										}
									}
								}
								response := requestServedJSONRPC(t, h.rpcEndpoint(), "channel.onboarding_start", map[string]any{"provider": "telegram", "verb": string(verb)})
								if response.Error == nil {
									t.Fatal("missing-provider reconnect unexpectedly advanced")
								}
								for _, row := range readChannelOnboardingRows(t, h.opts.ConfigPath, h.endpoint) {
									if row.Operation != nil && row.Operation.Phase == string(channelonboarding.PhasePreparing) {
										if operationID != "" {
											t.Fatal("ambiguous public reconnect responsibility")
										}
										operationID = row.Operation.OperationID
									}
								}
								if operationID == "" {
									t.Fatal("public reconnect responsibility unavailable")
								}
							}
						} else {
							response := requestServedJSONRPC(t, h.rpcEndpoint(), "channel.onboarding_start", map[string]any{
								"provider": "telegram", "verb": string(verb),
								"idempotency_key": "reserved-credential-proof",
							})
							if response.Error == nil {
								t.Fatal("credential-free connect did not retain a preparing responsibility")
							}
							rows := readChannelOnboardingRows(t, h.opts.ConfigPath, h.endpoint)
							if len(rows) != 1 || rows[0].Operation == nil {
								t.Fatalf("reserved responsibility = %#v", rows)
							}
							operationID = rows[0].Operation.OperationID
							result := getChannelOnboardingRPC(t, h, operationID)
							for _, reservation := range result.Operation.CredentialReservations {
								if reservation.Role == result.Candidate.SigningCredentialRole {
									key = reservation.StoreKey
								}
							}
						}
						if key == "" {
							t.Fatal("exact signing key is unavailable")
						}
						file, err := credentials.NewFileStore(h.credentialPath)
						if err != nil {
							t.Fatal(err)
						}
						if state == "absent" {
							if err := file.Delete(context.Background(), key); err != nil {
								t.Fatal(err)
							}
							if err := file.Set(context.Background(), "proof.other", "unrelated-private-value"); err != nil {
								t.Fatal(err)
							}
						} else if !previous || state == "empty" || state == "whitespace" {
							value := "reserved-signing-value"
							if state == "empty" {
								value = ""
							}
							if state == "whitespace" {
								value = " \t\n"
							}
							if err := file.Set(context.Background(), key, value); err != nil {
								t.Fatal(err)
							}
						}
						original, err := os.ReadFile(h.credentialPath)
						if err != nil {
							t.Fatal(err)
						}
						var originalKeys []string
						if state != "corrupt_file" && state != "read_error" {
							originalKeys, err = file.List(context.Background())
							if err != nil {
								t.Fatal(err)
							}
						} else if state == "corrupt_file" {
							if err := os.WriteFile(h.credentialPath, []byte("{invalid credential document"), 0o600); err != nil {
								t.Fatal(err)
							}
						} else {
							if err := os.Remove(h.credentialPath); err != nil {
								t.Fatal(err)
							}
							if err := os.Mkdir(h.credentialPath, 0o700); err != nil {
								t.Fatal(err)
							}
						}
						registrations, deliveries := h.provider.Counts()
						method := "channel.onboarding_start"
						params := map[string]any{"provider": "telegram", "verb": string(verb), "provider_credential": "replacement-token"}
						if !previous {
							params["idempotency_key"] = "reserved-credential-proof"
						}
						if entry == "retry" {
							method = "channel.onboarding_retry"
							params = map[string]any{"operation_id": operationID, "provider_credential": "replacement-token"}
						}
						response := requestServedJSONRPC(t, h.rpcEndpoint(), method, params)
						if state == "valid" || state == "absent" && !previous {
							if response.Error != nil {
								t.Fatalf("valid/absent positive %s failed: %#v", method, response.Error)
							}
							var admitted channelonboarding.Result
							if err := json.Unmarshal(response.Result, &admitted); err != nil || admitted.IdentityOperation == nil {
								t.Fatalf("public credential admission = %#v %v", admitted, err)
							}
							for _, admission := range admitted.Operation.CredentialAdmissions {
								if admission.Role != admitted.Candidate.SigningCredentialRole {
									continue
								}
								wantKind := channelonboarding.CredentialAdmissionObserved
								if state == "absent" {
									wantKind = channelonboarding.CredentialAdmissionWritten
								}
								if admission.Kind != wantKind {
									t.Fatalf("signing admission kind=%s want=%s", admission.Kind, wantKind)
								}
							}
							wantRegistrations := registrations + 1
							if previous {
								wantRegistrations = registrations
							}
							if gotRegistration, _ := h.provider.Counts(); gotRegistration != wantRegistrations {
								t.Fatalf("positive admission registration=%d want=%d", gotRegistration, wantRegistrations)
							}
							h.stop(t)
							return
						}
						if response.Error == nil {
							t.Fatalf("public %s replaced an invalid signing observation: %#v", method, response)
						}
						if state == "empty" || state == "whitespace" || state == "absent" {
							raw, err := json.Marshal(response.Error.Data)
							if err != nil {
								t.Fatal(err)
							}
							var failureData struct {
								Details struct {
									Failure runtimefailures.Envelope `json:"failure"`
								} `json:"details"`
							}
							if err := json.Unmarshal(raw, &failureData); err != nil {
								t.Fatal(err)
							}
							failure := failureData.Details.Failure
							if state == "absent" {
								if !strings.Contains(string(raw), "CHANNEL_CREDENTIAL_REQUIRED") || failure.Detail.Code == "credential_value_unusable" {
									t.Fatalf("genuine absence classification changed: %s", raw)
								}
							} else if failure.Class != runtimefailures.ClassAuthenticationNeeded || failure.Detail.Code != "credential_value_unusable" || failure.Detail.Attributes["auth_kind"] != "channel_credential" || failure.Retryable || !failure.Deterministic {
								t.Errorf("unusable-value refusal was lost: class=%s detail=%s; want %s/credential_value_unusable", failure.Class, failure.Detail.Code, runtimefailures.ClassAuthenticationNeeded)
							}
							for _, private := range []string{"replacement-token", "original-token", "unrelated-private-value", "value_seal", "receipt"} {
								if strings.Contains(string(raw), private) {
									t.Errorf("public refusal leaked %q: %s", private, raw)
								}
							}
							current, err := os.ReadFile(h.credentialPath)
							if err != nil || !bytes.Equal(current, original) {
								t.Fatalf("failed admission mutated credential evidence: equal=%t err=%v", bytes.Equal(current, original), err)
							}
							keys, err := file.List(context.Background())
							if err != nil || !reflect.DeepEqual(keys, originalKeys) {
								t.Fatalf("failed admission wrote a candidate credential: %#v -> %#v, %v", originalKeys, keys, err)
							}
						} else {
							raw, err := json.Marshal(response.Error.Data)
							if err != nil || strings.Contains(string(raw), "credential_value_unusable") || strings.Contains(string(raw), "CHANNEL_CREDENTIAL_REQUIRED") {
								t.Fatalf("I/O/corruption became credential absence: %s %v", raw, err)
							}
							if state == "read_error" {
								info, err := os.Stat(h.credentialPath)
								if err != nil || !info.IsDir() {
									t.Fatalf("failed observation replaced the unavailable credential tier: info=%v err=%v", info, err)
								}
								if err := os.Remove(h.credentialPath); err != nil {
									t.Fatal(err)
								}
							} else {
								current, err := os.ReadFile(h.credentialPath)
								if err != nil || !bytes.Equal(current, []byte("{invalid credential document")) {
									t.Fatalf("failed observation rewrote the corrupt credential tier: err=%v", err)
								}
							}
							if err := os.WriteFile(h.credentialPath, original, 0o600); err != nil {
								t.Fatal(err)
							}
						}
						if gotRegistration, gotDelivery := h.provider.Counts(); gotRegistration != registrations || gotDelivery != deliveries {
							t.Fatalf("refused observation caused provider effects: registration %d->%d delivery %d->%d", registrations, gotRegistration, deliveries, gotDelivery)
						}
						h.stop(t)
					})
				}
			}
		}
	}
}
