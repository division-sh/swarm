package runtimepersistence

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/durabledata"
	"github.com/division-sh/swarm/internal/operatorchannel"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	"github.com/division-sh/swarm/internal/runtime/triggergeneration"
	"github.com/division-sh/swarm/internal/sourceartifact"
)

func Test2376ChannelExactSelectionAndStoredPresentationBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, name := range []string{"", "Reception", "Reception\u202e"} {
			named := name != ""
			t.Run(fmt.Sprintf("%s/name=%q", backend, name), func(t *testing.T) {
				fixture := openChannelOnboardingConfirmationFixture(t, backend)
				selected := fixture.store.(interface {
					sourceartifact.Reader
					EnsureSourceArtifactWithData(context.Context, *sourceartifact.AdmittedSourceArtifact, durabledata.Catalog) (sourceartifact.EnsureResult, error)
				})
				ctx := context.Background()
				now := time.Now().UTC()
				principal, err := fixture.store.EnsureOperatorPrincipal(ctx, now)
				if err != nil {
					t.Fatal(err)
				}
				candidates := []channelonboarding.Candidate{}
				operations := []channelonboarding.Operation{}
				for i, seed := range []int{5432, 7316} {
					root := filepath.Join(t.TempDir(), fmt.Sprintf("store-%d", i))
					if err := os.Mkdir(root, 0700); err != nil {
						t.Fatal(err)
					}
					if err := os.WriteFile(filepath.Join(root, "schema.yaml"), []byte(fmt.Sprintf("description: candidate-%d\n", seed)), 0600); err != nil {
						t.Fatal(err)
					}
					if named {
						if err := os.WriteFile(filepath.Join(root, "manifest.yaml"), []byte(fmt.Sprintf("name: %q\nversion: 1.0.0\nplatform_version: '*'\n", name)), 0600); err != nil {
							t.Fatal(err)
						}
					}
					artifact, err := sourceartifact.AdmitDirectory(root)
					if err != nil {
						t.Fatal(err)
					}
					if _, err := selected.EnsureSourceArtifactWithData(ctx, artifact, durabledata.Catalog{BundleHash: artifact.BundleHash()}); err != nil {
						t.Fatal(err)
					}
					request := channelOnboardingStartRequest(principal.ID, now)
					request.RequestKeyHash, request.RequestHash = fmt.Sprintf("2376-key-%d", i), fmt.Sprintf("2376-input-%d", i)
					request.Coordinate.BundleHash = artifact.BundleHash()
					request.Coordinate.BundleIdentity = "exact-source:" + artifact.BundleHash()
					op, err := fixture.store.ReserveChannelOnboarding(ctx, request)
					if err != nil {
						t.Fatal(err)
					}
					operations = append(operations, op)
					candidates = append(candidates, channelonboarding.Candidate{
						Provider: request.Provider, Interface: request.Interface, Coordinate: request.Coordinate, SourceLabel: artifact.HumanLabel(),
						Target:  channelonboarding.CandidateTarget{Selector: request.TargetSelector, ServiceID: "service", FlowPath: "support/flow", Alias: "telegram", Provider: "telegram", Generation: request.Coordinate.TargetGeneration, PublicationSequence: 1, AdmissionGeneration: triggergeneration.FromCanonicalBytes([]byte("2376")), SigningCredentialKey: "telegram_signing"},
						Posture: request.Posture, Ceremony: request.Ceremony, ProviderCredentialRole: "bot_token", SigningCredentialRole: "webhook_signing", ConfirmationOperation: "deliver",
					})
					if err := os.RemoveAll(root); err != nil {
						t.Fatal(err)
					}
					record, err := selected.GetSourceArtifact(ctx, artifact.BundleHash())
					if err != nil {
						t.Fatal(err)
					}
					stored, err := record.Decode()
					if err != nil || stored.BundleHash() != artifact.BundleHash() {
						t.Fatalf("exact stored source changed: %v", err)
					}
					if named {
						metadata, present := stored.RootManifest()
						if !present || metadata.Name != name {
							t.Fatalf("display sanitization rewrote stored metadata: %#v", metadata)
						}
					}
				}
				if !named && (sourceartifact.ShortHashLabel(candidates[0].Coordinate.BundleHash) != "fa84932" || sourceartifact.ShortHashLabel(candidates[1].Coordinate.BundleHash) != "fa84932") {
					t.Fatal("literal collision vector changed")
				}
				for _, order := range [][]channelonboarding.Candidate{candidates, {candidates[1], candidates[0]}} {
					catalog, err := channelonboarding.NewCandidateCatalog(order)
					if err != nil {
						t.Fatal(err)
					}
					if _, err := catalog.Resolve(channelonboarding.CandidateSelection{Provider: "telegram"}); !errors.Is(err, channelonboarding.ErrConflict) {
						t.Fatalf("multiple candidates not ambiguous: %v", err)
					}
					for _, candidate := range order {
						got, err := catalog.Resolve(channelonboarding.CandidateSelection{Provider: "telegram", BundleHash: candidate.Coordinate.BundleHash, InterfaceSelector: candidate.Interface.Selector, TargetSelector: candidate.Target.Selector})
						if err != nil || !got.Coordinate.Matches(candidate.Coordinate) {
							t.Fatalf("exact selector=%+v / %v", got, err)
						}
						for _, display := range []string{candidate.SourceLabel, sourceartifact.ShortHashLabel(candidate.Coordinate.BundleHash)} {
							if _, err := catalog.Resolve(channelonboarding.CandidateSelection{Provider: "telegram", BundleHash: display}); !errors.Is(err, channelonboarding.ErrNotFound) {
								t.Fatalf("presentation selected source: %q / %v", display, err)
							}
						}
					}
				}
				for _, count := range []int{0, 1} {
					catalog, err := channelonboarding.NewCandidateCatalog(candidates[:count])
					if err != nil {
						t.Fatal(err)
					}
					got, err := catalog.Resolve(channelonboarding.CandidateSelection{Provider: "telegram"})
					if count == 0 && !errors.Is(err, channelonboarding.ErrNotFound) || count == 1 && (err != nil || !got.Coordinate.Matches(candidates[0].Coordinate)) {
						t.Fatalf("count=%d selector=%+v / %v", count, got, err)
					}
				}
				// Readback is real selected-store identity/onboarding composition,
				// without provider execution or a credential-readiness waiver.
				file, err := runtimecredentials.NewFileStore(filepath.Join(t.TempDir(), "credentials.json"))
				if err != nil {
					t.Fatal(err)
				}
				proofs, err := operatorchannel.NewFileProofStore(t.TempDir())
				if err != nil {
					t.Fatal(err)
				}
				identities, err := operatorchannel.NewService(fixture.store, proofs, &handoffCurrentness{file: file}, []operatorchannel.InterfaceIdentity{candidates[0].Interface}, "2376-session")
				if err != nil {
					t.Fatal(err)
				}
				if _, _, err := identities.Bootstrap(ctx, now); err != nil {
					t.Fatal(err)
				}
				writer, err := channelonboarding.NewCredentialWriter(file)
				if err != nil {
					t.Fatal(err)
				}
				catalog, err := channelonboarding.NewCandidateCatalog(nil)
				if err != nil {
					t.Fatal(err)
				}
				service, err := channelonboarding.NewService(channelonboarding.ServiceOptions{Store: fixture.store, Identities: identities, Credentials: writer, SourceArtifacts: selected, Catalog: func() (*channelonboarding.CandidateCatalog, error) { return catalog, nil }, Activations: retiredOnboardingActivations{}, Confirmation: retiredOnboardingConfirmation{}, Readiness: retiredOnboardingReadiness{}, Now: func() time.Time { return now }})
				if err != nil {
					t.Fatal(err)
				}
				for attempt := 0; attempt < 2; attempt++ {
					rows, err := service.ReadbackConnectedChannels(ctx)
					if err != nil || len(rows) != 2 {
						t.Fatalf("stored readback=%+v / %v", rows, err)
					}
					seen := map[string]bool{}
					for _, row := range rows {
						if row.Operation == nil {
							t.Fatalf("missing exact operation: %+v", row)
						}
						op := row.Operation
						want := strings.ReplaceAll(name, "\u202e", " ") + "@1.0.0"
						if !named {
							want = "fa84932"
						}
						if row.SourceLabel != want || strings.Contains(row.SourceLabel, "store-") {
							t.Fatalf("stored source borrowed a directory: %+v", row)
						}
						seen[op.Coordinate.BundleHash] = true
						persisted, err := fixture.store.GetChannelOnboarding(ctx, op.OperationID)
						if err != nil || !reflect.DeepEqual(persisted, *op) || persisted.Coordinate.BundleIdentity != "exact-source:"+op.Coordinate.BundleHash {
							t.Fatalf("readback changed durable authority: %+v / %v", persisted, err)
						}
					}
					for _, op := range operations {
						if !seen[op.Coordinate.BundleHash] {
							t.Fatal("display collision collapsed a stored row")
						}
					}
				}
			})
		}
	}
}
