package runtime

import (
	"crypto/sha256"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/providerconnectors"
	"github.com/division-sh/swarm/internal/providertriggers"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/division-sh/swarm/internal/testutil/packfixture"
	platformpacks "github.com/division-sh/swarm/packs"
)

func TestSchemaImportCanonicalProviderSpellings2287(t *testing.T) {
	repo := canonicalrouting.RepoRoot(t)
	connectors := packfixture.ConnectorRegistry(t)
	triggerBody, err := fs.ReadFile(platformpacks.FS(), "provider-triggers/telegram/trigger.yaml")
	if err != nil {
		t.Fatal(err)
	}
	triggerBody = []byte(strings.ReplaceAll(string(triggerBody), "telegram", "telegram_test"))
	manifest, err := providertriggers.ParseManifest(triggerBody)
	if err != nil {
		t.Fatal(err)
	}
	triggers, err := providertriggers.NewCatalogSnapshot(providertriggers.CatalogEntry{
		Identity: providertriggers.PackIdentity{
			ID: "provider.telegram_test", Version: "1.0.0",
			ManifestHash: fmt.Sprintf("sha256:%x", sha256.Sum256(triggerBody)), Provenance: "external",
		},
		Manifest: manifest, SourcePath: filepath.Join(t.TempDir(), "provider.telegram_test"), Source: "canonical import proof",
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range []struct {
		name, provider, member, imported, diagnostic string
		resolve                                      func(*runtimecontracts.WorkflowContractBundle) error
	}{
		{
			name: "connector_packs", provider: "microsoft_graph", member: "tool", imported: "microsoft_graph.send_mail",
			diagnostic: "connector import must use exact canonical tokens",
			resolve: func(bundle *runtimecontracts.WorkflowContractBundle) error {
				source, err := providerconnectors.SourceWithConnectorPackImports(semanticview.Wrap(bundle), connectors)
				if err != nil {
					return err
				}
				if _, ok := source.ToolEntries()["microsoft_graph.send_mail"]; !ok {
					return fmt.Errorf("canonical connector import did not resolve its installed tool")
				}
				return nil
			},
		},
		{
			name: "provider_trigger_events", provider: "telegram_test", member: "event", imported: "inbound.telegram_test.text_message",
			diagnostic: "provider trigger import must use exact canonical tokens",
			resolve: func(bundle *runtimecontracts.WorkflowContractBundle) error {
				source, err := SourceWithProviderTriggerEvents(semanticview.Wrap(bundle), triggers)
				if err != nil {
					return err
				}
				entry, ok := source.EventEntry("inbound.telegram_test.text_message")
				if !ok || entry.Payload.Properties["text"].Type != "text" {
					return fmt.Errorf("canonical trigger import did not resolve its admitted event schema")
				}
				return nil
			},
		},
	} {
		t.Run(family.name, func(t *testing.T) {
			for _, entrance := range []string{"directory", "retained_source"} {
				t.Run(entrance, func(t *testing.T) {
					for _, spelling := range []struct{ name, separator string }{
						{"underscore", "_"}, {"hyphen", "-"}, {"space", " "},
					} {
						t.Run(spelling.name, func(t *testing.T) {
							provider := strings.ReplaceAll(family.provider, "_", spelling.separator)
							body := fmt.Sprintf("name: import-spelling\nimports:\n  %s:\n    - provider: %s\n      %s: %s\n", family.name, provider, family.member, family.imported)
							root := t.TempDir()
							if err := os.WriteFile(filepath.Join(root, "schema.yaml"), []byte(body), 0o600); err != nil {
								t.Fatal(err)
							}
							var bundle *runtimecontracts.WorkflowContractBundle
							var err error
							if entrance == "directory" {
								bundle, err = runtimecontracts.LoadWorkflowContractBundleWithOverrides(repo, root, runtimecontracts.DefaultPlatformSpecFile(repo))
							} else {
								artifact, admitErr := sourceartifact.AdmitDirectory(root)
								if admitErr != nil {
									t.Fatal(admitErr)
								}
								retained, decodeErr := sourceartifact.DecodeLogical(artifact.LogicalBlob())
								if decodeErr != nil {
									t.Fatal(decodeErr)
								}
								if err := os.RemoveAll(root); err != nil {
									t.Fatal(err)
								}
								bundle, err = runtimecontracts.LoadWorkflowContractBundleFromArtifact(repo, retained, runtimecontracts.DefaultPlatformSpecFile(repo), runtimecontracts.WorkflowContractLoadOptions{})
							}
							if spelling.name != "underscore" {
								if err == nil || bundle != nil || !strings.Contains(err.Error(), family.diagnostic) || !strings.Contains(err.Error(), "schema.yaml:4:7") {
									t.Fatalf("%s spelling must fail at import admission with its canonical-token location, got bundle=%t error=%v", spelling.name, bundle != nil, err)
								}
								return
							}
							if err != nil {
								t.Fatalf("canonical import bundle: %v", err)
							}
							if err := family.resolve(bundle); err != nil {
								t.Fatalf("canonical import must resolve before its spelling refusals count: %v", err)
							}
						})
					}
				})
			}
		})
	}
}
