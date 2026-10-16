package sourceartifact

import (
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/packmodel"
)

const s1ProviderManifest = "version: 1.2.3\nplatform_version: '*'\ncapabilities:\n  can:\n    emit_events: [inbound.team.message]\n    receive_https_route: /webhooks/{alias}/{provider}\n    persist_dedupe_markers: true\n  cannot: [execute arbitrary code]\n"

func TestS1ProviderMembershipDiskLogicalParity(t *testing.T) {
	root := t.TempDir()
	for label, body := range map[string]string{
		"nodes.yaml":             "coordinator: {type: procedure}\n",
		"team-bot/manifest.yaml": s1ProviderManifest,
		"team-bot/trigger.yaml":  "event_name: {literal: inbound.team}\n",
		"child/data/value.txt":   "resource-established child\n",
	} {
		writeTestFile(t, root, label, body)
	}
	admitted, err := AdmitDirectory(root)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeLogical(admitted.LogicalBlob())
	if err != nil {
		t.Fatal(err)
	}
	for _, artifact := range []*AdmittedSourceArtifact{admitted, decoded} {
		children := artifact.Root().Children()
		if len(children) != 1 || children[0].Path() != "child" {
			t.Fatalf("provider acquired executable flow identity: %#v", children)
		}
		manifest, ok := artifact.Manifest("team-bot/manifest.yaml")
		if !ok || manifest.Kind() != packmodel.DirectoryProvider || manifest.Version != "1.2.3" {
			t.Fatalf("provider manifest missing: %+v, %t", manifest, ok)
		}
		if artifact.BundleHash() != admitted.BundleHash() {
			t.Fatal("retained membership changed source identity")
		}
	}
}

func TestS1ManifestRetiredFieldsTeachCanonicalOwners(t *testing.T) {
	for _, row := range []struct {
		field, value, teaching string
	}{
		{"name", "team-bot", "the directory is the name"},
		{"type", "provider", "a directory is a provider or a flow by the files it holds"},
		{"requires", "{secrets: [token]}", "credentials are declared on the action, trigger or channel that uses them"},
		{"requires", "{managed_credentials: [oauth]}", "credentials are declared on the action, trigger or channel that uses them"},
		{"implements", "[swarm.hitl-channel/v2]", "not a manifest key"},
	} {
		t.Run(row.field+row.value, func(t *testing.T) {
			root := t.TempDir()
			writeTestFile(t, root, "schema.yaml", "description: flow\n")
			writeTestFile(t, root, "manifest.yaml", manifest2376+row.field+": "+row.value+"\n")
			_, err := AdmitDirectory(root)
			if err == nil || !strings.Contains(err.Error(), row.teaching) || !strings.Contains(err.Error(), "manifest.yaml:") {
				t.Fatalf("missing located teaching for retired %s: %v", row.field, err)
			}
		})
	}
}

func TestS1ProviderMembershipRefusesMixedOrContradictoryDirectory(t *testing.T) {
	for _, row := range []struct {
		name, directory, manifest, extra, teaching string
	}{
		{"mixed", "team-bot", s1ProviderManifest, "schema.yaml", "holds both flow files and provider bodies"},
		{"authored name", "team-old", "name: team-bot\n" + s1ProviderManifest, "", "the directory is the name"},
		{"missing manifest", "team-bot", "", "", "manifest.yaml"},
	} {
		t.Run(row.name, func(t *testing.T) {
			root := t.TempDir()
			files := map[string]string{
				"schema.yaml":                   "description: root\n",
				row.directory + "/trigger.yaml": "event_name: {literal: inbound.team}\n",
			}
			if row.manifest != "" {
				files[row.directory+"/manifest.yaml"] = row.manifest
			}
			if row.extra != "" {
				files[row.directory+"/"+row.extra] = "description: not a provider body\n"
			}
			entries := make([]Entry, 0, len(files))
			for label, body := range files {
				writeTestFile(t, root, label, body)
				disposition, err := classifyLabel(label)
				if err != nil {
					t.Fatal(err)
				}
				entries = append(entries, Entry{label: label, disposition: disposition, body: []byte(body)})
			}
			sort.Slice(entries, func(i, j int) bool { return entries[i].label < entries[j].label })
			blob, err := encodeLogical(entries)
			if err != nil {
				t.Fatal(err)
			}
			for _, consumer := range []struct {
				name string
				load func() (*AdmittedSourceArtifact, error)
			}{
				{"disk", func() (*AdmittedSourceArtifact, error) { return AdmitDirectory(root) }},
				{"construct", func() (*AdmittedSourceArtifact, error) { return newArtifact(entries) }},
				{"logical", func() (*AdmittedSourceArtifact, error) { return DecodeLogical(blob) }},
			} {
				t.Run(consumer.name, func(t *testing.T) {
					_, err := consumer.load()
					if err == nil || !strings.Contains(err.Error(), row.teaching) {
						t.Fatalf("%s: error = %v, want %q", row.name, err, row.teaching)
					}
				})
			}
		})
	}
}

func TestS1ProviderNamesDoNotCollapseAndSnapshotsStayOwned(t *testing.T) {
	root := t.TempDir()
	writeTestFile(t, root, "schema.yaml", "description: root\n")
	for _, name := range []string{"team-bot", "team_bot"} {
		writeTestFile(t, root, name+"/manifest.yaml", s1ProviderManifest)
		writeTestFile(t, root, name+"/trigger.yaml", "event_name: {literal: inbound.team}\n")
	}
	artifact, err := AdmitDirectory(root)
	if err != nil {
		t.Fatal(err)
	}
	providers := artifact.ProviderDirectories()
	if len(providers) != 2 || providers[0].Path() != "team-bot" || providers[1].Path() != "team_bot" {
		t.Fatalf("distinct provider keys collapsed: %+v", providers)
	}
	for _, provider := range providers {
		if provider.Manifest().Kind() != packmodel.DirectoryProvider || provider.FlowPath() != "." {
			t.Fatalf("identity changed: %+v", provider)
		}
		if label, ok := provider.Body(packmodel.BodyTrigger); !ok || label != provider.Path()+"/trigger.yaml" {
			t.Fatalf("body detached from provider: %s, %t", label, ok)
		}
	}
	providers[0].directory = "changed"
	providers[0].manifest.Version = "9.9.9"
	flow := artifact.Root()
	flow.providers[0] = "changed"
	if next := artifact.ProviderDirectories(); next[0].Path() != "team-bot" || next[0].Manifest().Version != "1.2.3" || artifact.Root().ProviderDirectories()[0] != "team-bot" {
		t.Fatal("mutable readback changed admitted membership")
	}
}

func TestS1ProviderMembershipEmptyMixedAndRetiredHeaders(t *testing.T) {
	for _, role := range []string{"trigger", "connector", "channel"} {
		for _, malformed := range []string{"provider: team-bot\n", "<<: {provider: team-bot}\n", "null\n", "[]\n", "false\n"} {
			t.Run(role+"/"+malformed, func(t *testing.T) {
				root := t.TempDir()
				writeTestFile(t, root, "schema.yaml", "description: root\n")
				writeTestFile(t, root, "team-bot/manifest.yaml", s1ProviderManifest)
				writeTestFile(t, root, "team-bot/"+role+".yaml", malformed)
				_, err := AdmitDirectory(root)
				if err == nil || !strings.Contains(err.Error(), "team-bot/"+role+".yaml:") {
					t.Fatalf("malformed body gained flow or provider authority: %v", err)
				}
				if strings.Contains(malformed, "provider:") && !strings.Contains(err.Error(), "provider is not a body key; the directory is the provider name") {
					t.Fatalf("retired body header lost teaching: %v", err)
				}
			})
		}
	}
	for _, rootManifest := range []bool{false, true} {
		t.Run(fmt.Sprint("manifest_only/", rootManifest), func(t *testing.T) {
			root := t.TempDir()
			directory := "."
			if !rootManifest {
				writeTestFile(t, root, "schema.yaml", "description: root\n")
				directory = "team-bot"
			}
			writeTestFile(t, root, filepath.Join(directory, "manifest.yaml"), s1ProviderManifest)
			_, err := AdmitDirectory(root)
			if err == nil || !strings.Contains(err.Error(), "directory "+directory+" has a manifest and no body or flow files") {
				t.Fatalf("empty manifest directory gained kind: %v", err)
			}
		})
	}
}

func TestS1ProviderRootIsNotAnExecutableFlow(t *testing.T) {
	root := filepath.Join(t.TempDir(), "team-bot")
	writeTestFile(t, root, "manifest.yaml", s1ProviderManifest)
	writeTestFile(t, root, "trigger.yaml", "event_name: {literal: inbound.team}\n")
	artifact, err := AdmitDirectory(root)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeLogical(artifact.LogicalBlob())
	if err != nil {
		t.Fatal(err)
	}
	for _, source := range []*AdmittedSourceArtifact{artifact, decoded} {
		if source.Root() != nil {
			t.Fatal("selected provider root became a flow")
		}
		if providers := source.ProviderDirectories(); len(providers) != 1 || providers[0].Path() != "." || providers[0].FlowPath() != "" {
			t.Fatalf("provider root acquired ambient flow ownership: %+v", providers)
		}
	}
}

func TestS1ProviderCannotOwnNestedProviderDirectories(t *testing.T) {
	for _, selectedProvider := range []bool{false, true} {
		t.Run(fmt.Sprint(selectedProvider), func(t *testing.T) {
			root := t.TempDir()
			prefix := "team-bot/"
			if selectedProvider {
				prefix = ""
			} else {
				writeTestFile(t, root, "schema.yaml", "description: root\n")
			}
			for _, directory := range []string{prefix, prefix + "nested/"} {
				writeTestFile(t, root, directory+"manifest.yaml", s1ProviderManifest)
				writeTestFile(t, root, directory+"trigger.yaml", "event_name: {literal: inbound.team}\n")
			}
			_, err := AdmitDirectory(root)
			if err == nil || !strings.Contains(err.Error(), "contains nested provider") {
				t.Fatalf("provider acquired nested topology: %v", err)
			}
		})
	}
}

func TestS1ProviderBodyLookalikesRemainResources(t *testing.T) {
	for _, branch := range []string{"data", "docs", "tests"} {
		for _, body := range []string{"trigger.yaml", "connector.yaml", "channel.yaml"} {
			t.Run(fmt.Sprintf("%s/%s", branch, body), func(t *testing.T) {
				root := t.TempDir()
				writeTestFile(t, root, filepath.Join(branch, body), "provider: inert\n")
				writeTestFile(t, root, filepath.Join(branch, "manifest.yaml"), "type: inert\n")
				artifact, err := AdmitDirectory(root)
				if err != nil {
					t.Fatal(err)
				}
				if len(artifact.Root().Children()) != 0 {
					t.Fatal("resource lookalike acquired a flow")
				}
				if _, ok := artifact.Manifest(branch + "/manifest.yaml"); ok {
					t.Fatal("resource lookalike acquired manifest authority")
				}
			})
		}
	}
}
