package packadmission

import (
	"bytes"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/packmodel"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/division-sh/swarm/internal/testutil/packfixture"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
)

func scopedProviderMembers() map[string][]byte {
	members := map[string][]byte{
		"schema.yaml":            []byte("stages: []\n"),
		"child/schema.yaml":      []byte("stages: []\n"),
		"child/deep/schema.yaml": []byte("stages: []\n"),
		"sibling/schema.yaml":    []byte("stages: []\n"),
	}
	for label, body := range packfixture.NeutralProviderMembers() {
		members["team-bot/"+label] = body
		members["team_bot/"+label] = append([]byte(nil), body...)
	}
	members["child/team-bot/manifest.yaml"] = []byte("version: 2.0.0\nplatform_version: '*'\ncapabilities: {can: {call_provider_actions: [send_message]}, cannot: [bypass journal]}\n")
	members["child/team-bot/connector.yaml"] = bytes.Replace(members["team-bot/connector.yaml"], []byte("example.test"), []byte("child.example.test"), 1)
	members["sibling/team-bot/manifest.yaml"] = []byte("version: 1.0.0\nplatform_version: '*'\ncapabilities: {can: {emit_events: [raw, text_message]}, cannot: [bypass journal]}\n")
	members["sibling/team-bot/trigger.yaml"] = append([]byte(nil), members["team-bot/trigger.yaml"]...)
	return members
}

func capturedProviderSource(t *testing.T, members map[string][]byte) (sourceartifact.RootedSource, string) {
	t.Helper()
	artifact, err := sourceartifact.AdmitMembers(members)
	if err != nil {
		t.Fatal(err)
	}
	return sourceartifactfixture.Rooted(t, "provider-shop", artifact)
}

func TestS1ProviderSourceFreezesNearestWholeDirectories(t *testing.T) {
	rooted, _ := capturedProviderSource(t, scopedProviderMembers())
	source, err := AdmitProviderSource(rooted, providerDefinitionPlatform(t))
	if err != nil || !source.Valid() || !source.RootedSource().Admission().Equal(rooted.Admission()) {
		t.Fatalf("admit exact provider source: %v", err)
	}
	for _, cell := range []struct {
		flow, directory, version    string
		trigger, connector, channel bool
		held                        int
	}{
		{".", "team-bot", "1.0.0", true, true, true, 2},
		{"child", "child/team-bot", "2.0.0", false, true, false, 1},
		{"child/deep", "child/team-bot", "2.0.0", false, true, false, 0},
		{"sibling", "sibling/team-bot", "1.0.0", true, false, false, 1},
	} {
		t.Run(cell.flow, func(t *testing.T) {
			scope, err := source.Scope(cell.flow)
			if err != nil || scope.FlowPath() != cell.flow || len(scope.Providers()) != 2 || len(scope.HeldProviders()) != cell.held {
				t.Fatalf("scope membership differs: %+v %v", scope, err)
			}
			definition, present := scope.Provider("team-bot")
			if !present || definition.Provider().Directory() != cell.directory || definition.Provider().Metadata().Version != cell.version {
				t.Fatal("whole-directory selection lost exact source or version")
			}
			_, trigger := definition.TriggerCatalog()
			connector, hasConnector := definition.ConnectorDescriptor()
			_, channel := definition.ChannelPlan()
			if trigger != cell.trigger || hasConnector != cell.connector || channel != cell.channel {
				t.Fatal("scope borrowed a missing role from an ancestor or sibling")
			}
			if hasConnector && connector.Identity.Source().Path() != cell.directory+"/connector.yaml" {
				t.Fatal("connector source differs from whole-directory selection")
			}
			other, present := scope.Provider("team_bot")
			if !present || other.Provider().Directory() != "team_bot" {
				t.Fatal("hyphen and underscore keys were collapsed")
			}
			if definition.Provider().Key() == other.Provider().Key() {
				t.Fatal("distinct provider namespaces became aliases")
			}
			providers := scope.Providers()
			providers[0] = ProviderDefinition{}
			if !scope.Providers()[0].Valid() {
				t.Fatal("readback slice mutated frozen scope selection")
			}
			for _, key := range []string{" team-bot", "team-bot ", "child/team-bot", "team", "TEAM-BOT"} {
				if invalid, present := scope.Provider(key); present || invalid.Valid() {
					t.Fatalf("scope repaired foreign key %q", key)
				}
			}
		})
	}
	for _, flow := range []string{"", "child/", "child/../sibling", "./child", " child", "missing", "team-bot"} {
		if scope, err := source.Scope(flow); err == nil || len(scope.Providers()) != 0 || scope.FlowPath() != "" {
			t.Fatalf("scope repaired unknown/noncanonical flow %q: %v", flow, err)
		}
	}
}

func TestS1ProviderSourceRetainsFrozenSelectionAfterReplacementAndRemoval(t *testing.T) {
	rooted, directory := capturedProviderSource(t, scopedProviderMembers())
	platform := providerDefinitionPlatform(t)
	before, err := AdmitProviderSource(rooted, platform)
	if err != nil {
		t.Fatal(err)
	}
	row, err := sourceartifact.PersistedFromArtifact(rooted.Artifact(), time.Unix(1, 0))
	if err != nil {
		t.Fatal(err)
	}
	retained, err := sourceartifact.RestoreRootedSource(row, rooted.Admission().Reference())
	if err != nil {
		t.Fatal(err)
	}
	if _, present := retained.Artifact().RootAdmission(); present {
		t.Fatal("retained view mutated content-only root authority")
	}
	if err := os.WriteFile(directory+"/child/team-bot/connector.yaml", []byte("invalid: true\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(directory); err != nil {
		t.Fatal(err)
	}
	for _, selected := range []sourceartifact.RootedSource{rooted, retained} {
		after, err := AdmitProviderSource(selected, platform)
		if err != nil || !after.RootedSource().Admission().Equal(before.RootedSource().Admission()) {
			t.Fatalf("frozen source was rescanned or replaced: %v", err)
		}
		for _, flow := range []string{".", "child", "child/deep", "sibling"} {
			original, _ := before.Scope(flow)
			restored, err := after.Scope(flow)
			if err != nil {
				t.Fatal(err)
			}
			for _, definition := range original.Providers() {
				other, present := restored.Provider(definition.Provider().Key())
				if !present || other.Provider().Directory() != definition.Provider().Directory() {
					t.Fatal("retained source selected different provider directory")
				}
				for _, role := range []packmodel.BodyRole{packmodel.BodyTrigger, packmodel.BodyConnector, packmodel.BodyChannel} {
					body, hasBody := definition.Provider().Body(role)
					copy, hasCopy := other.Provider().Body(role)
					if hasBody != hasCopy || (hasBody && (body.Hash() != copy.Hash() || !bytes.Equal(body.Bytes(), copy.Bytes()))) {
						t.Fatal("retained provider changed exact role bytes")
					}
				}
			}
		}
	}
}

func TestS1ProviderSourceDoesNotPromoteVisibilityToHolderOwnership(t *testing.T) {
	rooted, _ := capturedProviderSource(t, scopedProviderMembers())
	source, err := AdmitProviderSource(rooted, providerDefinitionPlatform(t))
	if err != nil {
		t.Fatal(err)
	}
	deep, _ := source.Scope("child/deep")
	if len(deep.Providers()) != 2 || len(deep.HeldProviders()) != 0 {
		t.Fatal("inherited visibility fabricated holder membership")
	}
	root, _ := source.Scope(".")
	for _, definition := range root.HeldProviders() {
		if definition.Provider().DeclaringFlow() != "." {
			t.Fatal("root adopted child declaration")
		}
		plan, present := definition.ChannelPlan()
		if !present || plan.RequireExecutableProvider() == nil {
			t.Fatal("source membership fabricated an installed session")
		}
	}
	if scope, err := (ProviderSource{}).Scope("."); err == nil || len(scope.Providers()) != 0 {
		t.Fatal("zero source exposed a provider scope")
	}
}

func TestS1ProviderSourceRefusesPartialAdmissionAndStandaloneProvider(t *testing.T) {
	platform := providerDefinitionPlatform(t)
	for _, cell := range []struct{ label, old, replacement string }{
		{"sibling/team-bot/trigger.yaml", "event_name:", "unknown:"},
		{"child/team-bot/connector.yaml", "handler_type: http", "handler_type: unknown"},
		{"team-bot/channel.yaml", "tool: send_message", "tool: missing"},
		{"child/team-bot/manifest.yaml", "platform_version: '*'", "platform_version: '>=99.0.0'"},
	} {
		t.Run(cell.label, func(t *testing.T) {
			members := scopedProviderMembers()
			members[cell.label] = bytes.Replace(members[cell.label], []byte(cell.old), []byte(cell.replacement), 1)
			rooted, _ := capturedProviderSource(t, members)
			source, err := AdmitProviderSource(rooted, platform)
			if err == nil || source.Valid() || source.RootedSource().Validate() == nil {
				t.Fatalf("failed role leaked a source projection: %v", err)
			}
			if scope, err := source.Scope("."); err == nil || len(scope.Providers()) != 0 {
				t.Fatal("failed source exposed partial definitions")
			}
		})
	}
	provider, _ := capturedProviderSource(t, packfixture.NeutralProviderMembers())
	if result, err := AdmitProviderSource(provider, platform); err == nil || result.Valid() || !strings.Contains(err.Error(), "standalone provider") {
		t.Fatalf("standalone provider became an executable flow scope: %v", err)
	}
	if result, err := AdmitProviderSource(sourceartifact.RootedSource{}, platform); err == nil || result.Valid() {
		t.Fatal("zero root became a provider source")
	}
}

func TestS1ProviderSourceKeepsEqualContentRootsDistinct(t *testing.T) {
	artifact, err := sourceartifact.AdmitMembers(scopedProviderMembers())
	if err != nil {
		t.Fatal(err)
	}
	first, _ := sourceartifactfixture.Rooted(t, "first-shop", artifact)
	second, _ := sourceartifactfixture.Rooted(t, "second-shop", artifact)
	if first.Admission().BundleHash() != second.Admission().BundleHash() || first.Admission().Equal(second.Admission()) {
		t.Fatal("fixture did not establish equal content with distinct genuine roots")
	}
	platform := providerDefinitionPlatform(t)
	for _, rooted := range []sourceartifact.RootedSource{first, second} {
		source, err := AdmitProviderSource(rooted, platform)
		if err != nil || !source.RootedSource().Admission().Equal(rooted.Admission()) {
			t.Fatalf("provider source borrowed the other equal-content root: %v", err)
		}
		if _, present := source.RootedSource().Artifact().RootAdmission(); !present {
			t.Fatal("genuine filesystem source lost captured root evidence")
		}
	}
	decoded, err := sourceartifact.DecodeLogical(artifact.LogicalBlob())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := decoded.RootedSource(); err == nil {
		t.Fatal("content-only decoding acquired executable scope authority")
	}
}

func TestS1ProviderSourceEmptyMembershipStillRequiresValidPlatform(t *testing.T) {
	rooted, _ := capturedProviderSource(t, map[string][]byte{"schema.yaml": []byte("stages: []\n")})
	platform := providerDefinitionPlatform(t)
	source, err := AdmitProviderSource(rooted, platform)
	if err != nil || !source.Valid() {
		t.Fatalf("empty provider membership should be a valid empty scope: %v", err)
	}
	scope, err := source.Scope(".")
	if err != nil || scope.FlowPath() != "." || len(scope.Providers()) != 0 || len(scope.HeldProviders()) != 0 {
		t.Fatal("empty source fabricated a provider or lost its exact flow")
	}
	for _, version := range []string{"", " ", "invalid"} {
		invalid := platform
		invalid.Platform.Version = version
		if source, err := AdmitProviderSource(rooted, invalid); err == nil || source.Valid() {
			t.Fatalf("empty membership bypassed platform admission for %q", version)
		}
	}
}

func TestS1ProviderSourceConcurrentReadbackCannotReplaceEvidence(t *testing.T) {
	rooted, _ := capturedProviderSource(t, scopedProviderMembers())
	source, err := AdmitProviderSource(rooted, providerDefinitionPlatform(t))
	if err != nil {
		t.Fatal(err)
	}
	var workers sync.WaitGroup
	errors := make(chan error, 8)
	for index := 0; index < 8; index++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for _, flow := range []string{".", "child", "child/deep", "sibling"} {
				scope, err := source.Scope(flow)
				if err != nil {
					errors <- err
					return
				}
				definition, present := scope.Provider("team-bot")
				if !present || !definition.Valid() {
					errors <- fmt.Errorf("scope %s lost its provider", flow)
					return
				}
				if descriptor, present := definition.ConnectorDescriptor(); present {
					delete(descriptor.Tools, "team-bot.send_message")
					again, _ := definition.ConnectorDescriptor()
					if _, present := again.Tools["team-bot.send_message"]; !present {
						errors <- fmt.Errorf("scope %s accepted readback replacement", flow)
						return
					}
				}
				copy := scope.Providers()
				copy[0] = ProviderDefinition{}
			}
		}()
	}
	workers.Wait()
	close(errors)
	for err := range errors {
		t.Error(err)
	}
}
