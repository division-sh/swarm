package runtimepersistence

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/division-sh/swarm/internal/packartifact"
	"github.com/division-sh/swarm/internal/packmodel"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
)

func TestS1ProviderBodyRetainedKeyBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			selected := selectedRootAdmissionStore(t, backend)
			ctx := context.Background()
			var sources []sourceartifact.RootedSource
			var bodyHash string
			for _, key := range []string{"team-bot", "team_bot"} {
				content, err := sourceartifact.AdmitMembers(map[string][]byte{
					"manifest.yaml": []byte("version: 1.0.0\nplatform_version: '*'\ncapabilities: {can: {emit_events: [raw]}, cannot: [execute code]}\n"),
					"trigger.yaml":  []byte("event_name: {literal: raw}\n"),
				})
				if err != nil {
					t.Fatal(err)
				}
				source, root := sourceartifactfixture.Rooted(t, key, content)
				if len(sources) != 0 && sources[0].Artifact().BundleHash() != source.Artifact().BundleHash() {
					t.Fatal("directory key contaminated content identity")
				}
				if _, err := selected.EnsureSourceArtifact(ctx, source.Artifact()); err != nil {
					t.Fatal(err)
				}
				if _, err := selected.GetRootedSource(ctx, source.Admission().Reference()); !errors.Is(err, sourceartifact.ErrRootAdmissionMissing) {
					t.Fatalf("content ensure minted provider root: %v", err)
				}
				if _, err := selected.EnsureRootedSource(ctx, source); err != nil {
					t.Fatal(err)
				}
				sources = append(sources, source)
				if err := os.RemoveAll(root); err != nil {
					t.Fatal(err)
				}
			}
			for _, source := range sources {
				loaded, err := selected.GetRootedSource(ctx, source.Admission().Reference())
				if err != nil {
					t.Fatal(err)
				}
				provider, err := packartifact.ProviderFromRootedSource(loaded, ".")
				if err != nil || !provider.Valid() || provider.Key() != source.Admission().RootDirectory() {
					t.Fatalf("retained provider selected a different root: key=%q err=%v", provider.Key(), err)
				}
				body, present := provider.Body(packmodel.BodyTrigger)
				if !present || body.ProviderKey() != provider.Key() || body.Hash() == "" || bodyHash != "" && body.Hash() != bodyHash {
					t.Fatalf("provider BODY lost exact root/content separation: key=%q hash=%q", body.ProviderKey(), body.Hash())
				}
				bodyHash = body.Hash()
				if unrooted, err := packartifact.ProviderFromArtifact(loaded.Artifact(), "."); !errors.Is(err, sourceartifact.ErrRootAdmissionMissing) || unrooted.Valid() {
					t.Fatalf("shared decoded content borrowed the retained root: valid=%t err=%v", unrooted.Valid(), err)
				}
			}
			missing := sources[0].Admission().Reference()
			missing.RootDirectory = "unadmitted"
			if _, err := selected.GetRootedSource(ctx, missing); !errors.Is(err, sourceartifact.ErrRootAdmissionMissing) {
				t.Fatalf("provider spelling alone minted retained evidence: %v", err)
			}
		})
	}
}
