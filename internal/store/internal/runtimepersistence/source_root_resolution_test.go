package runtimepersistence

import (
	"context"
	"errors"
	"testing"

	"github.com/division-sh/swarm/internal/sourceartifact"
)

func TestS1UnboundHashRootAmbiguityBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			selected := selectedRootAdmissionStore(t, backend)
			resolver, present := any(selected).(interface {
				ResolveUnboundRootedSource(context.Context, string) (sourceartifact.RootedSource, error)
			})
			if !present {
				t.Fatal("selected source owner has no bounded unbound-root resolution")
			}
			ctx := context.Background()
			sourceA, _ := admittedRootForStorage(t, "shop-a")
			sourceB, _ := admittedRootForStorage(t, "shop-b")
			hash := sourceA.Admission().BundleHash()
			if sourceB.Admission().BundleHash() != hash {
				t.Fatal("fixture content differs")
			}
			if _, err := selected.EnsureSourceArtifact(ctx, sourceA.Artifact()); err != nil {
				t.Fatal(err)
			}
			if _, err := resolver.ResolveUnboundRootedSource(ctx, hash); !errors.Is(err, sourceartifact.ErrRootAdmissionMissing) {
				t.Fatalf("content without any root became executable evidence: %v", err)
			}
			if _, err := selected.EnsureRootedSource(ctx, sourceA); err != nil {
				t.Fatal(err)
			}
			frozen, err := resolver.ResolveUnboundRootedSource(ctx, hash)
			if err != nil || !frozen.Admission().Equal(sourceA.Admission()) {
				t.Fatalf("sole root failed resolution: %+v, %v", frozen, err)
			}
			if _, err := selected.EnsureRootedSource(ctx, sourceB); err != nil {
				t.Fatal(err)
			}
			if _, err := resolver.ResolveUnboundRootedSource(ctx, hash); !errors.Is(err, sourceartifact.ErrRootAdmissionAmbiguous) {
				t.Fatalf("ambiguous root acquired authority or was reported missing: %v", err)
			}
			if !frozen.Admission().Equal(sourceA.Admission()) {
				t.Fatal("second admission changed the already frozen resolution")
			}
			for _, source := range []sourceartifact.RootedSource{sourceA, sourceB} {
				loaded, err := selected.GetRootedSource(ctx, source.Admission().Reference())
				if err != nil || !loaded.Admission().Equal(source.Admission()) {
					t.Fatalf("ambiguity affected exact retained readback: %+v, %v", loaded, err)
				}
			}
			if _, err := selected.GetSourceArtifact(ctx, hash); err != nil {
				t.Fatalf("root ambiguity affected content-only read: %v", err)
			}
			canceled, cancel := context.WithCancel(ctx)
			cancel()
			if _, err := resolver.ResolveUnboundRootedSource(canceled, hash); !errors.Is(err, context.Canceled) {
				t.Fatalf("canceled resolution disclosed evidence: %v", err)
			}
			if _, err := resolver.ResolveUnboundRootedSource(ctx, "not-a-hash"); err == nil {
				t.Fatal("invalid hash gained root evidence")
			}
		})
	}
}
