package packartifact

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/packmodel"
	"github.com/division-sh/swarm/internal/sourceartifact"
)

func TestS1ProviderRootRequiresCapturedDirectoryIdentity(t *testing.T) {
	provider, err := AdmitProviderMembers(".", s1ProviderMembers())
	if !errors.Is(err, sourceartifact.ErrRootAdmissionMissing) || provider.Valid() {
		t.Fatalf("content-only provider acquired a directory identity: valid=%t err=%v", provider.Valid(), err)
	}
	root := filepath.Join(t.TempDir(), "team-bot")
	if err := os.Mkdir(root, 0o700); err != nil {
		t.Fatal(err)
	}
	for label, body := range s1ProviderMembers() {
		if err := os.WriteFile(filepath.Join(root, label), body, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	admitted, err := sourceartifact.AdmitDirectory(root)
	if err != nil {
		t.Fatal(err)
	}
	provider, err = ProviderFromArtifact(admitted, ".")
	if err != nil || !provider.Valid() {
		t.Fatalf("captured provider root refused: valid=%t err=%v", provider.Valid(), err)
	}
	decoded, err := sourceartifact.DecodeLogical(admitted.LogicalBlob())
	if err != nil {
		t.Fatal(err)
	}
	provider, err = ProviderFromArtifact(decoded, ".")
	if !errors.Is(err, sourceartifact.ErrRootAdmissionMissing) || provider.Valid() {
		t.Fatalf("logical decoding invented the root directory: valid=%t err=%v", provider.Valid(), err)
	}
}

func TestS1ProviderBodyCarriesFrozenRootKey(t *testing.T) {
	var firstHash string
	for _, key := range []string{"team-bot", "team_bot"} {
		root := filepath.Join(t.TempDir(), key)
		if err := os.Mkdir(root, 0o700); err != nil {
			t.Fatal(err)
		}
		members := s1ProviderMembers()
		members["connector.yaml"] = []byte("{}\n")
		members["channel.yaml"] = []byte("{}\n")
		for label, body := range members {
			if err := os.WriteFile(filepath.Join(root, label), body, 0o600); err != nil {
				t.Fatal(err)
			}
		}
		artifact, err := sourceartifact.AdmitDirectory(root)
		if err != nil {
			t.Fatal(err)
		}
		rooted, err := artifact.RootedSource()
		if err != nil {
			t.Fatal(err)
		}
		row, err := sourceartifact.PersistedFromArtifact(artifact, time.Now().UTC())
		if err != nil {
			t.Fatal(err)
		}
		retained, err := sourceartifact.RestoreRootedSource(row, rooted.Admission().Reference())
		if err != nil {
			t.Fatal(err)
		}
		captured, err := ProviderFromArtifact(artifact, ".")
		if err != nil {
			t.Fatal(err)
		}
		if err := os.RemoveAll(root); err != nil {
			t.Fatal(err)
		}
		for _, selected := range []sourceartifact.RootedSource{rooted, retained} {
			provider, err := ProviderFromRootedSource(selected, ".")
			if err != nil || !provider.Valid() || provider.Key() != key || captured.Key() != key {
				t.Fatalf("provider lost frozen root key: %q %q err=%v", provider.Key(), captured.Key(), err)
			}
			for _, role := range []packmodel.BodyRole{packmodel.BodyTrigger, packmodel.BodyConnector, packmodel.BodyChannel} {
				body, present := provider.Body(role)
				if !present || !body.Valid() || body.ProviderKey() != key {
					t.Fatalf("role %v lost directory ownership: present=%t key=%q", role, present, body.ProviderKey())
				}
				if role == packmodel.BodyTrigger {
					if firstHash != "" && body.Hash() != firstHash {
						t.Fatal("provider root key changed exact body hash")
					}
					firstHash = body.Hash()
				}
			}
		}
		if _, present := retained.Artifact().RootAdmission(); present {
			t.Fatal("provider view attached root authority to shared content")
		}
		if invalid, err := ProviderFromArtifact(retained.Artifact(), "."); !errors.Is(err, sourceartifact.ErrRootAdmissionMissing) || invalid.Valid() {
			t.Fatalf("content-only reader borrowed retained view's key: valid=%t err=%v", invalid.Valid(), err)
		}
	}
	if provider, err := ProviderFromRootedSource(sourceartifact.RootedSource{}, "."); err == nil || provider.Valid() || provider.Key() != "" {
		t.Fatalf("zero rooted source acquired provider view: valid=%t key=%q err=%v", provider.Valid(), provider.Key(), err)
	}
	if key := (ProviderBody{}).ProviderKey(); key != "" {
		t.Fatalf("zero body acquired provider key %q", key)
	}
}
