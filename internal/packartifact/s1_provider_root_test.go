package packartifact

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

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
