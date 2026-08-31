package credentials

import (
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCredentialValueCurrentnessHasOneProductionOwnerAndNoEpochSurvivors(t *testing.T) {
	repo := filepath.Clean(filepath.Join("..", "..", ".."))
	retirementZones := []string{
		"internal/runtime/credentials",
		"internal/channelonboarding",
		"internal/operatorchannel",
		"internal/runtime/publicingress",
		"internal/store/internal/backend/channelonboarding",
		"internal/store/internal/backend/operatorchannel",
	}
	for _, zone := range retirementZones {
		err := filepath.WalkDir(filepath.Join(repo, zone), func(path string, entry fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if entry.IsDir() || filepath.Ext(path) != ".go" || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			raw, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			for _, retired := range []string{"occurrenceEpoch", "CredentialEpoch", "credential_epoch", "occurrence_epoch", ".Epoch()"} {
				if strings.Contains(string(raw), retired) {
					t.Errorf("retired credential epoch interpreter %q remains in %s", retired, path)
				}
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	err := filepath.WalkDir(repo, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			if entry.Name() == ".git" {
				return filepath.SkipDir
			}
			return nil
		}
		if filepath.Ext(path) != ".go" || strings.HasSuffix(path, "_test.go") || strings.Contains(path, filepath.Join("internal", "runtime", "credentials")) {
			return nil
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		for _, ownerOnly := range []string{"credential-value-seal-v1:", "credentialValueSeal("} {
			if strings.Contains(string(raw), ownerOnly) {
				t.Errorf("credential value-seal construction escaped its owner in %s", path)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	for _, contractPath := range []string{
		"platform-spec.yaml",
		filepath.Join("internal", "store", "testdata", "persistence_authority_findings.tsv"),
	} {
		contract, err := os.ReadFile(filepath.Join(repo, contractPath))
		if err != nil {
			t.Fatal(err)
		}
		for _, retired := range []string{"credential epoch", "credential epochs", "occurrence epoch", "occurrence_epoch", "credentialepochscurrent"} {
			if strings.Contains(strings.ToLower(string(contract)), retired) {
				t.Errorf("retired credential epoch contract %q remains in %s", retired, contractPath)
			}
		}
	}
}

func TestPublicChannelConfirmationConsumesOnboardingCoordinator(t *testing.T) {
	repo := filepath.Clean(filepath.Join("..", "..", ".."))
	raw, err := os.ReadFile(filepath.Join(repo, "internal", "apiv1", "operator_channel.go"))
	if err != nil {
		t.Fatal(err)
	}
	source := string(raw)
	if !strings.Contains(source, "opts.Confirmation.ConfirmIdentity(") {
		t.Fatal("channel.confirm does not consume the onboarding confirmation coordinator")
	}
	for _, bypass := range []string{"opts.Channels.Confirm(", "opts.Channels.GetOperation("} {
		if strings.Contains(source, bypass) {
			t.Fatalf("public channel confirmation bypass %q is live", bypass)
		}
	}
}

func TestChannelConfirmationConsumesOneSealValidatedCredentialObservation(t *testing.T) {
	repo := filepath.Clean(filepath.Join("..", "..", ".."))
	raw, err := os.ReadFile(filepath.Join(repo, "internal", "serveapp", "channel_onboarding.go"))
	if err != nil {
		t.Fatal(err)
	}
	source := string(raw)
	if !strings.Contains(source, "d.credentials.ObserveValueMatchingSeal(") {
		t.Fatal("channel confirmation does not consume the atomic seal-validated observation owner")
	}
	if strings.Contains(source, "d.credentials.CurrentValueMatchesSeal(") || strings.Contains(source, "d.credentials.ObserveSecretBinding(") {
		t.Fatal("channel confirmation retains a split credential validation/read path")
	}
}
