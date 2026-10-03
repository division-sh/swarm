package serveapp

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/packs"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
)

func TestChannelProviderConsumersRequireExactAdmission(t *testing.T) {
	for _, operation := range []string{"deliver", "acknowledge_interaction", "install_inbox_entry", "read_inbox_entry", "identify_inbox_address"} {
		for _, change := range []string{"healthy", "provider_absent", "signing_absent", "rotated", "same_value_new_receipt", "absent_and_invalid"} {
			t.Run(operation+"/"+change, func(t *testing.T) {
				ctx := context.Background()
				file, err := runtimecredentials.NewFileStore(filepath.Join(t.TempDir(), "credentials.json"))
				if err != nil {
					t.Fatal(err)
				}
				owner, err := runtimecredentials.NewSnapshotOwner(file)
				if err != nil {
					t.Fatal(err)
				}
				keys := map[string]string{"telegram_bot_token": "provider", "webhook_signing_secret": "signing"}
				binding, err := packs.NewOutboundBindingPlanWithCredentials("telegram", loadSupportedTelegramChannelPlan(t), "42", nil, keys)
				if err != nil {
					t.Fatal(err)
				}
				_, tool, err := binding.ConnectorOperation(operation)
				if err != nil {
					t.Fatal(err)
				}
				admissions := []channelonboarding.CredentialAdmission{}
				for _, role := range []string{"telegram_bot_token", "webhook_signing_secret"} {
					key := keys[role]
					if _, err := file.AdmitWithReceipt(ctx, key, key+"-value", key+"-receipt"); err != nil {
						t.Fatal(err)
					}
					evidence, err := owner.SealCurrentValue(ctx, key)
					if err != nil {
						t.Fatal(err)
					}
					admissions = append(admissions, channelonboarding.CredentialAdmission{Role: role, StoreKey: key, Kind: channelonboarding.CredentialAdmissionWritten, Receipt: key + "-receipt", ValueSeal: evidence.Seal})
				}
				values, err := resolveChannelDeliveryCredentials(ctx, owner, binding, admissions, tool)
				if err != nil || values["telegram_bot_token"] != "provider-value" {
					t.Fatalf("healthy execution = %v/%v", values, err)
				}
				switch change {
				case "provider_absent", "absent_and_invalid":
					err = file.Delete(ctx, "provider")
				case "signing_absent":
					err = file.Delete(ctx, "signing")
				case "rotated":
					err = file.Set(ctx, "provider", "unadmitted")
				case "same_value_new_receipt":
					_, err = file.AdmitWithReceipt(ctx, "provider", "provider-value", "foreign-receipt")
				}
				if err != nil {
					t.Fatal(err)
				}
				if change == "absent_and_invalid" {
					if err := file.Set(ctx, "signing", " \t"); err != nil {
						t.Fatal(err)
					}
				}
				err = channelCredentialHTTPExecutor(nil, owner, binding, admissions, tool).Preflight(ctx)
				if change == "healthy" {
					if err != nil {
						t.Fatal(err)
					}
					return
				}
				if err == nil {
					t.Fatal("provider consumer adopted a replaced or missing credential")
				}
				if change == "absent_and_invalid" && !errors.Is(err, runtimecredentials.ErrCredentialValueUnusable) {
					t.Fatalf("absence masked invalid sibling: %v", err)
				}
			})
		}
	}
}
