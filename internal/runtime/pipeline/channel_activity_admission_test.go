package pipeline

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/channelonboarding"
	runtimechannelactivation "github.com/division-sh/swarm/internal/runtime/channelactivation"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
)

func TestPrivateChannelActivityRequiresExactCredentialAdmission(t *testing.T) {
	for _, change := range []string{"healthy", "deleted", "rotated", "same_value_new_receipt", "absent_and_invalid", "changed_after_prepare", "released_lease"} {
		t.Run(change, func(t *testing.T) {
			ctx := context.Background()
			file, err := runtimecredentials.NewFileStore(filepath.Join(t.TempDir(), "credentials.json"))
			if err != nil {
				t.Fatal(err)
			}
			owner, err := runtimecredentials.NewSnapshotOwner(file)
			if err != nil {
				t.Fatal(err)
			}
			admissions := []channelonboarding.CredentialAdmission{}
			for _, role := range []string{"provider", "signing"} {
				if _, err := file.AdmitWithReceipt(ctx, role, role+"-value", role+"-receipt"); err != nil {
					t.Fatal(err)
				}
				evidence, err := owner.SealCurrentValue(ctx, role)
				if err != nil {
					t.Fatal(err)
				}
				admissions = append(admissions, channelonboarding.CredentialAdmission{Role: role, StoreKey: role, ValueSeal: evidence.Seal, Receipt: role + "-receipt", Kind: channelonboarding.CredentialAdmissionWritten})
			}
			publication, err := channelonboarding.NewChannelActivationPublication(nil)
			if err != nil {
				t.Fatal(err)
			}
			activationOwner, err := runtimechannelactivation.NewOwner(publication)
			if err != nil {
				t.Fatal(err)
			}
			lease, _ := activationOwner.AcquirePresentation()
			defer lease.Release()
			target := ChannelActivityTarget{value: &channelActivityTargetValue{credentialKeys: map[string]string{"provider": "provider", "signing": "signing"}, admissions: admissions, lease: lease}}
			switch change {
			case "deleted", "absent_and_invalid":
				err = file.Delete(ctx, "provider")
			case "rotated":
				err = file.Set(ctx, "provider", "unadmitted")
			case "same_value_new_receipt":
				_, err = file.AdmitWithReceipt(ctx, "provider", "provider-value", "foreign-receipt")
			case "released_lease":
				lease.Release()
			}
			if err != nil {
				t.Fatal(err)
			}
			if change == "absent_and_invalid" {
				if err := file.Set(ctx, "signing", " \t"); err != nil {
					t.Fatal(err)
				}
			}
			values, _, err := target.resolveAdmittedCredentials(ctx, file, []string{"provider"})
			if change == "absent_and_invalid" {
				if !errors.Is(err, runtimecredentials.ErrCredentialValueUnusable) {
					t.Fatalf("missing role masked invalid signing: %v", err)
				}
				return
			}
			if change != "healthy" && change != "changed_after_prepare" {
				if err == nil {
					t.Fatal("stale or released authority executed")
				}
				return
			}
			if err != nil || values["provider"] != "provider-value" {
				t.Fatalf("exact credential execution = %v/%v", values, err)
			}
			if change == "healthy" {
				if err := target.validateAdmission(ctx); err != nil {
					t.Fatal(err)
				}
				return
			}
			if _, err := file.AdmitWithReceipt(ctx, "provider", "provider-value", "late-receipt"); err != nil {
				t.Fatal(err)
			}
			calls := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { calls++; w.Write([]byte(`{}`)) }))
			defer server.Close()
			_, err = executePreparedActivityHTTPTool(ctx, preparedActivityHTTPTool{method: http.MethodPost, url: server.URL, client: server.Client(), timeout: time.Second, validateAdmission: target.validateAdmission})
			if err == nil || calls != 0 {
				t.Fatalf("late credential replacement launched HTTP: %d/%v", calls, err)
			}
		})
	}
}
