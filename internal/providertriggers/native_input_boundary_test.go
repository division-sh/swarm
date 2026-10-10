package providertriggers_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// The reviewer's external raw-reader path no longer has a production API.
// These are compiler negatives, not a reader that merely promises to fail.
func TestReviewerExternalReaderCannotMintNativeAuthority(t *testing.T) {
	_, here, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate native-input boundary proof")
	}
	for _, probe := range []struct{ name, source, diagnostic string }{
		{"retired_reader", `package main
import onboarding "github.com/division-sh/swarm/internal/channelonboarding"
func main() { _ = onboarding.NewSessionInputOwner; var _ onboarding.NativeSessionInputReader }
`, "undefined: onboarding.NewSessionInputOwner"},
		{"internal_constructor", `package main
import "github.com/division-sh/swarm/internal/sessionprovider/internal/inputfact"
func main() { _ = inputfact.SealOwnedCapture(inputfact.Capture{Body: []byte("never received from SDK")}) }
`, "use of internal package"},
		{"facade_constructor", `package main
import "github.com/division-sh/swarm/internal/sessionprovider/input"
func main() { _ = input.SealOwnedCapture }
`, "undefined: input.SealOwnedCapture"},
		{"facade_fields", `package main
import "github.com/division-sh/swarm/internal/sessionprovider/input"
func main() { _ = input.Admission{value: nil} }
`, "unexported field value"},
		{"account_internal_constructor", `package main
import "github.com/division-sh/swarm/internal/sessionprovider/internal/authorityfact"
func main() { _ = authorityfact.SealOwnedAccount }
`, "use of internal package"},
		{"account_facade_constructor", `package main
import "github.com/division-sh/swarm/internal/sessionprovider/authority"
func main() { _ = authority.SealOwnedAccount }
`, "undefined: authority.SealOwnedAccount"},
		{"claim_facade_fields", `package main
import "github.com/division-sh/swarm/internal/sessionprovider/authority"
func main() { _ = authority.Claim{value: nil} }
`, "unexported field value"},
	} {
		t.Run(probe.name, func(t *testing.T) {
			directory, err := os.MkdirTemp(filepath.Dir(here), ".native-issuance-probe-")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := os.RemoveAll(directory); err != nil {
					t.Error(err)
				}
			})
			if err := os.WriteFile(filepath.Join(directory, "main.go"), []byte(probe.source), 0o600); err != nil {
				t.Fatal(err)
			}
			command := exec.Command("go", "build", "-o", filepath.Join(t.TempDir(), "forger"), directory)
			output, err := command.CombinedOutput()
			if err == nil {
				t.Fatal("external package manufactured native input authority")
			}
			if !strings.Contains(string(output), probe.diagnostic) {
				t.Fatalf("unexpected compiler refusal: %s", output)
			}
		})
	}
}
