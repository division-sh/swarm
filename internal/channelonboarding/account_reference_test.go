package channelonboarding

import (
	"slices"
	"testing"

	"github.com/division-sh/swarm/internal/channelprovider"
)

func TestAccountReferenceCanonicalIdentity(t *testing.T) {
	var installed []string
	for _, binding := range channelprovider.Installed() {
		installed = append(installed, binding.Provider())
		got, found := channelprovider.Lookup(binding.Provider())
		if !found || got.Provider() != binding.Provider() || binding.MatchesDeployment(binding.DeploymentPosture(), "token", "token") {
			t.Fatal("compiled binding lost its identity or admitted a shared signing/provider role")
		}
		if binding.Session() && (!binding.NativeWrite("whatsapp.send_text") || binding.NativeWrite("foreign.send_text") || binding.NativeRead("whatsapp.send_text")) {
			t.Fatal("native binding gained a foreign or wrong-kind target")
		}
		if binding.PollsClaim() != (binding.Provider() == "telegram") ||
			binding.AcceptsCaptureKind("message") != (binding.Provider() == "whatsapp") || binding.AcceptsCaptureKind("foreign") {
			t.Fatal("compiled intake or capture codec changed provider ownership")
		}
	}
	if !slices.Equal(installed, []string{"telegram", "whatsapp"}) || channelprovider.Session("unknown") {
		t.Fatalf("compiled installation changed: %v", installed)
	}
	for _, cell := range []struct{ provider, account, want string }{
		{"whatsapp", "+1 (555) 010-0123", "+15550100123"},
		{"whatsapp", "+15550100124", "+15550100124"},
		{"telegram", "123456789", "123456789"},
	} {
		got, err := CanonicalAccount(cell.provider, cell.account)
		if err != nil || got.Account != cell.want || got.Provider != cell.provider || got.Validate() != nil {
			t.Fatalf("canonical account %q: %+v, %v", cell.provider, got, err)
		}
	}
	for _, cell := range []struct{ provider, account string }{
		{"whatsapp", "15550100123"}, {"whatsapp", "+0123"}, {"whatsapp", "+1"},
		{"whatsapp", "+1234567890123456"}, {"whatsapp", "+123:4"}, {"whatsapp", "+123@server"},
		{"whatsapp", "+123\n456"}, {"whatsapp", "+123/456"},
		{"telegram", "00123"}, {"telegram", "-123"}, {"telegram", "0"}, {"telegram", "@bot"},
		{"telegram", " 123"}, {"WHATSAPP", "+123"}, {" whatsapp", "+123"}, {"other", "+123"},
	} {
		if _, err := CanonicalAccount(cell.provider, cell.account); err == nil {
			t.Fatalf("invalid provider/account accepted: %+v", cell)
		}
	}
	if (AccountReference{Provider: "whatsapp", Account: "+1 234"}).Validate() == nil {
		t.Fatal("noncanonical persisted identity was accepted")
	}
}
