package cliapp

import (
	"bytes"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/operatorchannel"
)

func TestWhatsAppClaimInstructionsCarryExactDestinationCodeAndExpiry(t *testing.T) {
	expires := time.Date(2026, 11, 6, 22, 42, 17, 123000, time.UTC)
	displayed, err := time.Parse("2006-01-02 15:04:05 MST (-07:00)", channelClaimDeadline(expires))
	if err != nil || displayed.After(expires) || expires.Sub(displayed) >= time.Second {
		t.Fatal("human deadline rounded later than its stored expiry", displayed, expires, err)
	}
	for _, registered := range []bool{false, true} {
		result := channelOnboardingResult{
			Operation:         channelonboarding.Operation{Provider: "whatsapp"},
			IdentityOperation: &operatorchannel.Operation{Challenge: "00007", ExpiresAt: expires},
			Connection:        &channelonboarding.SessionConnectionReadback{AccountPresentation: "+15551234567"},
		}
		if registered {
			result.Operation.AccountReference = "+15551234567"
			result.Connection.AccountPresentation = "not the destination"
		}
		var out bytes.Buffer
		if err := writeChannelClaimInstructions(&out, result); err != nil {
			t.Fatal(err)
		}
		text := out.String()
		for _, want := range []string{"FROM your operator phone TO +15551234567", "Send the code 00007", expires.Local().Format("2006-01-02 15:04:05 MST (-07:00)"), "SEND-CODE LINK (not a device-link QR): "} {
			if !strings.Contains(text, want) {
				t.Fatalf("missing %q in %q", want, text)
			}
		}
		link := strings.Fields(strings.Split(text, "SEND-CODE LINK (not a device-link QR): ")[1])[0]
		parsed, err := url.Parse(link)
		if err != nil || parsed.Scheme != "https" || parsed.Host != "wa.me" || parsed.Path != "/15551234567" || parsed.Query().Get("text") != "00007" || len(parsed.Query()) != 1 {
			t.Fatalf("send-code link changes destination/code: %s %v", link, err)
		}
		if strings.Contains(text, "not the destination") || strings.Contains(text, "countdown") || strings.Contains(text, "operator number") {
			t.Fatal("instructions invented a lookup or another clock", text)
		}
	}
}

func TestTelegramClaimInstructionsCarryAuthenticatedBotCodeAndExpiry(t *testing.T) {
	expires := time.Date(2026, 11, 6, 22, 42, 17, 987654321, time.UTC)
	result := channelOnboardingResult{
		Operation:         channelonboarding.Operation{Provider: "telegram", DeploymentRegistration: true, AccountReference: "420079"},
		IdentityOperation: &operatorchannel.Operation{Challenge: "00007", ExpiresAt: expires},
		Connection:        &channelonboarding.SessionConnectionReadback{AccountPresentation: "420079", AccountName: "SwarmTestBot"},
	}
	var out bytes.Buffer
	if err := writeTelegramClaimInstructions(&out, result); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"Send the code 00007 to @SwarmTestBot", channelClaimDeadline(expires), "SEND-CODE LINK: https://t.me/SwarmTestBot?text=00007"} {
		if !strings.Contains(out.String(), want) {
			t.Fatalf("missing %q in %q", want, out.String())
		}
	}
	if strings.Contains(out.String(), "987654321") || result.IdentityOperation.ExpiresAt != expires {
		t.Fatal("display changed stored precision or printed fractional seconds")
	}
	for _, mutate := range []func(*channelOnboardingResult){
		func(r *channelOnboardingResult) { r.Connection = nil },
		func(r *channelOnboardingResult) { r.IdentityOperation = nil },
		func(r *channelOnboardingResult) { r.Connection.AccountName = "" },
		func(r *channelOnboardingResult) { r.Connection.AccountName = "@masked***" },
		func(r *channelOnboardingResult) { r.Connection.AccountPresentation = "420080" },
	} {
		copy := result
		connection := *result.Connection
		copy.Connection = &connection
		mutate(&copy)
		out.Reset()
		if err := writeTelegramClaimInstructions(&out, copy); err == nil || out.Len() != 0 {
			t.Fatal("instructions inferred a bot destination from incomplete/foreign evidence")
		}
	}
}

func TestWhatsAppClaimInstructionsRefuseMissingOrMaskedDestination(t *testing.T) {
	for _, account := range []string{"", "+1555***4567", "@operator", "1555@s.whatsapp.net", "+1555\n678"} {
		var out bytes.Buffer
		result := channelOnboardingResult{Operation: channelonboarding.Operation{Provider: "whatsapp", AccountReference: account},
			IdentityOperation: &operatorchannel.Operation{Challenge: "12345", ExpiresAt: time.Now().Add(time.Minute)}}
		if err := writeChannelClaimInstructions(&out, result); err == nil || out.Len() != 0 {
			t.Fatal("instructions guessed a destination", account, out.String(), err)
		}
	}
}

func TestWhatsAppReadyTeachingUsesRuledCommands(t *testing.T) {
	var out bytes.Buffer
	writeWhatsAppConnection(&out, nil, true)
	const want = "WhatsApp ready. Run `swarm serve` to answer customers. Decisions: answer them in the terminal with `swarm mailbox decide`.\n"
	if out.String() != want {
		t.Fatalf("readiness teaching=%q", out.String())
	}
}
