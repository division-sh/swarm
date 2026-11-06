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
		if err := writeWhatsAppClaimInstructions(&out, result); err != nil {
			t.Fatal(err)
		}
		text := out.String()
		for _, want := range []string{"FROM your operator phone TO +15551234567", "Send the code 00007", expires.Local().Format(time.RFC3339Nano), "SEND-CODE LINK (not a device-link QR): "} {
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

func TestWhatsAppClaimInstructionsRefuseMissingOrMaskedDestination(t *testing.T) {
	for _, account := range []string{"", "+1555***4567", "@operator", "1555@s.whatsapp.net", "+1555\n678"} {
		var out bytes.Buffer
		result := channelOnboardingResult{Operation: channelonboarding.Operation{Provider: "whatsapp", AccountReference: account},
			IdentityOperation: &operatorchannel.Operation{Challenge: "12345", ExpiresAt: time.Now().Add(time.Minute)}}
		if err := writeWhatsAppClaimInstructions(&out, result); err == nil || out.Len() != 0 {
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
