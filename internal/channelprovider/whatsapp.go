package channelprovider

import (
	"fmt"
	"net/url"
	"regexp"
	"strings"
	"time"
)

var whatsapp = Binding{
	id: "whatsapp", displayName: "WhatsApp", session: true, canonicalAccount: whatsappAccount,
	claimInstructions: whatsappClaimInstructions,
	nativeRead:        "whatsapp.read_account", nativeWrite: "whatsapp.send_text",
	captureKey:    "whatsapp_capture",
	readyTeaching: "WhatsApp ready. Run `swarm serve` to answer customers. Decisions: answer them in the terminal with `swarm mailbox decide`.",
	notLinked:     "Not linked: run swarm channel connect whatsapp",
	pairingPrompt: "Open WhatsApp > Linked Devices > Link a device, then scan (expires %s):\n%s\n",
}

func whatsappClaimInstructions(account, code string, expires time.Time) (string, error) {
	destination, err := whatsappAccount(account)
	if err != nil || expires.IsZero() {
		return "", fmt.Errorf("operator code requires its exact WhatsApp destination and expiry")
	}
	return fmt.Sprintf("FROM your operator phone TO %s\nSend the code %s from your operator phone to this number before %s.\nSEND-CODE LINK (not a device-link QR): https://wa.me/%s?text=%s\nWaiting for an authenticated claimant...\n",
		destination, code, expires.Local().Format(time.RFC3339Nano), strings.TrimPrefix(destination, "+"), url.QueryEscape(code)), nil
}

var e164Account = regexp.MustCompile(`^\+[1-9][0-9]{1,14}$`)

func whatsappAccount(account string) (string, error) {
	account = strings.NewReplacer(" ", "", "-", "", "(", "", ")", "", ".", "").Replace(strings.TrimSpace(account))
	if !e164Account.MatchString(account) {
		return "", fmt.Errorf("account must be an international E.164 number beginning with +")
	}
	return account, nil
}
