package channelprovider

import (
	"fmt"
	"net/url"
	"regexp"
	"strings"
)

func WhatsAppClaimInstructions(prompt ClaimPrompt) (string, error) {
	account := prompt.Account
	if account == "" && prompt.Connection != nil {
		account = prompt.Connection.Account
	}
	destination, err := WhatsAppAccount(account)
	if err != nil || prompt.ExpiresAt.IsZero() {
		return "", fmt.Errorf("operator code requires its exact WhatsApp destination and expiry")
	}
	return fmt.Sprintf("FROM your operator phone TO %s\nSend the code %s from your operator phone to this number before %s.\nSEND-CODE LINK (not a device-link QR): https://wa.me/%s?text=%s\nWaiting for an authenticated claimant...\n",
		destination, prompt.Code, ClaimDeadline(prompt.ExpiresAt), strings.TrimPrefix(destination, "+"), url.QueryEscape(prompt.Code)), nil
}

var e164Account = regexp.MustCompile(`^\+[1-9][0-9]{1,14}$`)

func WhatsAppAccount(account string) (string, error) {
	account = strings.NewReplacer(" ", "", "-", "", "(", "", ")", "", ".", "").Replace(strings.TrimSpace(account))
	if !e164Account.MatchString(account) {
		return "", fmt.Errorf("account must be an international E.164 number beginning with +")
	}
	return account, nil
}
