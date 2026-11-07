package channelprovider

import (
	"fmt"
	"strconv"
	"time"
)

var telegram = Binding{
	id: "telegram", displayName: "Telegram", webhook: true, pollClaims: true, credential: true, signingCredential: true,
	canonicalAccount:  telegramAccount,
	claimInstructions: telegramClaimInstructions,
	claimantFormat:    "Claimed by %s in a %s conversation.\n",
	languageHelp: func(operation string) string {
		return fmt.Sprintf("Declare the Telegram language used on this device: swarm channel resume %s --client-language <language>.\n", operation)
	},
}

func telegramClaimInstructions(_, code string, expires time.Time) (string, error) {
	return fmt.Sprintf("Send the code %s from your operator phone to this number before %s.\nWaiting for an authenticated claimant...\n",
		code, expires.Local().Format(time.RFC3339Nano)), nil
}

func telegramAccount(account string) (string, error) {
	id, err := strconv.ParseUint(account, 10, 64)
	if err != nil || id == 0 || strconv.FormatUint(id, 10) != account {
		return "", fmt.Errorf("account must be the exact Telegram bot ID shown by channel list")
	}
	return account, nil
}
