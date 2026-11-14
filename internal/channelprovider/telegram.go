package channelprovider

import (
	"fmt"
	"net/url"
	"regexp"
	"strconv"
)

var telegramBotName = regexp.MustCompile(`^[A-Za-z0-9_]{5,32}$`)

func TelegramClaimInstructions(prompt ClaimPrompt) (string, error) {
	if prompt.Deployment {
		if prompt.Connection == nil || prompt.ExpiresAt.IsZero() {
			return "", fmt.Errorf("operator code requires its exact Telegram destination and expiry")
		}
		bot := prompt.Connection.Name
		if !telegramBotName.MatchString(bot) || prompt.Connection.Account != prompt.Account {
			return "", fmt.Errorf("operator code requires its authenticated Telegram bot address")
		}
		return fmt.Sprintf("Send the code %s to @%s before %s.\nSEND-CODE LINK: https://t.me/%s?text=%s\nWaiting for an authenticated claimant...\n",
			prompt.Code, bot, ClaimDeadline(prompt.ExpiresAt), bot, url.QueryEscape(prompt.Code)), nil
	}
	return fmt.Sprintf("Send the code %s from your operator phone to this number before %s.\nWaiting for an authenticated claimant...\n",
		prompt.Code, ClaimDeadline(prompt.ExpiresAt)), nil
}

func TelegramAccount(account string) (string, error) {
	id, err := strconv.ParseUint(account, 10, 64)
	if err != nil || id == 0 || strconv.FormatUint(id, 10) != account {
		return "", fmt.Errorf("account must be the exact Telegram bot ID shown by channel list")
	}
	return account, nil
}
