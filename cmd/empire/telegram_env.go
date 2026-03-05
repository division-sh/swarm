package main

import (
	"os"
	"strings"
)

func telegramBotTokenFromEnv() string {
	// Preferred (spec v2.0.1+).
	if v := strings.TrimSpace(os.Getenv("EMPIREAI_TELEGRAM_TOKEN")); v != "" {
		return v
	}
	// Backward compatible legacy name.
	return strings.TrimSpace(os.Getenv("EMPIREAI_NOTIFY_TELEGRAM_BOT_TOKEN"))
}

func telegramChatIDFromEnv() string {
	// Preferred (spec v2.0.1+).
	if v := strings.TrimSpace(os.Getenv("EMPIREAI_TELEGRAM_CHAT_ID")); v != "" {
		return v
	}
	// Backward compatible legacy name.
	return strings.TrimSpace(os.Getenv("EMPIREAI_NOTIFY_TELEGRAM_CHAT_ID"))
}

func telegramBaseURLFromEnv() string {
	if v := strings.TrimSpace(os.Getenv("EMPIREAI_TELEGRAM_BASE_URL")); v != "" {
		return v
	}
	return strings.TrimSpace(os.Getenv("EMPIREAI_NOTIFY_TELEGRAM_BASE_URL"))
}
