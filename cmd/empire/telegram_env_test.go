package main

import "testing"

func TestTelegramEnvHelpers_PreferredAndFallback(t *testing.T) {
	t.Setenv("EMPIREAI_TELEGRAM_TOKEN", "")
	t.Setenv("EMPIREAI_NOTIFY_TELEGRAM_BOT_TOKEN", "legacy-token")
	if got := telegramBotTokenFromEnv(); got != "legacy-token" {
		t.Fatalf("expected legacy token, got %q", got)
	}
	t.Setenv("EMPIREAI_TELEGRAM_TOKEN", "new-token")
	if got := telegramBotTokenFromEnv(); got != "new-token" {
		t.Fatalf("expected preferred token, got %q", got)
	}

	t.Setenv("EMPIREAI_TELEGRAM_CHAT_ID", "")
	t.Setenv("EMPIREAI_NOTIFY_TELEGRAM_CHAT_ID", "legacy-chat")
	if got := telegramChatIDFromEnv(); got != "legacy-chat" {
		t.Fatalf("expected legacy chat, got %q", got)
	}
	t.Setenv("EMPIREAI_TELEGRAM_CHAT_ID", "new-chat")
	if got := telegramChatIDFromEnv(); got != "new-chat" {
		t.Fatalf("expected preferred chat, got %q", got)
	}
}
