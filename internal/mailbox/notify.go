package mailbox

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/smtp"
	"net/url"
	"strings"
	"time"

	runtimetools "swarm/internal/runtime/tools"
)

type CriticalNotifier interface {
	NotifyCritical(ctx context.Context, item runtimetools.MailboxItem) error
}

type MultiCriticalNotifier struct {
	notifiers []CriticalNotifier
}

func NewMultiCriticalNotifier(notifiers ...CriticalNotifier) *MultiCriticalNotifier {
	filtered := make([]CriticalNotifier, 0, len(notifiers))
	for _, n := range notifiers {
		if n != nil {
			filtered = append(filtered, n)
		}
	}
	if len(filtered) == 0 {
		return nil
	}
	return &MultiCriticalNotifier{notifiers: filtered}
}

func (n *MultiCriticalNotifier) NotifyCritical(ctx context.Context, item runtimetools.MailboxItem) error {
	if n == nil || len(n.notifiers) == 0 {
		return nil
	}
	var errs []string
	success := 0
	for _, notifier := range n.notifiers {
		if err := notifier.NotifyCritical(ctx, item); err != nil {
			errs = append(errs, err.Error())
			continue
		}
		success++
	}
	if success > 0 {
		return nil
	}
	return fmt.Errorf("all critical notifiers failed: %s", strings.Join(errs, "; "))
}

type WebhookNotifier struct {
	URL    string
	Client *http.Client
}

func (n *WebhookNotifier) NotifyCritical(ctx context.Context, item runtimetools.MailboxItem) error {
	if strings.TrimSpace(n.URL) == "" {
		return fmt.Errorf("webhook url is required")
	}
	client := n.Client
	if client == nil {
		client = &http.Client{Timeout: 8 * time.Second}
	}
	body, _ := json.Marshal(map[string]any{
		"severity":   "critical",
		"mailbox_id": item.ID,
		"type":       item.Type,
		"from_agent": item.FromAgent,
		"entity_id":  item.EffectiveEntityID(),
		"entity":     item.EffectiveEntityID(),
		"summary":    item.Summary,
		"timeout_at": item.TimeoutAt.UTC().Format(time.RFC3339),
		"context":    json.RawMessage(item.Context),
	})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, n.URL, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("build webhook request: %w", err)
	}
	req.Header.Set("content-type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("send webhook: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("webhook returned status %d", resp.StatusCode)
	}
	return nil
}

type ChatNotifier struct {
	BotToken string
	ChatID   string
	BaseURL  string
	Client   *http.Client
}

func (n *ChatNotifier) NotifyCritical(ctx context.Context, item runtimetools.MailboxItem) error {
	if strings.TrimSpace(n.BotToken) == "" || strings.TrimSpace(n.ChatID) == "" {
		return fmt.Errorf("telegram token and chat id are required")
	}
	baseURL := strings.TrimSpace(n.BaseURL)
	if baseURL == "" {
		baseURL = "https://api.telegram.org"
	}
	client := n.Client
	if client == nil {
		client = &http.Client{Timeout: 8 * time.Second}
	}
	text := fmt.Sprintf(
		"[Platform] CRITICAL mailbox item\nid=%s\nentity=%s\ntype=%s\nfrom=%s\nsummary=%s",
		item.ID,
		item.EffectiveEntityID(),
		item.Type,
		item.FromAgent,
		strings.TrimSpace(item.Summary),
	)
	form := url.Values{}
	form.Set("chat_id", n.ChatID)
	form.Set("text", text)
	endpoint := fmt.Sprintf("%s/bot%s/sendMessage", strings.TrimRight(baseURL, "/"), n.BotToken)
	return sendTelegramWithRetry(ctx, client, endpoint, form)
}

// NotifyText sends an arbitrary text message to the configured Telegram chat.
// This is used for portfolio digest pushes (not mailbox items).
func (n *ChatNotifier) NotifyText(ctx context.Context, text string) error {
	if strings.TrimSpace(n.BotToken) == "" || strings.TrimSpace(n.ChatID) == "" {
		return fmt.Errorf("telegram token and chat id are required")
	}
	baseURL := strings.TrimSpace(n.BaseURL)
	if baseURL == "" {
		baseURL = "https://api.telegram.org"
	}
	client := n.Client
	if client == nil {
		client = &http.Client{Timeout: 8 * time.Second}
	}
	form := url.Values{}
	form.Set("chat_id", n.ChatID)
	form.Set("text", strings.TrimSpace(text))
	endpoint := fmt.Sprintf("%s/bot%s/sendMessage", strings.TrimRight(baseURL, "/"), n.BotToken)
	return sendTelegramWithRetry(ctx, client, endpoint, form)
}

func sendTelegramWithRetry(ctx context.Context, client *http.Client, endpoint string, form url.Values) error {
	if client == nil {
		client = &http.Client{Timeout: 8 * time.Second}
	}
	const maxAttempts = 3
	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, strings.NewReader(form.Encode()))
		if err != nil {
			return fmt.Errorf("build telegram request: %w", err)
		}
		req.Header.Set("content-type", "application/x-www-form-urlencoded")
		resp, err := client.Do(req)
		if err == nil {
			if resp.StatusCode >= 200 && resp.StatusCode < 300 {
				resp.Body.Close()
				return nil
			}
			lastErr = fmt.Errorf("telegram returned status %d", resp.StatusCode)
			resp.Body.Close()
		} else {
			lastErr = fmt.Errorf("send telegram: %w", err)
		}

		if attempt >= maxAttempts {
			break
		}
		backoff := time.Duration(1<<(attempt-1)) * time.Second // 1s, 2s
		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("telegram send failed")
	}
	return lastErr
}

type EmailNotifier struct {
	SMTPAddr string
	Username string
	Password string
	From     string
	To       []string
	Timeout  time.Duration
}

func (n *EmailNotifier) NotifyCritical(ctx context.Context, item runtimetools.MailboxItem) error {
	if strings.TrimSpace(n.SMTPAddr) == "" || strings.TrimSpace(n.From) == "" || len(n.To) == 0 {
		return fmt.Errorf("smtp addr, from, and to are required")
	}
	subject := fmt.Sprintf("Critical mailbox item [%s]", item.Type)
	body := fmt.Sprintf(
		"Critical mailbox item\n\nid: %s\nentity: %s\ntype: %s\nfrom: %s\nsummary: %s\n",
		item.ID, item.EffectiveEntityID(), item.Type, item.FromAgent, strings.TrimSpace(item.Summary),
	)
	msg := []byte(
		"To: " + strings.Join(n.To, ",") + "\r\n" +
			"Subject: " + subject + "\r\n" +
			"MIME-Version: 1.0\r\n" +
			"Content-Type: text/plain; charset=UTF-8\r\n\r\n" +
			body,
	)
	var auth smtp.Auth
	host := strings.Split(n.SMTPAddr, ":")[0]
	if strings.TrimSpace(n.Username) != "" || strings.TrimSpace(n.Password) != "" {
		auth = smtp.PlainAuth("", n.Username, n.Password, host)
	}
	if err := sendSMTPWithContext(ctx, n.SMTPAddr, host, auth, n.From, n.To, msg, n.Timeout); err != nil {
		return fmt.Errorf("send email: %w", err)
	}
	return nil
}

func sendSMTPWithContext(
	ctx context.Context,
	addr string,
	host string,
	auth smtp.Auth,
	from string,
	to []string,
	msg []byte,
	timeout time.Duration,
) error {
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	dialer := &net.Dialer{Timeout: timeout}
	conn, err := dialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		return fmt.Errorf("dial smtp: %w", err)
	}
	cleanupConn := func() { _ = conn.Close() }
	defer cleanupConn()

	_ = conn.SetDeadline(time.Now().Add(timeout))

	client, err := smtp.NewClient(conn, host)
	if err != nil {
		return fmt.Errorf("new smtp client: %w", err)
	}
	defer func() {
		_ = client.Quit()
		_ = client.Close()
	}()

	if ok, _ := client.Extension("STARTTLS"); ok {
		tlsConfig := &tls.Config{
			ServerName: host,
			MinVersion: tls.VersionTLS12,
		}
		if err := client.StartTLS(tlsConfig); err != nil {
			return fmt.Errorf("starttls: %w", err)
		}
	}

	if auth != nil {
		if ok, _ := client.Extension("AUTH"); !ok {
			return fmt.Errorf("smtp auth is not supported by server")
		}
		if err := client.Auth(auth); err != nil {
			return fmt.Errorf("smtp auth: %w", err)
		}
	}

	if err := client.Mail(from); err != nil {
		return fmt.Errorf("smtp mail from: %w", err)
	}
	for _, recipient := range to {
		if err := client.Rcpt(recipient); err != nil {
			return fmt.Errorf("smtp rcpt %s: %w", recipient, err)
		}
	}
	w, err := client.Data()
	if err != nil {
		return fmt.Errorf("smtp data: %w", err)
	}
	if _, err := w.Write(msg); err != nil {
		_ = w.Close()
		return fmt.Errorf("smtp write body: %w", err)
	}
	if err := w.Close(); err != nil {
		return fmt.Errorf("smtp close body: %w", err)
	}
	return nil
}
