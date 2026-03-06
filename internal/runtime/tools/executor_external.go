package tools

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/smtp"
	"net/url"
	"os"
	"regexp"
	"strings"
	"time"

	"empireai/internal/models"
)

func (e *Executor) execInstagramHandleCheck(ctx context.Context, actor models.AgentConfig, input any) (any, error) {
	_ = actor
	var in struct {
		Handle string `json:"handle"`
	}
	if err := decodeToolInput(input, &in); err != nil {
		return nil, err
	}
	handle := strings.TrimSpace(strings.TrimPrefix(in.Handle, "@"))
	if handle == "" {
		return nil, errors.New("handle is required")
	}
	valid := regexp.MustCompile(`^[a-zA-Z0-9._]{1,30}$`)
	if !valid.MatchString(handle) {
		return nil, errors.New("invalid instagram handle format")
	}
	url := "https://www.instagram.com/" + handle + "/"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	available := resp.StatusCode == http.StatusNotFound
	return map[string]any{
		"handle":    handle,
		"available": available,
		"status":    resp.StatusCode,
	}, nil
}

func (e *Executor) execEmailAPI(ctx context.Context, actor models.AgentConfig, input any) (any, error) {
	creds, err := e.loadVerticalCredentials(ctx, actor.VerticalID)
	if err != nil {
		return nil, err
	}
	emailCfg, _ := creds["email"].(map[string]any)
	smtpAddr, _ := emailCfg["smtp_addr"].(string)
	username, _ := emailCfg["username"].(string)
	password, _ := emailCfg["password"].(string)
	from, _ := emailCfg["from"].(string)
	if strings.TrimSpace(smtpAddr) == "" || strings.TrimSpace(from) == "" {
		return nil, errors.New("email credentials not configured (email.smtp_addr/email.from)")
	}

	var in struct {
		To      []string `json:"to"`
		Subject string   `json:"subject"`
		Body    string   `json:"body"`
	}
	if err := decodeToolInput(input, &in); err != nil {
		return nil, err
	}
	if len(in.To) == 0 {
		return nil, errors.New("email_api requires at least one recipient")
	}
	msg := []byte(
		"To: " + strings.Join(in.To, ",") + "\r\n" +
			"Subject: " + in.Subject + "\r\n" +
			"MIME-Version: 1.0\r\n" +
			"Content-Type: text/plain; charset=UTF-8\r\n\r\n" +
			in.Body,
	)
	host := strings.Split(strings.TrimSpace(smtpAddr), ":")[0]
	var auth smtp.Auth
	if strings.TrimSpace(username) != "" || strings.TrimSpace(password) != "" {
		auth = smtp.PlainAuth("", username, password, host)
	}
	if err := smtp.SendMail(smtpAddr, auth, from, in.To, msg); err != nil {
		return nil, fmt.Errorf("send email failed: %w", err)
	}
	return map[string]any{"status": "sent", "to": in.To}, nil
}

func (e *Executor) execExternalProxy(ctx context.Context, actor models.AgentConfig, toolName string, input any) (any, error) {
	creds, err := e.loadExternalCredentials(ctx, actor.VerticalID, toolName)
	if err != nil {
		return nil, err
	}
	for k, v := range DefaultExternalCredentialEnv(toolName) {
		if strings.TrimSpace(v) == "" {
			continue
		}
		if _, exists := creds[k]; !exists {
			creds[k] = v
		}
	}

	var in struct {
		Method         string         `json:"method"`
		URL            string         `json:"url"`
		Path           string         `json:"path"`
		Query          map[string]any `json:"query"`
		Headers        map[string]any `json:"headers"`
		Body           any            `json:"body"`
		TimeoutSeconds int            `json:"timeout_seconds"`
	}
	if err := decodeToolInput(input, &in); err != nil {
		return nil, err
	}

	reqURL := strings.TrimSpace(in.URL)
	if reqURL == "" {
		reqURL = strings.TrimSpace(asString(creds["endpoint"]))
	}
	if reqURL == "" {
		return nil, fmt.Errorf("%s endpoint not configured", toolName)
	}
	if strings.TrimSpace(in.Path) != "" {
		reqURL = strings.TrimRight(reqURL, "/") + "/" + strings.TrimLeft(strings.TrimSpace(in.Path), "/")
	}
	parsedURL, err := url.Parse(reqURL)
	if err != nil {
		return nil, fmt.Errorf("invalid url: %w", err)
	}
	q := parsedURL.Query()
	for k, v := range in.Query {
		key := strings.TrimSpace(k)
		val := strings.TrimSpace(asString(v))
		if key == "" || val == "" {
			continue
		}
		q.Set(key, val)
	}
	parsedURL.RawQuery = q.Encode()

	method := strings.ToUpper(strings.TrimSpace(in.Method))
	if method == "" {
		method = DefaultExternalMethod(toolName)
	}

	var bodyReader io.Reader
	if method != http.MethodGet && method != http.MethodHead {
		payload := in.Body
		if payload == nil {
			payload = input
		}
		raw, err := json.Marshal(payload)
		if err != nil {
			return nil, fmt.Errorf("marshal request body: %w", err)
		}
		bodyReader = bytes.NewReader(raw)
	}

	timeout := 30 * time.Second
	if in.TimeoutSeconds > 0 {
		timeout = time.Duration(in.TimeoutSeconds) * time.Second
	}
	reqCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, method, parsedURL.String(), bodyReader)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	ApplyExternalHeaders(req, in.Headers)
	ApplyExternalCredentialHeaders(req, creds, toolName)
	if req.Body != nil && req.Header.Get("content-type") == "" {
		req.Header.Set("content-type", "application/json")
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("external request failed: %w", err)
	}
	defer resp.Body.Close()

	respBytes, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	respBody := ParseExternalResponseBody(respBytes)
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("%s returned status=%d body=%v", toolName, resp.StatusCode, respBody)
	}
	return map[string]any{
		"status":      "ok",
		"tool":        toolName,
		"status_code": resp.StatusCode,
		"body":        respBody,
	}, nil
}

func (e *Executor) loadVerticalCredentials(ctx context.Context, verticalID string) (map[string]any, error) {
	e.mu.RLock()
	db := e.sqlDB
	e.mu.RUnlock()
	if db == nil {
		return nil, errors.New("sql db is not configured")
	}
	if strings.TrimSpace(verticalID) == "" {
		return nil, errors.New("vertical_id is required for credentialed tool")
	}
	var raw []byte
	if err := db.QueryRowContext(ctx, `
		SELECT COALESCE(credentials, '{}'::jsonb)
		FROM verticals
		WHERE id = $1::uuid
	`, verticalID).Scan(&raw); err != nil {
		return nil, fmt.Errorf("load vertical credentials: %w", err)
	}
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, fmt.Errorf("decode vertical credentials: %w", err)
	}
	if out == nil {
		out = map[string]any{}
	}
	return e.decryptCredentialMap(ctx, out), nil
}

func (e *Executor) loadExternalCredentials(ctx context.Context, verticalID, toolName string) (map[string]any, error) {
	creds := map[string]any{}
	if strings.TrimSpace(verticalID) != "" {
		verticalCreds, err := e.loadVerticalCredentials(ctx, verticalID)
		if err != nil {
			return nil, err
		}
		switch toolName {
		case "whatsapp_business_api":
			MergeCredMap(creds, AsMap(verticalCreds["whatsapp"]))
		case "instagram_api":
			MergeCredMap(creds, AsMap(verticalCreds["instagram"]))
		case "domain_purchase", "domain_availability_check":
			MergeCredMap(creds, AsMap(verticalCreds["registrar"]))
		case "dns_configure":
			MergeCredMap(creds, AsMap(verticalCreds["dns"]))
		case "whatsapp_name_check":
			MergeCredMap(creds, AsMap(verticalCreds["whatsapp_name_check"]))
		}
	}
	return creds, nil
}

func (e *Executor) decryptCredentialMap(ctx context.Context, in map[string]any) map[string]any {
	out := make(map[string]any, len(in))
	for k, v := range in {
		out[k] = e.decryptCredentialValue(ctx, v)
	}
	return out
}

func (e *Executor) decryptCredentialValue(ctx context.Context, v any) any {
	switch t := v.(type) {
	case map[string]any:
		return e.decryptCredentialMap(ctx, t)
	case []any:
		arr := make([]any, len(t))
		for i := range t {
			arr[i] = e.decryptCredentialValue(ctx, t[i])
		}
		return arr
	case string:
		const prefix = "enc::"
		if !strings.HasPrefix(t, prefix) {
			return t
		}
		key := strings.TrimSpace(os.Getenv("EMPIREAI_CREDENTIALS_KEY"))
		if key == "" {
			return t
		}
		e.mu.RLock()
		db := e.sqlDB
		e.mu.RUnlock()
		if db == nil {
			return t
		}
		encoded := strings.TrimSpace(strings.TrimPrefix(t, prefix))
		if encoded == "" {
			return ""
		}
		var plain string
		if err := db.QueryRowContext(ctx, `
			SELECT pgp_sym_decrypt(decode($1, 'base64'), $2::text)
		`, encoded, key).Scan(&plain); err != nil {
			return t
		}
		return plain
	default:
		return v
	}
}
