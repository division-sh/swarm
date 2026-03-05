package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"empireai/internal/config"
	"empireai/internal/events"
	"empireai/internal/mailbox"
	"empireai/internal/runtime"
)

// Spec v2.0: Human tasks are delivered via Telegram and the bot is bidirectional.
// For Phase 1 local-first operation we implement long polling (getUpdates) so we
// don't require a public webhook endpoint.

func startTelegramHumanTaskBot(ctx context.Context, stores storeBundle, cfg *config.Config, bus *runtime.EventBus) {
	if stores.SQLDB == nil || stores.EventStore == nil || bus == nil {
		return
	}
	tg := buildTelegramNotifierFromEnv()
	if tg == nil {
		return
	}

	// Delivery loop for approved tasks.
	go humanTaskTelegramDeliveryLoop(ctx, stores.SQLDB, bus, tg)
	// Bidirectional polling loop for /claim, /complete_*, /reject, /details.
	go telegramTaskPollingLoop(ctx, stores, cfg, tg)
}

func buildTelegramNotifierFromEnv() *mailbox.TelegramNotifier {
	tgToken := telegramBotTokenFromEnv()
	tgChat := telegramChatIDFromEnv()
	if tgToken == "" || tgChat == "" {
		return nil
	}
	return &mailbox.TelegramNotifier{
		BotToken: tgToken,
		ChatID:   tgChat,
		BaseURL:  telegramBaseURLFromEnv(),
		Client:   &http.Client{Timeout: 10 * time.Second},
	}
}

func humanTaskTelegramDeliveryLoop(ctx context.Context, db *sql.DB, bus *runtime.EventBus, tg *mailbox.TelegramNotifier) {
	if db == nil || bus == nil || tg == nil {
		return
	}

	// Best-effort startup: deliver any approved tasks not yet announced.
	deliverApprovedUnnotified(ctx, db, tg)

	ch := bus.Subscribe("telegram-human-tasks", events.EventType("human_task.approved"))
	for {
		select {
		case <-ctx.Done():
			return
		case evt, ok := <-ch:
			if !ok {
				return
			}
			taskID := extractString(evt.Payload, "task_id")
			if taskID == "" {
				continue
			}
			if err := deliverTaskByID(ctx, db, tg, taskID); err != nil {
				log.Printf("telegram task delivery failed task=%s err=%v", taskID, err)
			}
		}
	}
}

func deliverApprovedUnnotified(ctx context.Context, db *sql.DB, tg *mailbox.TelegramNotifier) {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	rows, err := db.QueryContext(ctx, `
		SELECT id::text
		FROM human_tasks
		WHERE status = 'approved'
		  AND COALESCE(review_decision->>'telegram_notified_at','') = ''
		ORDER BY reviewed_at ASC
		LIMIT 50
	`)
	if err != nil {
		return
	}
	defer rows.Close()
	for rows.Next() {
		var id string
		if rows.Scan(&id) == nil {
			_ = deliverTaskByID(context.Background(), db, tg, strings.TrimSpace(id))
		}
	}
}

type humanTaskRow struct {
	ID            string
	Requester     string
	VerticalID    string
	VerticalSlug  string
	Category      string
	Description   string
	ExpectedValue string
	Priority      string
	TalkingPoints []byte
	Deadline      sql.NullTime
	CreatedAt     time.Time
}

func deliverTaskByID(ctx context.Context, db *sql.DB, tg *mailbox.TelegramNotifier, taskID string) error {
	taskID = strings.TrimSpace(taskID)
	if taskID == "" {
		return fmt.Errorf("task id required")
	}
	ctx, cancel := context.WithTimeout(ctx, 4*time.Second)
	defer cancel()

	var t humanTaskRow
	err := db.QueryRowContext(ctx, `
		SELECT
			t.id::text,
			t.requesting_agent,
			COALESCE(t.vertical_id::text, ''),
			COALESCE(v.slug, ''),
			t.category,
			t.description,
			COALESCE(t.expected_value, ''),
			COALESCE(t.priority, 'medium'),
			COALESCE(t.talking_points, 'null'::jsonb)::jsonb,
			t.deadline,
			t.created_at
		FROM human_tasks t
		LEFT JOIN verticals v ON v.id = t.vertical_id
		WHERE t.id = $1::uuid
	`, taskID).Scan(
		&t.ID,
		&t.Requester,
		&t.VerticalID,
		&t.VerticalSlug,
		&t.Category,
		&t.Description,
		&t.ExpectedValue,
		&t.Priority,
		&t.TalkingPoints,
		&t.Deadline,
		&t.CreatedAt,
	)
	if err != nil {
		return err
	}

	msg := renderTaskTelegramMessage(t)
	sendCtx, cancel2 := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel2()
	if err := tg.NotifyText(sendCtx, msg); err != nil {
		return err
	}

	// Mark as notified to avoid duplicates.
	_, _ = db.ExecContext(context.Background(), `
		UPDATE human_tasks
		SET review_decision = jsonb_set(COALESCE(review_decision,'{}'::jsonb), '{telegram_notified_at}', to_jsonb($2::text), true)
		WHERE id = $1::uuid
	`, taskID, time.Now().UTC().Format(time.RFC3339))
	return nil
}

func renderTaskTelegramMessage(t humanTaskRow) string {
	id8 := t.ID
	if len(id8) > 8 {
		id8 = id8[:8]
	}
	vertical := strings.TrimSpace(t.VerticalSlug)
	if vertical == "" {
		if strings.TrimSpace(t.VerticalID) != "" {
			vertical = t.VerticalID
		} else {
			vertical = "holding"
		}
	}
	var b strings.Builder
	b.WriteString(fmt.Sprintf("[TASK] %s (%s)\n", t.Category, vertical))
	b.WriteString(fmt.Sprintf("id=%s\n", id8))
	b.WriteString(fmt.Sprintf("priority=%s\n", strings.TrimSpace(t.Priority)))
	if t.Deadline.Valid {
		b.WriteString(fmt.Sprintf("deadline=%s\n", t.Deadline.Time.UTC().Format(time.RFC3339)))
	}
	if strings.TrimSpace(t.ExpectedValue) != "" {
		b.WriteString(fmt.Sprintf("expected_value=%s\n", strings.TrimSpace(t.ExpectedValue)))
	}
	b.WriteString("\n")
	b.WriteString(strings.TrimSpace(t.Description))
	b.WriteString("\n\n")

	if tp := renderTalkingPoints(t.TalkingPoints); tp != "" {
		b.WriteString("Talking points:\n")
		b.WriteString(tp)
		b.WriteString("\n")
	}

	b.WriteString("Commands:\n")
	b.WriteString(fmt.Sprintf("/details %s\n", id8))
	b.WriteString(fmt.Sprintf("/claim %s\n", id8))
	b.WriteString(fmt.Sprintf("/reject %s <reason>\n", id8))
	b.WriteString(fmt.Sprintf("/complete_success %s <result>\n", id8))
	b.WriteString(fmt.Sprintf("/complete_partial %s <result>\n", id8))
	b.WriteString(fmt.Sprintf("/complete_failed %s <result>\n", id8))
	return b.String()
}

func renderTalkingPoints(raw []byte) string {
	if len(raw) == 0 || !json.Valid(raw) {
		return ""
	}
	var v any
	if json.Unmarshal(raw, &v) != nil {
		return ""
	}
	switch x := v.(type) {
	case []any:
		var b strings.Builder
		for _, it := range x {
			s := strings.TrimSpace(fmt.Sprintf("%v", it))
			if s == "" {
				continue
			}
			b.WriteString("- ")
			b.WriteString(s)
			b.WriteString("\n")
		}
		return strings.TrimSpace(b.String())
	case map[string]any:
		var b strings.Builder
		for k, it := range x {
			b.WriteString("- ")
			b.WriteString(strings.TrimSpace(k))
			b.WriteString(": ")
			b.WriteString(strings.TrimSpace(fmt.Sprintf("%v", it)))
			b.WriteString("\n")
		}
		return strings.TrimSpace(b.String())
	default:
		return strings.TrimSpace(fmt.Sprintf("%v", v))
	}
}

func telegramTaskPollingLoop(ctx context.Context, stores storeBundle, cfg *config.Config, tg *mailbox.TelegramNotifier) {
	if tg == nil || stores.SQLDB == nil || stores.EventStore == nil {
		return
	}
	baseURL := strings.TrimSpace(tg.BaseURL)
	if baseURL == "" {
		baseURL = "https://api.telegram.org"
	}
	chatIDAllowed := strings.TrimSpace(tg.ChatID)

	client := tg.Client
	if client == nil {
		client = &http.Client{Timeout: 35 * time.Second}
	}

	var offset int64
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		updates, err := telegramGetUpdates(ctx, client, baseURL, tg.BotToken, offset)
		if err != nil {
			log.Printf("telegram getUpdates failed: %v", err)
			time.Sleep(2 * time.Second)
			continue
		}
		for _, u := range updates {
			if u.UpdateID >= offset {
				offset = u.UpdateID + 1
			}
			if u.Message == nil || strings.TrimSpace(u.Message.Text) == "" {
				continue
			}
			if chatIDAllowed != "" && strconv.FormatInt(u.Message.Chat.ID, 10) != chatIDAllowed {
				continue
			}
			resp := handleTelegramTaskCommand(ctx, stores, cfg, u.Message)
			if strings.TrimSpace(resp) == "" {
				continue
			}
			_ = tg.NotifyText(context.Background(), resp)
		}
	}
}

type telegramUpdate struct {
	UpdateID int64            `json:"update_id"`
	Message  *telegramMessage `json:"message,omitempty"`
}

type telegramMessage struct {
	MessageID int64  `json:"message_id"`
	Text      string `json:"text"`
	Chat      struct {
		ID int64 `json:"id"`
	} `json:"chat"`
	From struct {
		Username  string `json:"username"`
		FirstName string `json:"first_name"`
		LastName  string `json:"last_name"`
	} `json:"from"`
}

func telegramGetUpdates(ctx context.Context, client *http.Client, baseURL, token string, offset int64) ([]telegramUpdate, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	endpoint := fmt.Sprintf("%s/bot%s/getUpdates", strings.TrimRight(baseURL, "/"), token)
	q := url.Values{}
	if offset > 0 {
		q.Set("offset", strconv.FormatInt(offset, 10))
	}
	q.Set("timeout", "25")
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint+"?"+q.Encode(), nil)
	if err != nil {
		return nil, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("telegram getUpdates status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(b)))
	}
	var out struct {
		OK     bool             `json:"ok"`
		Result []telegramUpdate `json:"result"`
	}
	if err := json.Unmarshal(b, &out); err != nil {
		return nil, err
	}
	return out.Result, nil
}

func handleTelegramTaskCommand(ctx context.Context, stores storeBundle, cfg *config.Config, msg *telegramMessage) string {
	text := strings.TrimSpace(msg.Text)
	if text == "" {
		return ""
	}
	parts := strings.Fields(text)
	if len(parts) == 0 {
		return ""
	}
	cmd := strings.ToLower(strings.TrimSpace(parts[0]))
	arg := ""
	if len(parts) > 1 {
		arg = strings.TrimSpace(parts[1])
	}
	rest := ""
	if len(parts) > 2 {
		rest = strings.TrimSpace(strings.Join(parts[2:], " "))
	}

	who := strings.TrimSpace(msg.From.Username)
	if who == "" {
		who = strings.TrimSpace(strings.Join([]string{msg.From.FirstName, msg.From.LastName}, " "))
	}
	if who == "" {
		who = "founder"
	}

	switch cmd {
	case "/details":
		id := resolveTaskIDPrefix(ctx, stores.SQLDB, arg)
		if id == "" {
			return "Task not found (use the 8-char id prefix)."
		}
		var desc, cat, vertical string
		_ = stores.SQLDB.QueryRowContext(ctx, `
			SELECT category, description, COALESCE(vertical_id::text,'')
			FROM human_tasks
			WHERE id = $1::uuid
		`, id).Scan(&cat, &desc, &vertical)
		return fmt.Sprintf("[TASK details] %s id=%s\n%s", cat, id[:8], strings.TrimSpace(desc))
	case "/claim":
		id := resolveTaskIDPrefix(ctx, stores.SQLDB, arg)
		if id == "" {
			return "Task not found (use the 8-char id prefix)."
		}
		if err := claimHumanTask(ctx, stores, id, who); err != nil {
			return fmt.Sprintf("Claim failed: %v", err)
		}
		return fmt.Sprintf("Claimed task %s.", id[:8])
	case "/reject":
		id := resolveTaskIDPrefix(ctx, stores.SQLDB, arg)
		if id == "" {
			return "Task not found (use the 8-char id prefix)."
		}
		if err := rejectHumanTask(ctx, stores, cfg, id, rest); err != nil {
			return fmt.Sprintf("Reject failed: %v", err)
		}
		return fmt.Sprintf("Rejected (pushback) task %s. Requeued next cycle.", id[:8])
	case "/complete_success", "/complete_partial", "/complete_failed":
		id := resolveTaskIDPrefix(ctx, stores.SQLDB, arg)
		if id == "" {
			return "Task not found (use the 8-char id prefix)."
		}
		outcome := "success"
		if cmd == "/complete_partial" {
			outcome = "partial"
		}
		if cmd == "/complete_failed" {
			outcome = "failed"
		}
		if strings.TrimSpace(rest) == "" {
			return "Result text required. Example: /complete_success abcd1234 Owner interested, demo Tuesday."
		}
		if err := completeHumanTask(ctx, stores, id, rest, outcome, false); err != nil {
			return fmt.Sprintf("Complete failed: %v", err)
		}
		return fmt.Sprintf("Completed task %s (outcome=%s).", id[:8], outcome)
	default:
		return ""
	}
}

func resolveTaskIDPrefix(ctx context.Context, db *sql.DB, prefix string) string {
	prefix = strings.TrimSpace(prefix)
	if prefix == "" || db == nil {
		return ""
	}
	if len(prefix) < 6 {
		return ""
	}
	like := prefix + "%"
	rows, err := db.QueryContext(ctx, `
		SELECT id::text
		FROM human_tasks
		WHERE id::text LIKE $1
		ORDER BY created_at DESC
		LIMIT 2
	`, like)
	if err != nil {
		return ""
	}
	defer rows.Close()
	ids := []string{}
	for rows.Next() {
		var id string
		if rows.Scan(&id) == nil {
			ids = append(ids, strings.TrimSpace(id))
		}
	}
	if len(ids) == 1 {
		return ids[0]
	}
	return ""
}

func extractString(raw []byte, key string) string {
	if len(raw) == 0 || !json.Valid(raw) {
		return ""
	}
	var obj map[string]any
	if json.Unmarshal(raw, &obj) != nil {
		return ""
	}
	v, _ := obj[key].(string)
	return strings.TrimSpace(v)
}
