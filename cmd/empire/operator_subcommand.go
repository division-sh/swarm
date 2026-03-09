package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"empireai/internal/digest"
	"empireai/internal/events"
	"empireai/internal/mailbox"
	runtimepipeline "empireai/internal/runtime/pipeline"
	runtimetools "empireai/internal/runtime/tools"
	"github.com/google/uuid"
)

type operatorOptions struct {
	mailboxStatus       bool
	mailboxList         bool
	mailboxListCritical bool
	mailboxListReviews  bool
	mailboxLimit        int
	mailboxViewID       string
	mailboxDecideID     string
	mailboxDecision     string
	mailboxNotes        string
	digestGenerate      bool
	digestTopN          int
}

func hasOperatorAction(flags ...bool) bool {
	for _, f := range flags {
		if f {
			return true
		}
	}
	return false
}

func runOperatorActions(ctx context.Context, stores storeBundle, opts operatorOptions) error {
	if opts.mailboxStatus || opts.mailboxList || opts.mailboxViewID != "" || opts.mailboxDecideID != "" {
		if stores.MailboxStore == nil {
			return fmt.Errorf("mailbox commands require persistent store mode (use -store postgres)")
		}
	}
	if opts.digestGenerate {
		if stores.MailboxStore == nil || stores.DigestStore == nil {
			return fmt.Errorf("digest command requires persistent store mode (use -store postgres)")
		}
	}

	if (opts.mailboxListCritical || opts.mailboxListReviews) && !opts.mailboxList {
		return fmt.Errorf("-mailbox-list-critical and -mailbox-list-reviews require -mailbox-list")
	}

	if opts.mailboxStatus {
		if err := mailbox.PrintStatus(ctx, stores.MailboxStore, os.Stdout); err != nil {
			return err
		}
	}
	if opts.mailboxList {
		if err := mailbox.PrintPendingWithOptions(ctx, stores.MailboxStore, os.Stdout, mailbox.ListOptions{
			Limit:        opts.mailboxLimit,
			CriticalOnly: opts.mailboxListCritical,
			ReviewsOnly:  opts.mailboxListReviews,
		}); err != nil {
			return err
		}
	}
	if opts.mailboxViewID != "" {
		if err := mailbox.PrintItem(ctx, stores.MailboxStore, os.Stdout, opts.mailboxViewID); err != nil {
			return err
		}
	}
	if opts.mailboxDecideID != "" {
		if opts.mailboxDecision == "" {
			return fmt.Errorf("-mailbox-decision is required with -mailbox-decide-id")
		}
		item, err := stores.MailboxStore.GetMailboxItem(ctx, opts.mailboxDecideID)
		if err != nil {
			return err
		}
		outcome, err := mailbox.Decide(ctx, stores.MailboxStore, opts.mailboxDecideID, opts.mailboxDecision, opts.mailboxNotes)
		if err != nil {
			return err
		}
		if err := emitMailboxDecisionSideEffects(ctx, stores, item, outcome, opts.mailboxNotes); err != nil {
			return err
		}
		fmt.Printf("mailbox: decided id=%s status=%s decision=%s\n", opts.mailboxDecideID, outcome.Status, outcome.Decision)
	}
	if opts.digestGenerate {
		snap, err := digest.BuildSnapshot(ctx, stores.DigestStore, stores.MailboxStore, opts.digestTopN)
		if err != nil {
			return err
		}
		fmt.Println(digest.RenderText(snap))
	}
	return nil
}

func emitMailboxDecisionSideEffects(
	ctx context.Context,
	stores storeBundle,
	item runtimetools.MailboxItem,
	outcome mailbox.DecisionOutcome,
	notes string,
) error {
	// function body moved intact from main.go
	// kept here as command-layer orchestration
	if stores.EventStore == nil {
		return nil
	}
	basePayload := map[string]any{
		"mailbox_id": item.ID,
		"type":       item.Type,
		"status":     outcome.Status,
		"decision":   outcome.Decision,
		"notes":      notes,
		"context":    json.RawMessage(item.Context),
	}

	if err := appendTargetedEvent(ctx, stores, events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("mailbox.decision"),
		SourceAgent: "mailbox",
		VerticalID:  item.VerticalID,
		Payload:     mustJSON(basePayload),
		CreatedAt:   time.Now(),
	}, nil); err != nil {
		return err
	}
	if err := appendTargetedEvent(ctx, stores, events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("mailbox.item_decided"),
		SourceAgent: "mailbox",
		VerticalID:  item.VerticalID,
		Payload:     mustJSON(basePayload),
		CreatedAt:   time.Now(),
	}, nil); err != nil {
		return err
	}
	if outcome.Status == "more_data" && item.VerticalID != "" {
		if err := appendTargetedEvent(ctx, stores, events.Event{
			ID:          uuid.NewString(),
			Type:        events.EventType("vertical.needs_more_data"),
			SourceAgent: "mailbox",
			VerticalID:  item.VerticalID,
			Payload:     mustJSON(basePayload),
			CreatedAt:   time.Now(),
		}, withControlPlaneRecipient()); err != nil {
			return err
		}
	}
	if item.Type == "vertical_approval" && item.VerticalID != "" {
		var evtType events.EventType
		switch outcome.Status {
		case "approved":
			evtType = events.EventType("vertical.approved")
		case "rejected":
			evtType = events.EventType("vertical.killed")
		}
		if evtType != "" {
			if err := appendTargetedEvent(ctx, stores, events.Event{
				ID:          uuid.NewString(),
				Type:        evtType,
				SourceAgent: "mailbox",
				VerticalID:  item.VerticalID,
				Payload:     mustJSON(basePayload),
				CreatedAt:   time.Now(),
			}, withControlPlaneRecipient()); err != nil {
				return err
			}
		}
	}
	if item.Type == "template_migration" && outcome.Status == "approved" {
		if err := appendTargetedEvent(ctx, stores, events.Event{
			ID:          uuid.NewString(),
			Type:        events.EventType("template.migration_approved"),
			SourceAgent: "mailbox",
			VerticalID:  item.VerticalID,
			Payload:     mustJSON(basePayload),
			CreatedAt:   time.Now(),
			}, withControlPlaneRecipient()); err != nil {
			return err
		}
	}
	if item.Type == "spend_request" || item.Type == "budget_increase" || item.Type == "devops.capacity_warning" {
		var evtType events.EventType
		switch outcome.Status {
		case "approved":
			evtType = events.EventType("spend.approved")
		case "rejected":
			evtType = events.EventType("spend.rejected")
		}
		if evtType != "" {
			recipients := []string{}
			if item.VerticalID != "" {
				recipients = []string{fmt.Sprintf("opco-ceo-%s", item.VerticalID)}
			} else if strings.TrimSpace(item.FromAgent) != "" {
				recipients = []string{strings.TrimSpace(item.FromAgent)}
			}
			if err := appendTargetedEvent(ctx, stores, events.Event{
				ID:          uuid.NewString(),
				Type:        evtType,
				SourceAgent: "mailbox",
				VerticalID:  item.VerticalID,
				Payload:     mustJSON(basePayload),
				CreatedAt:   time.Now(),
			}, recipients); err != nil {
				return err
			}
		}
	}
	if isFounderInputMailbox(item) && item.VerticalID != "" {
		if err := appendTargetedEvent(ctx, stores, events.Event{
			ID:          uuid.NewString(),
			Type:        events.EventType("founder_input.response"),
			SourceAgent: "mailbox",
			VerticalID:  item.VerticalID,
			Payload:     mustJSON(basePayload),
			CreatedAt:   time.Now(),
		}, []string{fmt.Sprintf("opco-ceo-%s", item.VerticalID)}); err != nil {
			return err
		}
	}
	if item.VerticalID != "" && strings.Contains(strings.ToLower(item.Type), "escalation") && outcome.Status == "approved" {
		directive := strings.TrimSpace(notes)
		if directive != "" {
			if err := appendTargetedEvent(ctx, stores, events.Event{
				ID:          uuid.NewString(),
				Type:        events.EventType("opco.escalation_response"),
				SourceAgent: "mailbox",
				VerticalID:  item.VerticalID,
				Payload: mustJSON(map[string]any{
					"mailbox_id":     item.ID,
					"directive_text": directive,
					"action_items":   []any{},
					"context":        json.RawMessage(item.Context),
				}),
				CreatedAt: time.Now(),
			}, []string{fmt.Sprintf("opco-ceo-%s", item.VerticalID)}); err != nil {
				return err
			}
		}
	}
	if outcome.Status == "approved" && isGeographyExpansionMailbox(item) {
		geoID, req, campaignID, err := queueGeographyExpansionValidation(ctx, stores.SQLDB, stores.ScanCampaignStore, item)
		if err != nil {
			return err
		}
		if err := appendTargetedEvent(ctx, stores, events.Event{
			ID:          uuid.NewString(),
			Type:        events.EventType("geography.expansion_queued"),
			SourceAgent: "mailbox",
			VerticalID:  item.VerticalID,
			Payload: mustJSON(map[string]any{
				"mailbox_id":   item.ID,
				"vertical_id":  item.VerticalID,
				"geography_id": geoID,
				"geography":    req.Geography,
				"country":      req.Country,
				"region":       req.Region,
				"mode":         req.Mode,
				"categories":   req.Categories,
				"priority":     req.Priority,
				"campaign_id":  campaignID,
				"context":      json.RawMessage(item.Context),
			}),
			CreatedAt: time.Now(),
		}, withControlPlaneRecipient()); err != nil {
			return err
		}
	}
	return nil
}

type geographyExpansionRequest struct {
	Geography  string
	Country    string
	Region     string
	Mode       string
	Categories []string
	Priority   string
}

func mailboxReviewType(raw json.RawMessage) string {
	var obj map[string]any
	if len(raw) == 0 || !json.Valid(raw) {
		return ""
	}
	if err := json.Unmarshal(raw, &obj); err != nil {
		return ""
	}
	for _, key := range []string{"review_type", "kind", "mailbox_type", "subtype"} {
		val := strings.ToLower(strings.TrimSpace(asString(obj[key])))
		if val != "" {
			return val
		}
	}
	return ""
}

func isGeographyExpansionMailbox(item runtimetools.MailboxItem) bool {
	t := strings.ToLower(strings.TrimSpace(item.Type))
	if t == "" {
		return false
	}
	switch t {
	case "domain_approval", "geography_expansion", "vertical_geography_expansion", "expansion_recommendation":
		return true
	}
	if strings.Contains(t, "geography") && strings.Contains(t, "expansion") {
		return true
	}
	if t == "review" {
		rt := mailboxReviewType(item.Context)
		switch rt {
		case "domain_approval", "geography_expansion", "vertical_geography_expansion", "expansion_recommendation":
			return true
		}
	}
	return false
}

func isFounderInputMailbox(item runtimetools.MailboxItem) bool {
	t := strings.ToLower(strings.TrimSpace(item.Type))
	if t == "founder_input" {
		return true
	}
	return t == "review" && mailboxReviewType(item.Context) == "founder_input"
}

func queueGeographyExpansionValidation(ctx context.Context, db *sql.DB, scanStore runtimepipeline.ScanCampaignPersistence, item runtimetools.MailboxItem) (string, geographyExpansionRequest, string, error) {
	req := parseGeographyExpansionRequest(item.Context)
	if strings.TrimSpace(req.Geography) == "" {
		return "", req, "", fmt.Errorf("geography expansion requires context.geography")
	}
	if db == nil {
		return "", req, "", fmt.Errorf("geography expansion requires postgres db")
	}
	geoID, err := ensureGeographyRecord(ctx, db, req)
	if err != nil {
		return "", req, "", err
	}
	if scanStore == nil {
		return geoID, req, "", fmt.Errorf("scan campaign store is unavailable")
	}
	campaign, err := scanStore.CreateScanCampaign(ctx, runtimepipeline.CreateScanCampaignInput{
		GeographyID: geoID,
		Mode:        req.Mode,
		Categories:  req.Categories,
		Priority:    req.Priority,
		Status:      "queued",
	})
	if err != nil {
		return "", req, "", fmt.Errorf("queue geography expansion scan campaign: %w", err)
	}
	return geoID, req, campaign.ID, nil
}

func parseGeographyExpansionRequest(raw json.RawMessage) geographyExpansionRequest {
	out := geographyExpansionRequest{
		Mode:     "local_services",
		Priority: "normal",
	}
	var obj map[string]any
	if len(raw) > 0 && json.Valid(raw) {
		_ = json.Unmarshal(raw, &obj)
	}
	lookup := func(keys ...string) string {
		for _, k := range keys {
			if obj == nil {
				continue
			}
			if v := strings.TrimSpace(asString(obj[k])); v != "" && v != "null" {
				return v
			}
		}
		return ""
	}
	out.Geography = lookup("geography", "target_geography", "geography_name")
	out.Country = lookup("country", "country_code")
	out.Region = lookup("region")
	if mode := strings.ToLower(lookup("mode")); mode != "" {
		out.Mode = mode
	}
	if priority := strings.ToLower(lookup("priority")); priority != "" {
		out.Priority = priority
	}
	if cats := parseStringList(anyFrom(obj, "categories", "taxonomy_categories")); len(cats) > 0 {
		out.Categories = cats
	}
	if out.Country == "" && strings.Contains(out.Geography, ",") {
		parts := strings.Split(out.Geography, ",")
		out.Country = strings.TrimSpace(parts[len(parts)-1])
	}
	if out.Country == "" {
		out.Country = "unspecified"
	}
	return out
}

func ensureGeographyRecord(ctx context.Context, db *sql.DB, req geographyExpansionRequest) (string, error) {
	if db == nil {
		return "", fmt.Errorf("postgres db is required")
	}
	name := strings.TrimSpace(req.Geography)
	country := strings.TrimSpace(req.Country)
	region := strings.TrimSpace(req.Region)

	var id string
	err := db.QueryRowContext(ctx, `
		SELECT id::text
		FROM geographies
		WHERE lower(name) = lower($1)
		  AND ($2 = '' OR lower(country) = lower($2))
		ORDER BY created_at DESC
		LIMIT 1
	`, name, country).Scan(&id)
	if err == nil && strings.TrimSpace(id) != "" {
		return id, nil
	}
	if err != nil && err != sql.ErrNoRows {
		return "", fmt.Errorf("lookup geography %q: %w", name, err)
	}

	id = uuid.NewString()
	scanCfg := mustJSON(map[string]any{
		"source":      "mailbox.geography_expansion",
		"mode":        req.Mode,
		"categories":  req.Categories,
		"priority":    req.Priority,
		"geography":   name,
		"country":     country,
		"region":      region,
		"recorded_at": time.Now().UTC().Format(time.RFC3339),
	})
	if _, err := db.ExecContext(ctx, `
		INSERT INTO geographies (id, name, country, region, scan_config, created_at)
		VALUES ($1::uuid, $2, $3, NULLIF($4,''), $5::jsonb, now())
	`, id, name, country, region, string(scanCfg)); err != nil {
		return "", fmt.Errorf("insert geography %q: %w", name, err)
	}
	return id, nil
}

func anyFrom(m map[string]any, keys ...string) any {
	for _, k := range keys {
		if m == nil {
			continue
		}
		if v, ok := m[k]; ok {
			return v
		}
	}
	return nil
}

func parseStringList(v any) []string {
	normalize := func(in []string) []string {
		seen := make(map[string]struct{}, len(in))
		out := make([]string, 0, len(in))
		for _, raw := range in {
			s := strings.TrimSpace(raw)
			if s == "" {
				continue
			}
			if _, ok := seen[s]; ok {
				continue
			}
			seen[s] = struct{}{}
			out = append(out, s)
		}
		return out
	}
	switch t := v.(type) {
	case []string:
		return normalize(t)
	case []any:
		out := make([]string, 0, len(t))
		for _, x := range t {
			s := strings.TrimSpace(asString(x))
			if s != "" && s != "null" {
				out = append(out, s)
			}
		}
		return normalize(out)
	case string:
		parts := strings.Split(t, ",")
		out := make([]string, 0, len(parts))
		for _, p := range parts {
			s := strings.TrimSpace(p)
			if s != "" {
				out = append(out, s)
			}
		}
		return normalize(out)
	default:
		return nil
	}
}
