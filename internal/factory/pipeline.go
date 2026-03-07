package factory

import (
	"context"
	"crypto/sha1"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"strings"
	"time"

	"empireai/internal/events"
	runtimebus "empireai/internal/runtime/bus"
	runtimetools "empireai/internal/runtime/tools"
	"empireai/internal/specaudit"
	"github.com/google/uuid"
)

type Pipeline struct {
	DB       *sql.DB
	Events   runtimebus.EventStore
	Mailbox  runtimetools.MailboxPersistence
	Scanners []Scanner
	Scoring  ScoringEngine
}

type Summary struct {
	Discovered     int
	Scored         int
	ReadyForReview int
	Killed         int
	VerticalIDs    []string
}

type ScoreResult struct {
	RubricUsed string
	Dimensions map[string]int
	Viability  int
	Market     int
	Total      int
	Method     string
}

type ScoringEngine interface {
	Score(ctx context.Context, mode, name, geography string, signals []Signal) (ScoreResult, error)
}

type RulesScoringEngine struct{}

func NewPipeline(db *sql.DB, eventStore runtimebus.EventStore, mailbox runtimetools.MailboxPersistence) *Pipeline {
	return &Pipeline{
		DB:      db,
		Events:  eventStore,
		Mailbox: mailbox,
		Scanners: []Scanner{
			GoogleMapsScanner{},
			InstagramScanner{},
			ReviewScanner{},
		},
		Scoring: RulesScoringEngine{},
	}
}

func (p *Pipeline) RunScan(ctx context.Context, geography, depth string, count int) (Summary, error) {
	return p.runScan(ctx, geography, depth, "local_services", nil, count)
}

func (p *Pipeline) RunScanWithMode(ctx context.Context, geography, depth, mode string, taxonomyCategories any, count int) (Summary, error) {
	return p.runScan(ctx, geography, depth, mode, taxonomyCategories, count)
}

func (p *Pipeline) activeScanners() []Scanner {
	if p == nil || len(p.Scanners) == 0 {
		return []Scanner{
			GoogleMapsScanner{},
			InstagramScanner{},
			ReviewScanner{},
		}
	}
	return p.Scanners
}

func (p *Pipeline) scoringEngine() ScoringEngine {
	if p != nil && p.Scoring != nil {
		return p.Scoring
	}
	return RulesScoringEngine{}
}

func (p *Pipeline) runScan(ctx context.Context, geography, depth, mode string, taxonomyCategories any, count int) (Summary, error) {
	if p == nil || p.DB == nil {
		return Summary{}, fmt.Errorf("factory pipeline requires postgres db")
	}
	geography = strings.TrimSpace(geography)
	if geography == "" {
		return Summary{}, fmt.Errorf("geography is required")
	}
	if count <= 0 {
		count = 3
	}
	if depth == "" {
		depth = "full"
	}
	depth = strings.ToLower(strings.TrimSpace(depth))
	mode = normalizeScanMode(mode)

	out := Summary{VerticalIDs: make([]string, 0, count)}
	_ = p.emit(ctx, events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("scan.started"),
		SourceAgent: "discovery-coordinator",
		Payload: mustJSON(map[string]any{
			"geography":           geography,
			"depth":               depth,
			"mode":                mode,
			"taxonomy_categories": taxonomyCategories,
		}),
		CreatedAt: time.Now(),
	}, []string{"empire-coordinator"})

	signals := make([]Signal, 0, 24)
	scannerErrors := make([]map[string]any, 0, 4)
	for _, scanner := range p.activeScanners() {
		found, err := scanner.Scan(ctx, geography, depth)
		if err != nil {
			scannerErrors = append(scannerErrors, map[string]any{
				"scanner": scanner.Name(),
				"error":   err.Error(),
			})
			_ = p.emit(ctx, events.Event{
				ID:          uuid.NewString(),
				Type:        events.EventType("scan.scanner_failed"),
				SourceAgent: "discovery-coordinator",
				Payload: mustJSON(map[string]any{
					"scanner":   scanner.Name(),
					"geography": geography,
					"mode":      mode,
					"depth":     depth,
					"error":     err.Error(),
				}),
				CreatedAt: time.Now(),
			}, []string{"empire-coordinator"})
			continue
		}
		signals = append(signals, found...)
	}
	if len(signals) == 0 {
		if len(scannerErrors) > 0 {
			return out, fmt.Errorf("all scanners failed: %v", scannerErrors)
		}
		return out, fmt.Errorf("scan produced no signals")
	}
	p.emitModeSignals(ctx, mode, geography, taxonomyCategories, signals)

	names := deriveVerticalNamesFromSignals(signals, geography, count)
	seenKeys := make(map[string]struct{}, len(names))
	for _, name := range names {
		key := strings.ToLower(strings.TrimSpace(name) + "|" + strings.TrimSpace(geography))
		if _, exists := seenKeys[key]; exists {
			continue
		}
		seenKeys[key] = struct{}{}

		if existingID, existingStage, found, err := p.findVerticalByNameAndGeography(ctx, name, geography); err != nil {
			return out, fmt.Errorf("dedup discovered vertical: %w", err)
		} else if found {
			out.VerticalIDs = appendUnique(out.VerticalIDs, existingID)
			_ = p.emit(ctx, events.Event{
				ID:          uuid.NewString(),
				Type:        events.EventType("vertical.discovered"),
				SourceAgent: "discovery-coordinator",
				VerticalID:  existingID,
				Payload: mustJSON(map[string]any{
					"vertical_id":         existingID,
					"name":                name,
					"geography":           geography,
					"mode":                mode,
					"depth":               depth,
					"taxonomy_categories": taxonomyCategories,
					"signal_count":        len(signals),
					"dedup":               true,
					"existing_stage":      existingStage,
				}),
				CreatedAt: time.Now(),
			}, []string{"pipeline-coordinator"})
			continue
		}

		id := uuid.NewString()
		slug := makeVerticalSlug(name, id)
		rawSignals := mustJSON(map[string]any{
			"geography":   geography,
			"depth":       depth,
			"mode":        mode,
			"source":      "scan",
			"scanned_at":  time.Now().UTC().Format(time.RFC3339),
			"signal_pack": signalSources(signals),
			"errors":      scannerErrors,
			"signals":     signals,
		})
		if _, err := p.DB.ExecContext(ctx, `
				INSERT INTO verticals (id, name, slug, geography, stage, mode, raw_signals, created_at, updated_at)
				VALUES ($1::uuid, $2, $3, $4, 'discovered', 'factory', $5::jsonb, now(), now())
			`, id, name, slug, geography, string(rawSignals)); err != nil {
			return out, fmt.Errorf("insert discovered vertical: %w", err)
		}
		out.Discovered++
		out.VerticalIDs = append(out.VerticalIDs, id)
		_ = p.emit(ctx, events.Event{
			ID:          uuid.NewString(),
			Type:        events.EventType("vertical.discovered"),
			SourceAgent: "discovery-coordinator",
			VerticalID:  id,
			Payload: mustJSON(map[string]any{
				"vertical_id":         id,
				"name":                name,
				"slug":                slug,
				"geography":           geography,
				"mode":                mode,
				"depth":               depth,
				"taxonomy_categories": taxonomyCategories,
				"signal_count":        len(signals),
				"dedup":               false,
			}),
			CreatedAt: time.Now(),
		}, []string{"pipeline-coordinator"})
	}
	if depth == "discovery" {
		return out, nil
	}

	for _, id := range out.VerticalIDs {
		scoredStage, err := p.scoreVertical(ctx, id)
		if err != nil {
			return out, err
		}
		out.Scored++
		if scoredStage == "killed" {
			out.Killed++
			continue
		}
		if depth == "full" {
			ready, err := p.validateVertical(ctx, id)
			if err != nil {
				return out, err
			}
			if ready {
				out.ReadyForReview++
			}
		}
	}
	return out, nil
}

func (p *Pipeline) RunPending(ctx context.Context, limit int) (Summary, error) {
	if p == nil || p.DB == nil {
		return Summary{}, fmt.Errorf("factory pipeline requires postgres db")
	}
	if limit <= 0 {
		limit = 20
	}
	rows, err := p.DB.QueryContext(ctx, `
		SELECT id::text, stage
		FROM verticals
		WHERE mode = 'factory'
		  AND stage IN ('discovered','scoring','shortlisted','marginal_review','researching','mvp_speccing','spec_review','cto_spec_review','branding')
		ORDER BY updated_at ASC
		LIMIT $1
	`, limit)
	if err != nil {
		return Summary{}, fmt.Errorf("list pending factory verticals: %w", err)
	}
	defer rows.Close()

	out := Summary{VerticalIDs: make([]string, 0)}
	for rows.Next() {
		var id string
		var stage string
		if err := rows.Scan(&id, &stage); err != nil {
			return out, fmt.Errorf("scan pending vertical: %w", err)
		}
		out.VerticalIDs = append(out.VerticalIDs, id)
		if stage == "discovered" || stage == "scoring" {
			next, err := p.scoreVertical(ctx, id)
			if err != nil {
				return out, err
			}
			out.Scored++
			if next == "killed" {
				out.Killed++
				continue
			}
		}
		ready, err := p.validateVertical(ctx, id)
		if err != nil {
			return out, err
		}
		if ready {
			out.ReadyForReview++
		}
	}
	if err := rows.Err(); err != nil {
		return out, fmt.Errorf("iterate pending verticals: %w", err)
	}
	return out, nil
}

func (p *Pipeline) scoreVertical(ctx context.Context, verticalID string) (string, error) {
	var name, geography, currentStage string
	var rawSignals []byte
	if err := p.DB.QueryRowContext(ctx, `
		SELECT name, geography, stage, COALESCE(raw_signals, '{}'::jsonb)
		FROM verticals
		WHERE id = $1::uuid
	`, verticalID).Scan(&name, &geography, &currentStage, &rawSignals); err != nil {
		return "", fmt.Errorf("load vertical for scoring: %w", err)
	}

	mode := "local_services"
	signals := make([]Signal, 0, 32)
	if len(rawSignals) > 0 {
		var rs map[string]any
		if err := json.Unmarshal(rawSignals, &rs); err == nil {
			mode = normalizeScanMode(asString(rs["mode"]))
			signals = parseRawSignals(rs["signals"])
		}
	}
	scoreResult, err := p.scoringEngine().Score(ctx, mode, name, geography, signals)
	if err != nil {
		return "", fmt.Errorf("score vertical: %w", err)
	}
	rubricUsed := strings.TrimSpace(scoreResult.RubricUsed)
	if rubricUsed == "" {
		rubricUsed = normalizeScanMode(mode)
	}
	dimensions := scoreResult.Dimensions
	viability := clampScore(scoreResult.Viability)
	market := clampScore(scoreResult.Market)
	total := clampScore(scoreResult.Total)
	computationMethod := strings.TrimSpace(scoreResult.Method)
	if computationMethod == "" {
		computationMethod = "rules_v1"
	}
	_ = p.emit(ctx, events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("scoring.requested"),
		SourceAgent: "pipeline-coordinator",
		VerticalID:  verticalID,
		Payload: mustJSON(map[string]any{
			"vertical_id":  verticalID,
			"mode":         mode,
			"rubric_used":  rubricUsed,
			"requested_at": time.Now().UTC().Format(time.RFC3339),
		}),
		CreatedAt: time.Now(),
	}, []string{"analysis-agent"})
	for dimension, score := range dimensions {
		_ = p.emit(ctx, events.Event{
			ID:          uuid.NewString(),
			Type:        events.EventType("score.dimension_complete"),
			SourceAgent: "analysis-agent",
			VerticalID:  verticalID,
			Payload: mustJSON(map[string]any{
				"vertical_id": verticalID,
				"dimension":   dimension,
				"score":       score,
				"rubric_used": rubricUsed,
			}),
			CreatedAt: time.Now(),
		}, []string{"pipeline-coordinator"})
	}

	stage := "marginal_review"
	result := "marginal"
	var killReason any
	if viability < 65 || total < 50 {
		stage = "killed"
		result = "rejected"
		killReason = "viability floor below 65"
	} else if total >= 75 {
		stage = "shortlisted"
		result = "shortlisted"
	}
	if err := validateStageTransition(currentStage, stage); err != nil {
		return "", err
	}
	scores := mustJSON(map[string]any{
		"composite":              total,
		"viability":              viability,
		"market":                 market,
		"operational_viability":  viability,
		"market_attractiveness":  market,
		"result":                 result,
		"mode":                   mode,
		"rubric_used":            rubricUsed,
		"dimensions":             dimensions,
		"model":                  "v2.0.16",
		"viability_gate":         65,
		"weights_version":        "v2.0.16",
		"computation_method":     computationMethod,
		"dimension_count_scored": len(dimensions),
		"scored_at":              time.Now().UTC().Format(time.RFC3339),
	})

	if _, err := p.DB.ExecContext(ctx, `
		UPDATE verticals
		SET stage = $2,
		    scores = $3::jsonb,
		    parked_at = CASE
				WHEN $2 = 'marginal_review' THEN COALESCE(parked_at, now())
				ELSE NULL
			END,
		    kill_reason = COALESCE($4, kill_reason),
		    updated_at = now()
		WHERE id = $1::uuid
	`, verticalID, stage, string(scores), killReason); err != nil {
		return "", fmt.Errorf("update scoring stage: %w", err)
	}
	_ = p.emit(ctx, events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("vertical.scored"),
		SourceAgent: "pipeline-coordinator",
		VerticalID:  verticalID,
		Payload:     scores,
		CreatedAt:   time.Now(),
	}, []string{"empire-coordinator"})
	switch result {
	case "shortlisted":
		_ = p.emit(ctx, events.Event{
			ID:          uuid.NewString(),
			Type:        events.EventType("vertical.shortlisted"),
			SourceAgent: "pipeline-coordinator",
			VerticalID:  verticalID,
			Payload:     scores,
			CreatedAt:   time.Now(),
		}, []string{"validation-coordinator"})
	case "marginal":
		_ = p.emit(ctx, events.Event{
			ID:          uuid.NewString(),
			Type:        events.EventType("vertical.marginal"),
			SourceAgent: "pipeline-coordinator",
			VerticalID:  verticalID,
			Payload:     scores,
			CreatedAt:   time.Now(),
		}, []string{"empire-coordinator"})
	case "rejected":
		_ = p.emit(ctx, events.Event{
			ID:          uuid.NewString(),
			Type:        events.EventType("vertical.rejected"),
			SourceAgent: "pipeline-coordinator",
			VerticalID:  verticalID,
			Payload:     scores,
			CreatedAt:   time.Now(),
		}, nil)
	}
	return stage, nil
}

func (p *Pipeline) validateVertical(ctx context.Context, verticalID string) (bool, error) {
	var name, geography, stage string
	if err := p.DB.QueryRowContext(ctx, `
		SELECT name, geography, stage
		FROM verticals
		WHERE id = $1::uuid
	`, verticalID).Scan(&name, &geography, &stage); err != nil {
		return false, fmt.Errorf("load vertical for validation: %w", err)
	}
	if stage == "killed" || stage == "ready_for_review" {
		return stage == "ready_for_review", nil
	}
	_ = p.emit(ctx, events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("validation.started"),
		SourceAgent: "validation-coordinator",
		VerticalID:  verticalID,
		Payload: mustJSON(map[string]any{
			"vertical_id": verticalID,
			"name":        name,
			"geography":   geography,
			"stage":       stage,
		}),
		CreatedAt: time.Now(),
	}, []string{"business-research-agent"})

	brief := mustJSON(map[string]any{
		"customer_profile":      "owner-operator SMB",
		"pain_analysis":         fmt.Sprintf("%s ops are fragmented in %s", strings.ToLower(name), geography),
		"competitive_landscape": "fragmented incumbent tooling with poor localization",
		"distribution_channels": "local outbound + referrals",
		"revenue_model":         "$15/mo starter",
	})
	if err := p.updateStageField(ctx, verticalID, "researching", "business_brief", brief); err != nil {
		return false, err
	}
	_ = p.emit(ctx, events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("research.completed"),
		SourceAgent: "business-research-agent",
		VerticalID:  verticalID,
		Payload:     brief,
		CreatedAt:   time.Now(),
	}, []string{"validation-coordinator"})
	_ = p.emit(ctx, events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("spec.requested"),
		SourceAgent: "business-research-agent",
		VerticalID:  verticalID,
		Payload:     brief,
		CreatedAt:   time.Now(),
	}, []string{"lightweight-spec-agent"})

	mvpSpec := mustJSON(map[string]any{
		"problem":          fmt.Sprintf("%s coordination and bookings", name),
		"target_user":      "local service businesses",
		"core_workflow":    "capture demand -> schedule -> confirm -> follow up",
		"features":         []string{"inbound capture", "calendar", "follow-up reminders"},
		"data_model":       []string{"business", "booking", "customer", "message_log"},
		"user_story":       "As an owner, I want fewer missed bookings so daily operations run predictably.",
		"exclusions":       []string{"advanced analytics", "multi-region billing", "marketplace features"},
		"scope_discipline": "MVP only; no edge-case expansion unless it blocks launch viability",
	})
	if err := p.updateStageField(ctx, verticalID, "mvp_speccing", "mvp_spec", mvpSpec); err != nil {
		return false, err
	}
	_ = p.emit(ctx, events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("spec.draft_ready"),
		SourceAgent: "lightweight-spec-agent",
		VerticalID:  verticalID,
		Payload:     mvpSpec,
		CreatedAt:   time.Now(),
	}, []string{"business-research-agent"})
	_ = p.emit(ctx, events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("spec_review.requested"),
		SourceAgent: "business-research-agent",
		VerticalID:  verticalID,
		Payload: mustJSON(map[string]any{
			"brief":    json.RawMessage(brief),
			"mvp_spec": json.RawMessage(mvpSpec),
		}),
		CreatedAt: time.Now(),
	}, []string{"spec-reviewer"})
	_ = p.emit(ctx, events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("spec_review.passed"),
		SourceAgent: "spec-reviewer",
		VerticalID:  verticalID,
		Payload: mustJSON(map[string]any{
			"checklist": []string{"pain_addressed", "scope_enforced", "feasibility", "user_story"},
		}),
		CreatedAt: time.Now(),
	}, []string{"business-research-agent"})
	_ = p.emit(ctx, events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("spec.approved"),
		SourceAgent: "business-research-agent",
		VerticalID:  verticalID,
		Payload:     mvpSpec,
		CreatedAt:   time.Now(),
	}, []string{"validation-coordinator"})

	feasibility := mustJSON(map[string]any{
		"cto_assessment": "feasible with standard stack",
		"risk":           "medium",
	})

	requestPayload := mustJSON(map[string]any{
		"spec_type":    "vertical_spec",
		"vertical_id":  verticalID,
		"spec":         json.RawMessage(mvpSpec),
		"requested_by": "factory-cto",
	})
	_ = p.emit(ctx, events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("spec.validation_requested"),
		SourceAgent: "factory-cto",
		VerticalID:  verticalID,
		Payload:     requestPayload,
		CreatedAt:   time.Now(),
	}, []string{"spec-auditor"})

	result := specaudit.Validate("vertical_spec", mvpSpec)
	resPayload := mustJSON(map[string]any{
		"spec_type":   result.SpecType,
		"vertical_id": verticalID,
		"passed":      result.Passed,
		"issues":      result.Issues,
	})
	// Persist the spec audit result into the vertical spec_review field and stage.
	if err := p.updateStageField(ctx, verticalID, "spec_review", "spec_review", resPayload); err != nil {
		return false, err
	}

	if !result.Passed {
		_ = p.emit(ctx, events.Event{
			ID:          uuid.NewString(),
			Type:        events.EventType("spec.validation_failed"),
			SourceAgent: "spec-auditor",
			VerticalID:  verticalID,
			Payload:     resPayload,
			CreatedAt:   time.Now(),
		}, []string{"factory-cto"})
		return false, nil
	}
	_ = p.emit(ctx, events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("cto.spec_approved"),
		SourceAgent: "factory-cto",
		VerticalID:  verticalID,
		Payload:     feasibility,
		CreatedAt:   time.Now(),
	}, []string{"validation-coordinator"})

	// CTO feasibility is evaluated after a passing spec audit so the stage transition
	// follows the declared pipeline order (mvp_speccing -> spec_review -> cto_spec_review).
	if err := p.updateStageField(ctx, verticalID, "cto_spec_review", "cto_feasibility", feasibility); err != nil {
		return false, err
	}

	_ = p.emit(ctx, events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("spec.validation_passed"),
		SourceAgent: "spec-auditor",
		VerticalID:  verticalID,
		Payload:     resPayload,
		CreatedAt:   time.Now(),
	}, []string{"factory-cto"})

	brand := mustJSON(map[string]any{
		"name":         makeBrandName(name),
		"tone":         "practical",
		"tagline":      "Run the day without chaos.",
		"generated_at": time.Now().UTC().Format(time.RFC3339),
	})
	_ = p.emit(ctx, events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("brand.requested"),
		SourceAgent: "validation-coordinator",
		VerticalID:  verticalID,
		Payload:     brief,
		CreatedAt:   time.Now(),
	}, []string{"pre-brand-agent"})
	if err := p.updateStageField(ctx, verticalID, "branding", "brand", brand); err != nil {
		return false, err
	}
	_ = p.emit(ctx, events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("brand.candidates_ready"),
		SourceAgent: "pre-brand-agent",
		VerticalID:  verticalID,
		Payload:     brand,
		CreatedAt:   time.Now(),
	}, []string{"validation-coordinator"})

	kit := mustJSON(map[string]any{
		"brief":           json.RawMessage(brief),
		"mvp_spec":        json.RawMessage(mvpSpec),
		"cto_feasibility": json.RawMessage(feasibility),
		"brand":           json.RawMessage(brand),
	})
	if err := p.updateStageField(ctx, verticalID, "ready_for_review", "validation_kit", kit); err != nil {
		return false, err
	}
	_ = p.emit(ctx, events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("vertical.ready_for_review"),
		SourceAgent: "validation-coordinator",
		VerticalID:  verticalID,
		Payload:     kit,
		CreatedAt:   time.Now(),
	}, nil)

	if p.Mailbox != nil {
		_, err := p.Mailbox.InsertMailboxItem(ctx, runtimetools.MailboxItem{
			VerticalID: verticalID,
			FromAgent:  "validation-coordinator",
			Type:       "vertical_approval",
			Priority:   "normal",
			Status:     "pending",
			Context:    kit,
			Summary:    fmt.Sprintf("Factory validation ready: %s (%s)", name, geography),
			TimeoutAt:  time.Now().UTC().Add(48 * time.Hour),
		})
		if err != nil {
			return false, fmt.Errorf("create vertical decision mailbox item: %w", err)
		}
	}

	return true, nil
}

func (p *Pipeline) updateStageField(ctx context.Context, verticalID, stage, field string, raw []byte) error {
	switch field {
	case "business_brief", "mvp_spec", "spec_review", "cto_feasibility", "brand", "validation_kit":
	default:
		return fmt.Errorf("unsupported vertical json field: %s", field)
	}
	if len(raw) == 0 {
		raw = []byte("{}")
	}
	var currentStage string
	if err := p.DB.QueryRowContext(ctx, `
		SELECT stage
		FROM verticals
		WHERE id = $1::uuid
	`, verticalID).Scan(&currentStage); err != nil {
		return fmt.Errorf("load current stage: %w", err)
	}
	if err := validateStageTransition(currentStage, stage); err != nil {
		return err
	}
	q := fmt.Sprintf(`
		UPDATE verticals
		SET stage = $2,
		    %s = $3::jsonb,
		    updated_at = now()
		WHERE id = $1::uuid
	`, field)
	if _, err := p.DB.ExecContext(ctx, q, verticalID, stage, string(raw)); err != nil {
		return fmt.Errorf("update vertical stage field %s: %w", field, err)
	}
	return nil
}

func validateStageTransition(current, next string) error {
	current = strings.TrimSpace(current)
	next = strings.TrimSpace(next)
	if current == "" || next == "" {
		return fmt.Errorf("invalid stage transition: %q -> %q", current, next)
	}
	if current == next {
		return nil
	}
	allowed := map[string]map[string]struct{}{
		// Some flows score synchronously immediately after discovery.
		// Allow direct transitions to the post-scoring outcomes.
		"discovered":       {"scoring": {}, "shortlisted": {}, "marginal_review": {}, "killed": {}},
		"scoring":          {"shortlisted": {}, "marginal_review": {}, "killed": {}},
		"shortlisted":      {"researching": {}, "killed": {}},
		"marginal_review":  {"researching": {}, "killed": {}, "scoring": {}},
		"researching":      {"mvp_speccing": {}, "killed": {}},
		"mvp_speccing":     {"spec_review": {}, "killed": {}},
		"spec_review":      {"cto_spec_review": {}, "researching": {}, "killed": {}},
		"cto_spec_review":  {"branding": {}, "researching": {}, "killed": {}},
		"branding":         {"ready_for_review": {}, "researching": {}, "killed": {}},
		"ready_for_review": {"approved": {}, "killed": {}, "researching": {}},
		"approved":         {"full_speccing": {}, "killed": {}},
		"full_speccing":    {"building": {}, "killed": {}},
		"building":         {"pre_launch": {}, "killed": {}},
		"pre_launch":       {"launched": {}, "killed": {}},
		"launched":         {"operating": {}},
		"operating":        {"expanding": {}, "winding_down": {}},
		"expanding":        {"operating": {}, "winding_down": {}},
		"winding_down":     {},
		"killed":           {},
	}
	nextSet, ok := allowed[current]
	if !ok {
		return fmt.Errorf("unknown current stage: %s", current)
	}
	if _, ok := nextSet[next]; !ok {
		return fmt.Errorf("invalid stage transition: %s -> %s", current, next)
	}
	return nil
}

func (p *Pipeline) emit(ctx context.Context, evt events.Event, recipients []string) error {
	if p.Events == nil {
		return nil
	}
	if evt.ID == "" {
		evt.ID = uuid.NewString()
	}
	if evt.CreatedAt.IsZero() {
		evt.CreatedAt = time.Now()
	}
	if len(evt.Payload) == 0 {
		evt.Payload = []byte("{}")
	}
	if err := p.Events.AppendEvent(ctx, evt); err != nil {
		log.Printf("factory emit append failed type=%s vertical=%s err=%v", evt.Type, evt.VerticalID, err)
		return err
	}
	if len(recipients) > 0 {
		recipients = p.filterExistingRecipients(ctx, recipients)
		if len(recipients) == 0 {
			return nil
		}
		if err := p.Events.InsertEventDeliveries(ctx, evt.ID, recipients); err != nil {
			log.Printf("factory emit deliveries failed event=%s type=%s err=%v", evt.ID, evt.Type, err)
			return err
		}
	}
	return nil
}

func (p *Pipeline) filterExistingRecipients(ctx context.Context, recipients []string) []string {
	if p.DB == nil || len(recipients) == 0 {
		return recipients
	}
	exists := make(map[string]struct{}, len(recipients))
	rows, err := p.DB.QueryContext(ctx, `SELECT id FROM agents`)
	if err != nil {
		return recipients
	}
	defer rows.Close()
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err == nil {
			exists[id] = struct{}{}
		}
	}
	filtered := make([]string, 0, len(recipients))
	for _, id := range recipients {
		if _, ok := exists[id]; ok {
			filtered = append(filtered, id)
		}
	}
	return filtered
}

func candidateVerticalNames(geography string, count int) []string {
	base := []string{
		"Pet Grooming Operations",
		"Dental Clinic Scheduling",
		"Home Cleaning Dispatch",
		"HVAC Service Workflow",
		"Auto Detail Booking",
	}
	out := make([]string, 0, count)
	for i := 0; i < count; i++ {
		out = append(out, fmt.Sprintf("%s - %s", base[i%len(base)], geography))
	}
	return out
}

func deriveVerticalNamesFromSignals(signals []Signal, geography string, count int) []string {
	if count <= 0 {
		count = 3
	}
	if len(signals) == 0 {
		return candidateVerticalNames(geography, count)
	}
	out := make([]string, 0, count)
	seen := make(map[string]struct{})
	for _, s := range signals {
		name := classifyLeadAsVertical(s.Lead)
		if _, ok := seen[name]; ok {
			continue
		}
		seen[name] = struct{}{}
		out = append(out, fmt.Sprintf("%s - %s", name, geography))
		if len(out) >= count {
			return out
		}
	}
	if len(out) < count {
		fallback := candidateVerticalNames(geography, count)
		for _, n := range fallback {
			if _, ok := seen[n]; ok {
				continue
			}
			out = append(out, n)
			if len(out) >= count {
				break
			}
		}
	}
	return out
}

func classifyLeadAsVertical(lead string) string {
	l := strings.ToLower(lead)
	switch {
	case strings.Contains(l, "pet"):
		return "Pet Grooming Operations"
	case strings.Contains(l, "dental"):
		return "Dental Clinic Scheduling"
	case strings.Contains(l, "clean"):
		return "Home Cleaning Dispatch"
	case strings.Contains(l, "hvac"):
		return "HVAC Service Workflow"
	case strings.Contains(l, "auto"):
		return "Auto Detail Booking"
	case strings.Contains(l, "fitness"):
		return "Fitness Studio Operations"
	default:
		return "Local Services Workflow"
	}
}

func makeVerticalSlug(name, id string) string {
	base := strings.ToLower(strings.TrimSpace(name))
	base = strings.ReplaceAll(base, "&", " and ")
	base = strings.ReplaceAll(base, "/", "-")
	base = strings.ReplaceAll(base, "_", "-")
	parts := strings.FieldsFunc(base, func(r rune) bool {
		return (r < 'a' || r > 'z') && (r < '0' || r > '9')
	})
	base = strings.Join(parts, "-")
	base = strings.Trim(base, "-")
	if base == "" {
		base = "vertical"
	}
	suffix := strings.ReplaceAll(strings.TrimSpace(id), "-", "")
	if len(suffix) > 8 {
		suffix = suffix[:8]
	}
	if suffix == "" {
		return base
	}
	return base + "-" + suffix
}

func makeBrandName(verticalName string) string {
	clean := strings.TrimSpace(verticalName)
	clean = strings.ReplaceAll(clean, "Operations", "")
	clean = strings.ReplaceAll(clean, "Workflow", "")
	clean = strings.ReplaceAll(clean, "-", " ")
	parts := strings.Fields(clean)
	if len(parts) == 0 {
		return "LaunchPad"
	}
	if len(parts) > 2 {
		parts = parts[:2]
	}
	return strings.Join(parts, "")
}

func normalizeScanMode(mode string) string {
	mode = strings.ToLower(strings.TrimSpace(mode))
	switch mode {
	case "local_services", "saas_gap", "saas_trend":
		return mode
	default:
		return "local_services"
	}
}

func computeScore(mode, name, geography string) (rubricUsed string, dimensions map[string]int, viability, market, total int) {
	res, _ := (RulesScoringEngine{}).Score(context.Background(), mode, name, geography, nil)
	return res.RubricUsed, res.Dimensions, res.Viability, res.Market, res.Total
}

func (RulesScoringEngine) Score(_ context.Context, mode, name, geography string, signals []Signal) (ScoreResult, error) {
	mode = normalizeScanMode(mode)
	stats := buildSignalStats(mode, name, geography, signals)

	switch mode {
	case "saas_gap", "saas_trend":
		dimensions := map[string]int{
			"willingness_to_pay":     clampScore(35 + stats.avgScore/2 + stats.painSignals*2),
			"retention_likelihood":   clampScore(30 + stats.avgScore/2 + stats.reviewsCount*3),
			"technical_feasibility":  clampScore(45 + stats.googleCount*4 + stats.instagramCount*2),
			"distribution_access":    clampScore(30 + stats.instagramCount*8 + stats.totalSignals*2),
			"regulatory_moat":        clampScore(20 + stats.highScoreSignals*4 + stats.painSignals*2),
			"competition_weakness":   clampScore(25 + maxInt(0, 5-stats.googleCount)*6 + stats.painSignals*2),
			"pain_severity":          clampScore(20 + stats.painSignals*12 + stats.avgScore/3),
			"market_size":            clampScore(25 + stats.googleCount*8 + stats.totalSignals*2),
			"localization_advantage": clampScore(30 + stats.geoSpecificity*6 + stats.painSignals*2),
		}
		viability := weightedSubscore(dimensions, map[string]float64{
			"willingness_to_pay":    15,
			"retention_likelihood":  15,
			"technical_feasibility": 15,
			"distribution_access":   15,
		})
		market := weightedSubscore(dimensions, map[string]float64{
			"regulatory_moat":        12,
			"competition_weakness":   10,
			"pain_severity":          8,
			"market_size":            5,
			"localization_advantage": 5,
		})
		total := weightedComposite(dimensions, map[string]float64{
			"willingness_to_pay":     15,
			"retention_likelihood":   15,
			"technical_feasibility":  15,
			"distribution_access":    15,
			"regulatory_moat":        12,
			"competition_weakness":   10,
			"pain_severity":          8,
			"market_size":            5,
			"localization_advantage": 5,
		})
		return ScoreResult{
			RubricUsed: "saas",
			Dimensions: dimensions,
			Viability:  viability,
			Market:     market,
			Total:      total,
			Method:     "rules_v1",
		}, nil
	default:
		dimensions := map[string]int{
			"willingness_to_pay":   clampScore(40 + stats.avgScore/2 + stats.reviewsCount*4),
			"retention_likelihood": clampScore(35 + stats.avgScore/3 + stats.reviewsCount*5),
			"channel_access":       clampScore(30 + stats.instagramCount*10 + stats.googleCount*2),
			"operational_friction": clampScore(25 + stats.painSignals*10 + stats.reviewsCount*3),
			"business_density":     clampScore(30 + stats.googleCount*9 + stats.totalSignals*2),
			"pain_severity":        clampScore(25 + stats.painSignals*12 + stats.avgScore/4),
			"competition_weakness": clampScore(20 + maxInt(0, 6-stats.googleCount)*5 + stats.painSignals*3),
			"revenue_per_business": clampScore(35 + stats.avgScore/2 + stats.highScoreSignals*3),
		}
		viability := weightedSubscore(dimensions, map[string]float64{
			"willingness_to_pay":   20,
			"retention_likelihood": 15,
			"channel_access":       15,
			"operational_friction": 10,
		})
		market := weightedSubscore(dimensions, map[string]float64{
			"business_density":     12,
			"pain_severity":        10,
			"competition_weakness": 10,
			"revenue_per_business": 8,
		})
		total := weightedComposite(dimensions, map[string]float64{
			"willingness_to_pay":   20,
			"retention_likelihood": 15,
			"channel_access":       15,
			"operational_friction": 10,
			"business_density":     12,
			"pain_severity":        10,
			"competition_weakness": 10,
			"revenue_per_business": 8,
		})
		return ScoreResult{
			RubricUsed: "local_services",
			Dimensions: dimensions,
			Viability:  viability,
			Market:     market,
			Total:      total,
			Method:     "rules_v1",
		}, nil
	}
}

type signalStats struct {
	totalSignals     int
	avgScore         int
	googleCount      int
	instagramCount   int
	reviewsCount     int
	painSignals      int
	highScoreSignals int
	geoSpecificity   int
}

func buildSignalStats(mode, name, geography string, signals []Signal) signalStats {
	stats := signalStats{}
	normalizedGeo := strings.ToLower(strings.TrimSpace(geography))
	totalScore := 0
	for _, s := range signals {
		score := clampScore(s.Score)
		totalScore += score
		stats.totalSignals++
		switch strings.ToLower(strings.TrimSpace(s.Source)) {
		case "google_maps":
			stats.googleCount++
		case "instagram":
			stats.instagramCount++
		case "reviews":
			stats.reviewsCount++
		}
		lead := strings.ToLower(strings.TrimSpace(s.Lead))
		if containsAny(lead, "pain", "no-show", "wait", "manual", "slow", "confusion", "issue", "friction") {
			stats.painSignals++
		}
		if score >= 80 {
			stats.highScoreSignals++
		}
		if normalizedGeo != "" && strings.Contains(lead, normalizedGeo) {
			stats.geoSpecificity++
		}
	}
	if stats.totalSignals == 0 {
		seed := scoreHash(mode + "|" + name + "|" + geography)
		stats.totalSignals = 4
		stats.avgScore = clampScore(45 + (seed % 35))
		stats.googleCount = 2
		stats.instagramCount = 1
		stats.reviewsCount = 1
		stats.painSignals = 1 + (seed % 2)
		stats.highScoreSignals = seed % 2
		stats.geoSpecificity = 1
		return stats
	}
	stats.avgScore = clampScore(totalScore / stats.totalSignals)
	if stats.geoSpecificity == 0 {
		stats.geoSpecificity = 1
	}
	return stats
}

func containsAny(v string, needles ...string) bool {
	for _, n := range needles {
		if strings.Contains(v, strings.ToLower(strings.TrimSpace(n))) {
			return true
		}
	}
	return false
}

func weightedSubscore(dimensions map[string]int, weights map[string]float64) int {
	if len(weights) == 0 {
		return 0
	}
	var weighted float64
	var totalWeight float64
	for dim, w := range weights {
		if w <= 0 {
			continue
		}
		weighted += float64(dimensions[dim]) * w
		totalWeight += w
	}
	if totalWeight == 0 {
		return 0
	}
	return clampScore(int(math.Round(weighted / totalWeight)))
}

func weightedComposite(dimensions map[string]int, weights map[string]float64) int {
	if len(weights) == 0 {
		return 0
	}
	var weighted float64
	var totalWeight float64
	for dim, w := range weights {
		if w <= 0 {
			continue
		}
		weighted += float64(dimensions[dim]) * w
		totalWeight += w
	}
	if totalWeight == 0 {
		return 0
	}
	return clampScore(int(math.Round(weighted / totalWeight)))
}

func clampScore(score int) int {
	if score < 0 {
		return 0
	}
	if score > 100 {
		return 100
	}
	return score
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func intFromAny(v any) int {
	switch t := v.(type) {
	case int:
		return t
	case int32:
		return int(t)
	case int64:
		return int(t)
	case float64:
		return int(t)
	case float32:
		return int(t)
	case json.Number:
		if n, err := t.Int64(); err == nil {
			return int(n)
		}
		if f, err := t.Float64(); err == nil {
			return int(f)
		}
		return 0
	case string:
		var out int
		_, _ = fmt.Sscanf(strings.TrimSpace(t), "%d", &out)
		return out
	default:
		return 0
	}
}

func (p *Pipeline) emitModeSignals(ctx context.Context, mode, geography string, taxonomyCategories any, signals []Signal) {
	switch normalizeScanMode(mode) {
	case "saas_gap":
		categories := parseCategories(taxonomyCategories)
		if len(categories) == 0 {
			categories = []string{"operations", "workflow", "billing", "crm", "automation"}
		}
		limit := minInt(len(categories), 5)
		for i := 0; i < limit; i++ {
			category := strings.TrimSpace(categories[i])
			if category == "" {
				continue
			}
			_ = p.emit(ctx, events.Event{
				ID:          uuid.NewString(),
				Type:        events.EventType("category.assessed"),
				SourceAgent: "market-research-agent",
				Payload: mustJSON(map[string]any{
					"category":        category,
					"signal_strength": 50 + (scoreHash(category+"|"+geography) % 51),
					"geography":       geography,
					"mode":            mode,
					"sample_size":     len(signals),
				}),
				CreatedAt: time.Now(),
			}, []string{"discovery-coordinator"})
		}
	case "saas_trend":
		trends := []string{"ai_assisted_ops", "verticalized_crm", "self_serve_onboarding"}
		for _, trend := range trends {
			_ = p.emit(ctx, events.Event{
				ID:          uuid.NewString(),
				Type:        events.EventType("trend.identified"),
				SourceAgent: "trend-research-agent",
				Payload: mustJSON(map[string]any{
					"trend_category": trend,
					"urgency":        scoreBand(60 + (scoreHash(trend+"|"+geography) % 41)),
					"signal_count":   len(signals),
					"geography":      geography,
					"mode":           mode,
				}),
				CreatedAt: time.Now(),
			}, []string{"discovery-coordinator"})
		}
	}
}

func parseCategories(raw any) []string {
	switch v := raw.(type) {
	case []string:
		return uniqueCategories(v)
	case []any:
		out := make([]string, 0, len(v))
		for _, item := range v {
			val := strings.TrimSpace(asString(item))
			if val != "" {
				out = append(out, val)
			}
		}
		return uniqueCategories(out)
	default:
		return nil
	}
}

func parseRawSignals(raw any) []Signal {
	switch v := raw.(type) {
	case []Signal:
		return v
	case []any:
		out := make([]Signal, 0, len(v))
		for _, item := range v {
			obj, _ := item.(map[string]any)
			if obj == nil {
				continue
			}
			s := Signal{
				Source: strings.TrimSpace(asString(obj["source"])),
				Lead:   strings.TrimSpace(asString(obj["lead"])),
				Score:  clampScore(intFromAny(obj["score"])),
			}
			if s.Source == "" && s.Lead == "" && s.Score == 0 {
				continue
			}
			out = append(out, s)
		}
		return out
	default:
		return nil
	}
}

func signalSources(signals []Signal) []string {
	if len(signals) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(signals))
	out := make([]string, 0, len(signals))
	for _, s := range signals {
		source := strings.ToLower(strings.TrimSpace(s.Source))
		if source == "" {
			continue
		}
		if _, ok := seen[source]; ok {
			continue
		}
		seen[source] = struct{}{}
		out = append(out, source)
	}
	return out
}

func uniqueCategories(in []string) []string {
	if len(in) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(in))
	out := make([]string, 0, len(in))
	for _, raw := range in {
		v := strings.TrimSpace(raw)
		if v == "" {
			continue
		}
		key := strings.ToLower(v)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, v)
	}
	return out
}

func scoreBand(score int) string {
	switch {
	case score >= 85:
		return "high"
	case score >= 70:
		return "medium"
	default:
		return "low"
	}
}

func appendUnique(in []string, v string) []string {
	v = strings.TrimSpace(v)
	if v == "" {
		return in
	}
	for _, existing := range in {
		if existing == v {
			return in
		}
	}
	return append(in, v)
}

func (p *Pipeline) findVerticalByNameAndGeography(ctx context.Context, name, geography string) (id, stage string, found bool, err error) {
	name = strings.TrimSpace(name)
	geography = strings.TrimSpace(geography)
	if name == "" || geography == "" {
		return "", "", false, nil
	}
	if err = p.DB.QueryRowContext(ctx, `
		SELECT id::text, stage
		FROM verticals
		WHERE lower(name) = lower($1)
		  AND lower(geography) = lower($2)
		ORDER BY created_at DESC
		LIMIT 1
	`, name, geography).Scan(&id, &stage); err != nil {
		if err == sql.ErrNoRows {
			return "", "", false, nil
		}
		return "", "", false, err
	}
	return id, stage, true, nil
}

func scoreHash(v string) int {
	h := sha1.Sum([]byte(strings.ToLower(strings.TrimSpace(v))))
	hexed := hex.EncodeToString(h[:4])
	n := 0
	for _, r := range hexed {
		n += int(r)
	}
	return n
}

func mustJSON(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		return []byte("{}")
	}
	return b
}
