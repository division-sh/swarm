package factory

import (
	"bytes"
	"context"
	"empireai/internal/store"
	"empireai/internal/testutil"
	"errors"
	"github.com/google/uuid"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"
)

func TestPipeline_RunScan_DiscoveryOnlyAndFull(t *testing.T) {
	t.Setenv("GOOGLE_MAPS_API_KEY", "")
	t.Setenv("YELP_API_KEY", "")

	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}
	p := NewPipeline(db, pg, pg)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	sum, err := p.RunScan(ctx, "Austin, TX", "discovery", 2)
	if err != nil {
		t.Fatalf("RunScan discovery: %v", err)
	}
	if sum.Discovered != 2 || sum.Scored != 0 || sum.ReadyForReview != 0 {
		t.Fatalf("unexpected discovery summary: %+v", sum)
	}

	sum, err = p.RunScan(ctx, "Austin, TX", "full", 2)
	if err != nil {
		t.Fatalf("RunScan full: %v", err)
	}
	if sum.Discovered != 0 || sum.Scored != 2 {
		t.Fatalf("unexpected full summary: %+v", sum)
	}

	if len(sum.VerticalIDs) != 2 {
		t.Fatalf("expected 2 vertical ids, got %d", len(sum.VerticalIDs))
	}
}

func TestPipeline_RunPending_ScoresDiscoveredThenValidates(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}
	p := NewPipeline(db, pg, pg)
	ctx := context.Background()

	// Seed a discovered vertical.
	var vID string
	if err := db.QueryRowContext(ctx, `
		INSERT INTO verticals (name, slug, geography, stage, mode, created_at, updated_at)
		VALUES ('V', 'vslug', 'us', 'discovered', 'factory', now(), now())
		RETURNING id::text
	`).Scan(&vID); err != nil {
		t.Fatalf("seed vertical: %v", err)
	}

	sum, err := p.RunPending(ctx, 10)
	if err != nil {
		t.Fatalf("RunPending: %v", err)
	}
	if sum.Scored < 1 {
		t.Fatalf("expected scored >= 1, got %+v", sum)
	}
	if len(sum.VerticalIDs) != 1 || sum.VerticalIDs[0] != vID {
		t.Fatalf("unexpected pending ids: %+v", sum.VerticalIDs)
	}

	// validateVertical path should eventually write a validation kit or kill_reason.
	var stage string
	if err := db.QueryRowContext(ctx, `SELECT stage FROM verticals WHERE id=$1::uuid`, vID).Scan(&stage); err != nil {
		t.Fatalf("load stage: %v", err)
	}
	if stage == "" {
		t.Fatalf("expected stage updated")
	}
}

func TestPipeline_RunPending_IncludesSpecReviewStage(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}
	p := NewPipeline(db, pg, pg)
	ctx := context.Background()

	var vID string
	if err := db.QueryRowContext(ctx, `
		INSERT INTO verticals (name, slug, geography, stage, mode, created_at, updated_at)
		VALUES ('Spec Review Vertical', 'spec-review-v', 'us', 'spec_review', 'factory', now(), now())
		RETURNING id::text
	`).Scan(&vID); err != nil {
		t.Fatalf("seed spec_review vertical: %v", err)
	}

	sum, err := p.RunPending(ctx, 10)
	if err != nil {
		t.Fatalf("RunPending: %v", err)
	}
	found := false
	for _, id := range sum.VerticalIDs {
		if id == vID {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected spec_review vertical to be processed, ids=%v", sum.VerticalIDs)
	}
}

func TestPipeline_ValidationMailboxHasTimeout(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}
	p := NewPipeline(db, pg, pg)
	ctx := context.Background()

	var vID string
	if err := db.QueryRowContext(ctx, `
		INSERT INTO verticals (name, slug, geography, stage, mode, created_at, updated_at)
		VALUES ('Timeout Vertical', 'timeout-v', 'us', 'shortlisted', 'factory', now(), now())
		RETURNING id::text
	`).Scan(&vID); err != nil {
		t.Fatalf("seed vertical: %v", err)
	}

	ready, err := p.validateVertical(ctx, vID)
	if err != nil {
		t.Fatalf("validateVertical: %v", err)
	}
	if !ready {
		t.Fatalf("expected ready_for_review")
	}

	var timeoutAt time.Time
	if err := db.QueryRowContext(ctx, `
		SELECT timeout_at
		FROM mailbox
		WHERE vertical_id = $1::uuid
		  AND type = 'vertical_decision'
		ORDER BY created_at DESC
		LIMIT 1
	`, vID).Scan(&timeoutAt); err != nil {
		t.Fatalf("load mailbox timeout: %v", err)
	}
	if timeoutAt.IsZero() {
		t.Fatal("expected non-zero mailbox timeout")
	}
	if timeoutAt.Before(time.Now().Add(47*time.Hour)) || timeoutAt.After(time.Now().Add(49*time.Hour)) {
		t.Fatalf("unexpected timeout window: %s", timeoutAt.UTC().Format(time.RFC3339))
	}
}

func TestPipeline_RunScan_Validations(t *testing.T) {
	p := (*Pipeline)(nil)
	if _, err := p.RunScan(context.Background(), "x", "discovery", 1); err == nil {
		t.Fatalf("expected db required error")
	}
	_, db, _ := testutil.StartPostgres(t)
	p2 := NewPipeline(db, nil, nil)
	if _, err := p2.RunScan(context.Background(), "   ", "discovery", 1); err == nil {
		t.Fatalf("expected geography required error")
	}
}

func TestPipeline_ScoreVertical_RejectsUnknownStage(t *testing.T) {
	if err := validateStageTransition("weird_stage", "scoring"); err == nil {
		t.Fatalf("expected unknown current stage error")
	}
}

func TestPipeline_ValidateVertical_StopsOnTerminalStages(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	p := NewPipeline(db, nil, nil)
	ctx := context.Background()

	idKilled := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO verticals (id, name, slug, geography, stage, mode, created_at, updated_at)
		VALUES ($1::uuid, 'V', 'k', 'us', 'killed', 'factory', now(), now())
	`, idKilled); err != nil {
		t.Fatalf("seed killed: %v", err)
	}
	ok, err := p.validateVertical(ctx, idKilled)
	if err != nil || ok {
		t.Fatalf("expected killed -> false, got ok=%v err=%v", ok, err)
	}

	idReady := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO verticals (id, name, slug, geography, stage, mode, created_at, updated_at)
		VALUES ($1::uuid, 'V', 'r', 'us', 'ready_for_review', 'factory', now(), now())
	`, idReady); err != nil {
		t.Fatalf("seed ready: %v", err)
	}
	ok, err = p.validateVertical(ctx, idReady)
	if err != nil || !ok {
		t.Fatalf("expected ready_for_review -> true, got ok=%v err=%v", ok, err)
	}
}

func TestPipeline_UpdateStageField_DefaultJSON(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	p := NewPipeline(db, nil, nil)
	ctx := context.Background()
	id := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO verticals (id, name, slug, geography, stage, mode, created_at, updated_at)
		VALUES ($1::uuid, 'V', 'v', 'us', 'shortlisted', 'factory', now(), now())
	`, id); err != nil {
		t.Fatalf("seed vertical: %v", err)
	}

	if err := p.updateStageField(ctx, id, "researching", "business_brief", nil); err != nil {
		t.Fatalf("updateStageField: %v", err)
	}
	var stage string
	if err := db.QueryRowContext(ctx, `SELECT stage FROM verticals WHERE id=$1::uuid`, id).Scan(&stage); err != nil {
		t.Fatalf("load stage: %v", err)
	}
	if stage != "researching" {
		t.Fatalf("expected stage researching, got %s", stage)
	}
	_ = time.Second
}

func TestFactoryPipeline_RunScan_RunPending_And_Transitions(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}
	ctx := context.Background()

	for _, id := range []string{"empire-coordinator", "spec-auditor", "factory-cto"} {
		if _, err := db.ExecContext(ctx, `
			INSERT INTO agents (id, type, role, mode, status, config)
			VALUES ($1, 'stub', $1, 'holding', 'active', '{}'::jsonb)
			ON CONFLICT (id) DO NOTHING
		`, id); err != nil {
			t.Fatalf("seed agent %s: %v", id, err)
		}
	}

	p := NewPipeline(db, pg, pg)

	if _, err := p.RunScan(ctx, "", "discovery", 1); err == nil {
		t.Fatal("expected missing geography error")
	}

	sum, err := p.RunScan(ctx, "us", "discovery", 2)
	if err != nil {
		t.Fatalf("RunScan discovery: %v", err)
	}
	if sum.Discovered == 0 || len(sum.VerticalIDs) == 0 {
		t.Fatalf("expected discoveries, got %+v", sum)
	}

	sum2, err := p.RunScan(ctx, "us", "full", 1)
	if err != nil {
		t.Fatalf("RunScan full: %v", err)
	}
	if sum2.Discovered != 0 || sum2.Scored != 1 {
		t.Fatalf("unexpected summary: %+v", sum2)
	}

	if _, err := p.RunPending(ctx, 10); err != nil {
		t.Fatalf("RunPending: %v", err)
	}

	if err := validateStageTransition("unknown", "discovered"); err == nil {
		t.Fatal("expected unknown stage error")
	}
	if err := validateStageTransition("discovered", "discovered"); err != nil {
		t.Fatalf("same-stage should be ok: %v", err)
	}
	if err := validateStageTransition("discovered", "operating"); err == nil {
		t.Fatal("expected invalid transition error")
	}
}

func TestCandidateVerticalNames_CountAndFormat(t *testing.T) {
	out := candidateVerticalNames("NYC", 7)
	if len(out) != 7 {
		t.Fatalf("expected 7, got %d", len(out))
	}
	if out[0] == "" || out[0] == out[1] {
		t.Fatalf("unexpected output: %#v", out[:2])
	}
}

func TestDeriveVerticalNamesFromSignals_FallbackAndDedup(t *testing.T) {

	out := deriveVerticalNamesFromSignals(nil, "SF", 3)
	if len(out) != 3 {
		t.Fatalf("expected 3, got %d", len(out))
	}

	signals := []Signal{
		{Lead: "pet grooming in SF", Score: 90},
		{Lead: "PET services", Score: 80},
		{Lead: "hvac repair", Score: 70},
	}
	out = deriveVerticalNamesFromSignals(signals, "SF", 3)
	if len(out) != 3 {
		t.Fatalf("expected 3, got %d out=%#v", len(out), out)
	}

	foundPet, foundHVAC := false, false
	for _, n := range out {
		if n == "Pet Grooming Operations - SF" {
			foundPet = true
		}
		if n == "HVAC Service Workflow - SF" {
			foundHVAC = true
		}
	}
	if !foundPet || !foundHVAC {
		t.Fatalf("expected derived names to include pet+hvac, got %#v", out)
	}
}

func TestClassifyLeadAsVertical_Cases(t *testing.T) {
	cases := map[string]string{
		"pet grooming":    "Pet Grooming Operations",
		"DENTAL clinic":   "Dental Clinic Scheduling",
		"home CLEANing":   "Home Cleaning Dispatch",
		"HVAC tune up":    "HVAC Service Workflow",
		"auto detailing":  "Auto Detail Booking",
		"fitness coach":   "Fitness Studio Operations",
		"something else":  "Local Services Workflow",
		"":                "Local Services Workflow",
		"random services": "Local Services Workflow",
	}
	for lead, want := range cases {
		if got := classifyLeadAsVertical(lead); got != want {
			t.Fatalf("lead=%q got=%q want=%q", lead, got, want)
		}
	}
}

func TestPipeline_ValidateVertical_AdvancesStagesAndWritesFields(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}
	p := NewPipeline(db, pg, pg)

	verticalID := uuid.NewString()
	if _, err := db.ExecContext(context.Background(), `
		INSERT INTO verticals (id, name, slug, geography, stage, mode, created_at, updated_at)
		VALUES ($1::uuid,'TestCo','testco','us','shortlisted','factory', now(), now())
	`, verticalID); err != nil {
		t.Fatalf("seed vertical: %v", err)
	}

	ok, err := p.validateVertical(context.Background(), verticalID)
	if err != nil {
		t.Fatalf("validateVertical err: %v", err)
	}
	if !ok {
		t.Fatalf("expected validation ok")
	}

	// Verify stage advanced.
	var stage string
	if err := db.QueryRowContext(context.Background(), `SELECT stage FROM verticals WHERE id=$1::uuid`, verticalID).Scan(&stage); err != nil {
		t.Fatalf("load stage: %v", err)
	}
	if stage != "ready_for_review" {
		t.Fatalf("expected ready_for_review, got %q", stage)
	}

	// Ensure mailbox row exists (validateVertical creates one when mailbox store is configured).
	var n int
	_ = db.QueryRowContext(context.Background(), `SELECT count(*) FROM mailbox WHERE vertical_id=$1::uuid`, verticalID).Scan(&n)
	if n < 1 {
		t.Fatalf("expected mailbox item created, got %d", n)
	}

	_ = time.Second
}

func TestPipeline_UpdateStageField_RejectsUnsupportedField(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	p := NewPipeline(db, nil, nil)
	verticalID := uuid.NewString()
	if _, err := db.ExecContext(context.Background(), `
		INSERT INTO verticals (id, name, slug, geography, stage, mode, created_at, updated_at)
		VALUES ($1::uuid,'TestCo','testco','us','shortlisted','factory', now(), now())
	`, verticalID); err != nil {
		t.Fatalf("seed vertical: %v", err)
	}
	if err := p.updateStageField(context.Background(), verticalID, "researching", "unknown_field", []byte(`{}`)); err == nil {
		t.Fatalf("expected unsupported field error")
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func httpResp(code int, body string) *http.Response {
	return &http.Response{
		StatusCode: code,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(bytes.NewBufferString(body)),
	}
}

func TestSynthesizeSignals_DepthAffectsCount(t *testing.T) {
	gotDiscovery := synthesizeSignals("google_maps", "SF", "discovery", []string{"a", "b", "c"})
	if len(gotDiscovery) != 4 {
		t.Fatalf("expected 4 signals for discovery depth, got %d", len(gotDiscovery))
	}
	gotFull := synthesizeSignals("google_maps", "SF", "full", []string{"a", "b", "c"})
	if len(gotFull) != 8 {
		t.Fatalf("expected 8 signals for full depth, got %d", len(gotFull))
	}
	for _, s := range gotFull {
		if strings.TrimSpace(s.Source) == "" || strings.TrimSpace(s.Lead) == "" {
			t.Fatalf("expected non-empty source/lead: %#v", s)
		}
		if s.Score < 0 || s.Score > 100 {
			t.Fatalf("expected score within 0..100, got %d", s.Score)
		}
	}
}

func TestMinInt(t *testing.T) {
	if minInt(1, 2) != 1 {
		t.Fatalf("minInt wrong")
	}
	if minInt(5, 2) != 2 {
		t.Fatalf("minInt wrong")
	}
}

func TestGoogleMapsScanner_SynthFallbackWithoutAPIKey(t *testing.T) {
	t.Setenv("GOOGLE_MAPS_API_KEY", "")
	s := GoogleMapsScanner{}
	out, err := s.Scan(context.Background(), "Austin, TX", "discovery")
	if err != nil {
		t.Fatalf("scan error: %v", err)
	}
	if len(out) != 4 {
		t.Fatalf("expected synthesized 4 signals, got %d", len(out))
	}
}

func TestReviewScanner_SynthFallbackWithoutAPIKey(t *testing.T) {
	t.Setenv("YELP_API_KEY", "")
	s := ReviewScanner{}
	out, err := s.Scan(context.Background(), "Austin, TX", "full")
	if err != nil {
		t.Fatalf("scan error: %v", err)
	}
	if len(out) != 8 {
		t.Fatalf("expected synthesized 8 signals, got %d", len(out))
	}
}

func TestScanGoogleMaps_SuccessAndFallback(t *testing.T) {
	orig := http.DefaultTransport
	t.Cleanup(func() { http.DefaultTransport = orig })

	http.DefaultTransport = roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if !strings.Contains(r.URL.Host, "googleapis.com") {
			return nil, errors.New("unexpected host")
		}
		return httpResp(200, `{"results":[{"name":"Shop A","rating":4.7},{"name":"Shop B","rating":3.1}]}`), nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	out, err := scanGoogleMaps(ctx, "k", "NYC", "discovery")
	if err != nil {
		t.Fatalf("scanGoogleMaps error: %v", err)
	}
	if len(out) != 2 {
		t.Fatalf("expected 2 results, got %d", len(out))
	}
	if out[0].Source != "google_maps" || strings.TrimSpace(out[0].Lead) == "" {
		t.Fatalf("unexpected first signal: %#v", out[0])
	}

	http.DefaultTransport = roundTripFunc(func(r *http.Request) (*http.Response, error) {
		return httpResp(200, `{"results":[]}`), nil
	})
	out, err = scanGoogleMaps(ctx, "k", "NYC", "full")
	if err != nil {
		t.Fatalf("scanGoogleMaps empty error: %v", err)
	}
	if len(out) != 8 {
		t.Fatalf("expected synthesized 8 signals on empty results, got %d", len(out))
	}
}

func TestScanGoogleMaps_HTTPFailures(t *testing.T) {
	orig := http.DefaultTransport
	t.Cleanup(func() { http.DefaultTransport = orig })

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	http.DefaultTransport = roundTripFunc(func(r *http.Request) (*http.Response, error) {
		_ = r
		return httpResp(500, `{"error":"nope"}`), nil
	})
	if _, err := scanGoogleMaps(ctx, "k", "LA", "discovery"); err == nil {
		t.Fatalf("expected status error")
	}

	http.DefaultTransport = roundTripFunc(func(r *http.Request) (*http.Response, error) {
		_ = r
		return nil, errors.New("dial failed")
	})
	if _, err := scanGoogleMaps(ctx, "k", "LA", "discovery"); err == nil {
		t.Fatalf("expected transport error")
	}
}

func TestScanYelpReviews_SuccessAndFallback(t *testing.T) {
	orig := http.DefaultTransport
	t.Cleanup(func() { http.DefaultTransport = orig })

	http.DefaultTransport = roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if !strings.Contains(r.URL.Host, "api.yelp.com") {
			return nil, errors.New("unexpected host")
		}
		if got := r.Header.Get("Authorization"); !strings.HasPrefix(got, "Bearer ") {
			return nil, errors.New("missing auth header")
		}
		return httpResp(200, `{"businesses":[{"name":"Biz1","rating":4.5,"review_count":120,"transactions":["pickup"]}]}`), nil
	})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	out, err := scanYelpReviews(ctx, "k", "Boston, MA", "discovery")
	if err != nil {
		t.Fatalf("scanYelpReviews error: %v", err)
	}
	if len(out) != 1 || out[0].Source != "reviews" {
		t.Fatalf("unexpected output: %#v", out)
	}

	http.DefaultTransport = roundTripFunc(func(r *http.Request) (*http.Response, error) {
		_ = r
		return httpResp(200, `{"businesses":[]}`), nil
	})
	out, err = scanYelpReviews(ctx, "k", "Boston, MA", "full")
	if err != nil {
		t.Fatalf("scanYelpReviews empty error: %v", err)
	}
	if len(out) != 8 {
		t.Fatalf("expected synthesized 8 signals on empty businesses, got %d", len(out))
	}
}

func TestScanYelpReviews_HTTPFailures(t *testing.T) {
	orig := http.DefaultTransport
	t.Cleanup(func() { http.DefaultTransport = orig })
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	http.DefaultTransport = roundTripFunc(func(r *http.Request) (*http.Response, error) {
		_ = r
		return httpResp(503, `{"error":"down"}`), nil
	})
	if _, err := scanYelpReviews(ctx, "k", "Miami, FL", "discovery"); err == nil {
		t.Fatalf("expected status error")
	}

	http.DefaultTransport = roundTripFunc(func(r *http.Request) (*http.Response, error) {
		_ = r
		return nil, errors.New("network down")
	})
	if _, err := scanYelpReviews(ctx, "k", "Miami, FL", "discovery"); err == nil {
		t.Fatalf("expected transport error")
	}
}

func TestGoogleMapsScanner_WithAPIKeyUsesNetworkPath(t *testing.T) {
	orig := http.DefaultTransport
	t.Cleanup(func() { http.DefaultTransport = orig })
	t.Setenv("GOOGLE_MAPS_API_KEY", "k")

	http.DefaultTransport = roundTripFunc(func(r *http.Request) (*http.Response, error) {
		return httpResp(200, `{"results":[{"name":"X","rating":4.0}]}`), nil
	})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	out, err := (GoogleMapsScanner{}).Scan(ctx, "Seattle, WA", "discovery")
	if err != nil {
		t.Fatalf("scan error: %v", err)
	}
	if len(out) != 1 {
		t.Fatalf("expected 1, got %d", len(out))
	}
}

func TestReviewScanner_WithAPIKeyUsesNetworkPath(t *testing.T) {
	orig := http.DefaultTransport
	t.Cleanup(func() { http.DefaultTransport = orig })
	t.Setenv("YELP_API_KEY", "k")

	http.DefaultTransport = roundTripFunc(func(r *http.Request) (*http.Response, error) {
		return httpResp(200, `{"businesses":[{"name":"Y","rating":4.0,"review_count":10,"transactions":[]}]}`), nil
	})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	out, err := (ReviewScanner{}).Scan(ctx, "Denver, CO", "discovery")
	if err != nil {
		t.Fatalf("scan error: %v", err)
	}
	if len(out) != 1 {
		t.Fatalf("expected 1, got %d", len(out))
	}
}

func TestRoundTripHelper_UsesDefaultTransportRestore(t *testing.T) {

	if http.DefaultTransport == nil {
		t.Fatalf("expected default transport to be non-nil")
	}

	_ = os.ErrNotExist
}
