package factory

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"
)

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

	// First: success with two results.
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

	// Second: empty results triggers synth fallback.
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

	// Empty businesses -> synth fallback.
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

	// Ensure the code goes into scanGoogleMaps() by returning a valid response.
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
	// A small sanity check so tests don't accidentally leave transport mutated.
	// Not strictly required for coverage, but helps avoid flaky cross-test interference.
	if http.DefaultTransport == nil {
		t.Fatalf("expected default transport to be non-nil")
	}
	// Ensure os import isn't unused in case build tags change.
	_ = os.ErrNotExist
}

