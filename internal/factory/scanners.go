package factory

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

type Signal struct {
	Source string `json:"source"`
	Lead   string `json:"lead"`
	Score  int    `json:"score"`
}

type Scanner interface {
	Name() string
	Scan(ctx context.Context, geography, depth string) ([]Signal, error)
}

type GoogleMapsScanner struct{}
type InstagramScanner struct{}
type ReviewScanner struct{}

func (GoogleMapsScanner) Name() string { return "google_maps" }
func (InstagramScanner) Name() string  { return "instagram" }
func (ReviewScanner) Name() string     { return "reviews" }

func (GoogleMapsScanner) Scan(ctx context.Context, geography, depth string) ([]Signal, error) {
	if apiKey := strings.TrimSpace(os.Getenv("GOOGLE_MAPS_API_KEY")); apiKey != "" {
		return scanGoogleMaps(ctx, apiKey, geography, depth)
	}
	return synthesizeSignals("google_maps", geography, depth, []string{
		"pet grooming", "dental clinic", "home cleaning", "hvac", "auto detail",
	}), nil
}

func (InstagramScanner) Scan(_ context.Context, geography, depth string) ([]Signal, error) {
	return synthesizeSignals("instagram", geography, depth, []string{
		"beauty studio", "fitness coach", "pet services", "local food prep", "events planner",
	}), nil
}

func (ReviewScanner) Scan(ctx context.Context, geography, depth string) ([]Signal, error) {
	if apiKey := strings.TrimSpace(os.Getenv("YELP_API_KEY")); apiKey != "" {
		return scanYelpReviews(ctx, apiKey, geography, depth)
	}
	return synthesizeSignals("reviews", geography, depth, []string{
		"long wait times", "no-show issues", "manual scheduling pain", "slow response", "payment confusion",
	}), nil
}

func synthesizeSignals(source, geography, depth string, seeds []string) []Signal {
	n := 4
	if strings.EqualFold(strings.TrimSpace(depth), "full") {
		n = 8
	}
	out := make([]Signal, 0, n)
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("%s|%s|%d|%s", source, geography, i, seeds[i%len(seeds)])
		h := sha1.Sum([]byte(strings.ToLower(key)))
		hexed := hex.EncodeToString(h[:3])
		score := 50
		for _, r := range hexed {
			score += int(r) % 6
		}
		if score > 100 {
			score = 100
		}
		out = append(out, Signal{
			Source: source,
			Lead:   fmt.Sprintf("%s in %s", seeds[i%len(seeds)], geography),
			Score:  score,
		})
	}
	return out
}

func scanGoogleMaps(ctx context.Context, apiKey, geography, depth string) ([]Signal, error) {
	q := "small local services in " + strings.TrimSpace(geography)
	u := "https://maps.googleapis.com/maps/api/place/textsearch/json?query=" +
		url.QueryEscape(q) + "&key=" + url.QueryEscape(apiKey)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("google maps api status %d", resp.StatusCode)
	}
	var payload struct {
		Results []struct {
			Name   string  `json:"name"`
			Rating float64 `json:"rating"`
		} `json:"results"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return nil, err
	}
	limit := 5
	if strings.EqualFold(strings.TrimSpace(depth), "full") {
		limit = 10
	}
	out := make([]Signal, 0, limit)
	for i, r := range payload.Results {
		if i >= limit {
			break
		}
		score := int(r.Rating * 20)
		if score < 40 {
			score = 40
		}
		if score > 100 {
			score = 100
		}
		out = append(out, Signal{
			Source: "google_maps",
			Lead:   strings.TrimSpace(r.Name),
			Score:  score,
		})
	}
	if len(out) == 0 {
		return synthesizeSignals("google_maps", geography, depth, []string{"pet grooming", "dental clinic", "home cleaning"}), nil
	}
	return out, nil
}

func scanYelpReviews(ctx context.Context, apiKey, geography, depth string) ([]Signal, error) {
	u := "https://api.yelp.com/v3/businesses/search?location=" + url.QueryEscape(strings.TrimSpace(geography)) + "&categories=localservices&limit=20"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+apiKey)
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("yelp api status %d", resp.StatusCode)
	}
	var payload struct {
		Businesses []struct {
			Name         string   `json:"name"`
			Rating       float64  `json:"rating"`
			ReviewCount  int      `json:"review_count"`
			Transactions []string `json:"transactions"`
		} `json:"businesses"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return nil, err
	}
	limit := 6
	if strings.EqualFold(strings.TrimSpace(depth), "full") {
		limit = 12
	}
	out := make([]Signal, 0, limit)
	for i, b := range payload.Businesses {
		if i >= limit {
			break
		}
		score := int((b.Rating * 15) + float64(minInt(b.ReviewCount, 200))/4.0)
		if score < 35 {
			score = 35
		}
		if score > 100 {
			score = 100
		}
		out = append(out, Signal{
			Source: "reviews",
			Lead:   strings.TrimSpace(b.Name),
			Score:  score,
		})
	}
	if len(out) == 0 {
		return synthesizeSignals("reviews", geography, depth, []string{"long wait times", "no-show issues", "manual scheduling pain"}), nil
	}
	return out, nil
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
