package testtiming

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/testplanning"
)

type FailureLedger struct {
	Version    int                 `json:"version"`
	Sources    []FailureSource     `json:"sources"`
	Historical []HistoricalFailure `json:"historical"`
	Records    []FailureRecord     `json:"records"`
}

type FailureSource struct {
	ID     string `json:"id"`
	URL    string `json:"url"`
	SHA256 string `json:"sha256"`
}

type HistoricalFailure struct {
	ID      string   `json:"id"`
	Sources []string `json:"sources"`
}

type FailureRecord struct {
	ID               string               `json:"id"`
	Package          string               `json:"package,omitempty"`
	Test             string               `json:"test,omitempty"`
	Job              string               `json:"job"`
	Signature        string               `json:"signature"`
	Backend          string               `json:"backend,omitempty"`
	GOOS             string               `json:"goos,omitempty"`
	Phenomenon       string               `json:"phenomenon"`
	Attribution      string               `json:"attribution"`
	Status           string               `json:"status"`
	Repair           string               `json:"repair"`
	FirstSeen        string               `json:"first_seen"`
	LastSeen         string               `json:"last_seen"`
	Tracking         string               `json:"tracking"`
	Review           string               `json:"review"`
	Sources          []string             `json:"sources"`
	ExcludePreflight bool                 `json:"exclude_preflight"`
	Rate             *FailureRate         `json:"rate,omitempty"`
	Observations     []FailureObservation `json:"observations"`
}

type FailureRate struct {
	Failures int    `json:"failures"`
	Attempts int    `json:"attempts"`
	Context  string `json:"context"`
	Evidence string `json:"evidence"`
}

type FailureObservation struct {
	RunID       int64  `json:"run_id"`
	Attempt     int    `json:"attempt"`
	JobID       int64  `json:"job_id"`
	Source      string `json:"source"`
	Attribution string `json:"attribution"`
	Review      string `json:"review"`
}

type ObservedFailure struct {
	RunID      int64  `json:"run_id"`
	Attempt    int    `json:"attempt"`
	JobID      int64  `json:"job_id"`
	HeadSHA    string `json:"head_sha"`
	Source     string `json:"source"`
	Package    string `json:"package,omitempty"`
	Test       string `json:"test,omitempty"`
	Job        string `json:"job"`
	Detail     string `json:"detail"`
	GOOS       string `json:"goos,omitempty"`
	Problem    string `json:"problem,omitempty"`
	Conclusion string `json:"job_conclusion,omitempty"`
}

type FailureMatch struct {
	ObservedFailure
	Record      string       `json:"record,omitempty"`
	Phenomenon  string       `json:"phenomenon"`
	Attribution string       `json:"attribution"`
	Review      string       `json:"review,omitempty"`
	Tracking    string       `json:"tracking,omitempty"`
	Status      string       `json:"status,omitempty"`
	Repair      string       `json:"repair,omitempty"`
	Rate        *FailureRate `json:"historical_rate,omitempty"`
}

func ReadFailureLedger(r io.Reader) (FailureLedger, error) {
	var ledger FailureLedger
	decoder := json.NewDecoder(r)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&ledger); err != nil {
		return ledger, err
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return ledger, fmt.Errorf("trailing failure-ledger data")
	}
	return ledger, ledger.Validate()
}

func (l FailureLedger) Validate() error {
	if l.Version != 1 || len(l.Sources) == 0 {
		return fmt.Errorf("unversioned or unsourced failure ledger")
	}
	sources := map[string]bool{}
	for _, source := range l.Sources {
		if source.ID == "" || sources[source.ID] || !reviewURL(source.URL) || !hexIdentity(source.SHA256, 32) {
			return fmt.Errorf("invalid or duplicate failure source %q", source.ID)
		}
		sources[source.ID] = true
	}
	ids := map[string]bool{}
	for _, item := range l.Historical {
		if ids[item.ID] || item.ID == "" || !failureSourcesPresent(item.Sources, sources) {
			return fmt.Errorf("invalid or duplicate historical record %q", item.ID)
		}
		ids[item.ID] = true
	}
	for _, record := range l.Records {
		if ids[record.ID] {
			return fmt.Errorf("duplicate failure record %s", record.ID)
		}
		ids[record.ID] = true
		if err := record.validate(sources); err != nil {
			return fmt.Errorf("failure record %s: %w", record.ID, err)
		}
	}
	return nil
}

func (r FailureRecord) validate(sources map[string]bool) error {
	if r.ID == "" || r.Job == "" || r.Signature == "" || !reviewURL(r.Review) || !reviewURL(r.Tracking) || !failureSourcesPresent(r.Sources, sources) {
		return fmt.Errorf("missing exact identity or reviewed provenance")
	}
	if !slices.Contains([]string{"flake", "timing-budget", "runner-infra", "unknown-variance"}, r.Phenomenon) || !failureAttribution(r.Attribution) || !slices.Contains([]string{"open", "fixed", "quarantined"}, r.Status) || r.Repair == "" {
		return fmt.Errorf("unknown phenomenon, attribution, status or repair disposition")
	}
	first, err := time.Parse(time.DateOnly, r.FirstSeen)
	if err != nil {
		return err
	}
	last, err := time.Parse(time.DateOnly, r.LastSeen)
	if err != nil || last.Before(first) {
		return fmt.Errorf("invalid observation interval")
	}
	if r.ExcludePreflight && (r.Phenomenon != "flake" || r.Package == "" || r.Test == "" || r.GOOS == "") {
		return fmt.Errorf("preflight exclusion lacks exact flaky execution variant")
	}
	if r.Rate != nil && (r.Rate.Attempts <= 0 || r.Rate.Failures < 0 || r.Rate.Failures > r.Rate.Attempts || r.Rate.Context == "" || !reviewURL(r.Rate.Evidence)) {
		return fmt.Errorf("invalid or unscoped observed rate")
	}
	return r.validateObservations()
}

func (r FailureRecord) validateObservations() error {
	seen := map[string]bool{}
	for _, observation := range r.Observations {
		key := fmt.Sprintf("%d/%d/%d", observation.RunID, observation.Attempt, observation.JobID)
		if observation.RunID <= 0 || observation.Attempt <= 0 || observation.JobID <= 0 || !hexIdentity(observation.Source, 20) || !failureAttribution(observation.Attribution) || !reviewURL(observation.Review) || seen[key] {
			return fmt.Errorf("invalid or duplicate reviewed observation %s", key)
		}
		seen[key] = true
	}
	return nil
}

func failureSourcesPresent(refs []string, sources map[string]bool) bool {
	if len(refs) == 0 {
		return false
	}
	seen := map[string]bool{}
	for _, ref := range refs {
		if !sources[ref] || seen[ref] {
			return false
		}
		seen[ref] = true
	}
	return true
}

func failureAttribution(value string) bool {
	return slices.Contains([]string{"PR-owned", "non-PR-proven", "unknown"}, value)
}

func reviewURL(value string) bool {
	u, err := url.Parse(value)
	if err != nil || u.Scheme != "https" || u.Host != "github.com" || u.User != nil || u.RawQuery != "" {
		return false
	}
	path := strings.TrimPrefix(u.Path, "/division-sh/swarm/")
	family, id, ok := strings.Cut(path, "/")
	if family == "actions" {
		family, id, ok = strings.Cut(id, "/")
		if family != "runs" {
			return false
		}
	} else if family != "issues" && family != "pull" {
		return false
	}
	number, err := strconv.ParseInt(id, 10, 64)
	return ok && err == nil && number > 0
}

func hexIdentity(value string, size int) bool {
	raw, err := hex.DecodeString(value)
	return err == nil && len(raw) == size
}

func (l FailureLedger) Match(f ObservedFailure) (FailureMatch, error) {
	result := FailureMatch{ObservedFailure: f, Attribution: "unknown", Phenomenon: "unknown-variance"}
	if err := l.Validate(); err != nil {
		return result, err
	}
	if f.RunID <= 0 || f.Attempt <= 0 || f.JobID <= 0 || !hexIdentity(f.HeadSHA, 20) || f.Problem != "" || !hexIdentity(f.Source, 20) {
		return result, nil
	}
	for _, record := range l.Records {
		if record.Job != f.Job || record.Package != f.Package || record.Test != f.Test || !strings.Contains(f.Detail, record.Signature) || (record.GOOS != "" && record.GOOS != f.GOOS) {
			continue
		}
		if result.Record != "" {
			return result, fmt.Errorf("ambiguous manifestation match for %s/%s", f.Package, f.Test)
		}
		result.Record, result.Phenomenon, result.Rate = record.ID, record.Phenomenon, record.Rate
		result.Tracking, result.Status, result.Repair = record.Tracking, record.Status, record.Repair
		for _, observation := range record.Observations {
			if observation.RunID == f.RunID && observation.Attempt == f.Attempt && observation.JobID == f.JobID && observation.Source == f.Source {
				result.Attribution, result.Review = observation.Attribution, observation.Review
			}
		}
	}
	return result, nil
}

func ValidatePreflightEligibility(plan testplanning.PreflightPlan, ledger FailureLedger) error {
	if err := plan.Validate(); err != nil {
		return err
	}
	if err := ledger.Validate(); err != nil {
		return err
	}
	for _, record := range ledger.Records {
		if !record.ExcludePreflight || record.GOOS != plan.BuildContext.GOOS {
			continue
		}
		root, _, _ := strings.Cut(record.Test, "/")
		for _, pkg := range plan.Packages {
			for _, selected := range pkg.Required {
				if selected.Package == record.Package && selected.Name == root {
					return fmt.Errorf("finite preflight selection includes known-flaky variant %s; review whole-root membership, do not skip its leaf", record.ID)
				}
			}
		}
	}
	return nil
}
