package cataloge2e

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	operatorread "github.com/division-sh/swarm/internal/operatorread"

	"github.com/division-sh/swarm/internal/events"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/testcatalog"
	"github.com/division-sh/swarm/internal/testutil/replayconformance"
	"github.com/google/uuid"
)

const (
	catalogBarrierRunTerminal         = "run_terminal"
	catalogBarrierAutomaticEventCount = "automatic_event_count"
	catalogReplayTranscriptVersion    = replayconformance.TranscriptVersion
	catalogReplayInputRootIngress     = replayconformance.InputRootIngress
)

type catalogTranscriptBarrier struct {
	Kind    string `yaml:"kind" json:"kind,omitempty"`
	Event   string `yaml:"event" json:"event,omitempty"`
	Minimum int    `yaml:"minimum" json:"minimum,omitempty"`
}

func (b catalogTranscriptBarrier) normalized() catalogTranscriptBarrier {
	b.Kind = strings.TrimSpace(b.Kind)
	b.Event = strings.TrimSpace(b.Event)
	return b
}

func (b catalogTranscriptBarrier) validate() error {
	b = b.normalized()
	switch b.Kind {
	case "":
		if b.Event != "" || b.Minimum != 0 {
			return fmt.Errorf("empty transcript barrier has fields")
		}
	case catalogBarrierRunTerminal:
		if b.Event != "" || b.Minimum != 0 {
			return fmt.Errorf("run-terminal transcript barrier has unsupported fields")
		}
	case catalogBarrierAutomaticEventCount:
		if b.Event == "" || b.Minimum < 1 {
			return fmt.Errorf("automatic-event-count transcript barrier requires event and positive minimum")
		}
	default:
		return fmt.Errorf("transcript barrier kind %q is unsupported", b.Kind)
	}
	return nil
}

var catalogReplayCleanExclusions = map[string]string{
	"tests/tier12-runtime-fork/test-non-agent-replay-fail-closed":     "selected-contract fork",
	"tests/tier12-runtime-fork/test-selected-contract-fork-execution": "selected-contract fork",
	"tests/tier12-runtime-tools/test-flow-data-access":                "direct tool execution",
	"tests/tier5-flow-lifecycle/test-template-no-boot-instance":       "boot-only runtime",
	"tests/tier6-event-loop/test-cross-entity-concurrent":             "exact target lifecycle identity tracked by #2132",
}

type catalogExecutionTranscript struct {
	version            string
	platformSpecDigest string
	bundleHash         string
	bundleSource       string
	fixtureName        string
	runID              string
	runtimeStart       bool
	expected           catalogExpectedDocument
	groups             []catalogTranscriptGroup
	agentFixtures      agentFixtureDoc
	frozen             []byte
}

type catalogTranscriptGroup struct {
	barrierBefore catalogTranscriptBarrier
	concurrent    bool
	steps         []catalogTriggerStep
}

type catalogExecutionResult struct {
	outcomes   []catalogStepOutcome
	projection []byte
}

type catalogStepOutcome struct {
	EventID  string                    `json:"event_id"`
	Accepted bool                      `json:"accepted"`
	Failure  *runtimefailures.Envelope `json:"failure,omitempty"`
	Receipt  *catalogReceiptOutcome    `json:"receipt,omitempty"`
}

type catalogReceiptOutcome struct {
	SubscriberID string                    `json:"subscriber_id"`
	Outcome      string                    `json:"outcome"`
	SideEffects  json.RawMessage           `json:"side_effects,omitempty"`
	Failure      *runtimefailures.Envelope `json:"failure,omitempty"`
}

func buildCatalogExecutionTranscript(t testing.TB, fixture testcatalog.Fixture) *catalogExecutionTranscript {
	t.Helper()
	var expected catalogExpectedDocument
	loadYAML(t, filepath.Join(fixture.Root, "expected.yaml"), &expected)
	if expected.Trigger.Boot || strings.TrimSpace(expected.Expected.BootResult) != "" {
		t.Fatalf("boot-only fixture %s cannot construct replay-clean transcript", fixture.Name)
	}

	transcript := &catalogExecutionTranscript{
		fixtureName:  fixture.Name,
		runID:        catalogRuntimeRunID,
		runtimeStart: true,
		expected:     cloneCatalogExpectedDocument(t, expected),
	}
	fixturePath := filepath.Join(fixture.Root, "fixtures.yaml")
	if _, err := os.Stat(fixturePath); err == nil {
		loadYAML(t, fixturePath, &transcript.agentFixtures)
	} else if !os.IsNotExist(err) {
		t.Fatalf("stat %s: %v", fixturePath, err)
	}

	steps := expected.triggerSequence()
	if len(expected.Trigger.Concurrent) > 0 {
		steps = append([]catalogTriggerStep(nil), expected.Trigger.Concurrent...)
		transcript.groups = append(transcript.groups, catalogTranscriptGroup{concurrent: true, steps: steps})
	} else {
		for _, step := range steps {
			transcript.groups = append(transcript.groups, catalogTranscriptGroup{
				barrierBefore: step.BarrierBefore.normalized(),
				steps:         []catalogTriggerStep{step},
			})
		}
	}

	bundle := loadFixtureBundle(t, fixture.Root)
	bundleHash, err := runtimecontracts.BundleHash(bundle)
	if err != nil {
		t.Fatalf("hash replay-clean bundle %s: %v", fixture.RelativePath, err)
	}
	platformSpecDigest, err := catalogReplayPlatformSpecDigest(repoRootFromCatalogE2E(t))
	if err != nil {
		t.Fatalf("hash replay-clean platform spec: %v", err)
	}
	transcript.version = catalogReplayTranscriptVersion
	transcript.platformSpecDigest = platformSpecDigest
	transcript.bundleHash = bundleHash
	transcript.bundleSource = "catalog-fixture:" + fixture.RelativePath
	baseTime := time.Now().UTC().Truncate(time.Microsecond)
	stepIndex := 0
	for groupIndex := range transcript.groups {
		group := &transcript.groups[groupIndex]
		group.barrierBefore = group.barrierBefore.normalized()
		if err := group.barrierBefore.validate(); err != nil {
			t.Fatalf("fixture %s has invalid transcript barrier: %v", fixture.Name, err)
		}
		for index := range group.steps {
			step := &group.steps[index]
			step.Event = strings.TrimSpace(step.Event)
			if step.Event == "" {
				t.Fatalf("fixture %s transcript step %d has no event", fixture.Name, stepIndex)
			}
			step.Payload = cloneStringAnyMap(step.Payload)
			step.inputKind = catalogReplayInputRootIngress
			step.eventID = uuid.NewSHA1(uuid.NameSpaceURL, []byte(fmt.Sprintf("swarm/catalog-replay/%s/%d", fixture.Root, stepIndex))).String()
			step.createdAt = baseTime.Add(time.Duration(stepIndex) * time.Microsecond)
			if strings.TrimSpace(step.sourceAgent) == "" {
				step.sourceAgent = "cataloge2e"
				step.excludeFromEmitted = true
			}
			stepIndex++
		}
	}
	transcript.frozen = catalogTranscriptBytes(t, transcript)
	return transcript
}

func cloneCatalogExpectedDocument(t testing.TB, in catalogExpectedDocument) catalogExpectedDocument {
	t.Helper()
	raw, err := json.Marshal(in)
	if err != nil {
		t.Fatalf("marshal catalog expected document: %v", err)
	}
	var out catalogExpectedDocument
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("clone catalog expected document: %v", err)
	}
	return out
}

func catalogTranscriptBytes(t testing.TB, transcript *catalogExecutionTranscript) []byte {
	t.Helper()
	type step struct {
		InputKind          string         `json:"input_kind"`
		Event              string         `json:"event"`
		Payload            map[string]any `json:"payload,omitempty"`
		ErrorContains      string         `json:"error_contains,omitempty"`
		EventID            string         `json:"event_id"`
		CreatedAt          time.Time      `json:"created_at"`
		SourceAgent        string         `json:"source_agent"`
		ExcludeFromEmitted bool           `json:"exclude_from_emitted"`
	}
	type group struct {
		BarrierBefore catalogTranscriptBarrier `json:"barrier_before"`
		Concurrent    bool                     `json:"concurrent"`
		Steps         []step                   `json:"steps"`
	}
	manifest := struct {
		Version            string                  `json:"version"`
		PlatformSpecDigest string                  `json:"platform_spec_digest"`
		BundleHash         string                  `json:"bundle_hash"`
		BundleSource       string                  `json:"bundle_source"`
		FixtureName        string                  `json:"fixture_name"`
		RunID              string                  `json:"run_id"`
		RuntimeStart       bool                    `json:"runtime_start"`
		Expected           catalogExpectedDocument `json:"expected"`
		Groups             []group                 `json:"groups"`
		AgentFixtures      agentFixtureDoc         `json:"agent_fixtures"`
	}{
		Version: transcript.version, PlatformSpecDigest: transcript.platformSpecDigest,
		BundleHash: transcript.bundleHash, BundleSource: transcript.bundleSource,
		FixtureName: transcript.fixtureName, RunID: transcript.runID, RuntimeStart: transcript.runtimeStart,
		Expected: transcript.expected, AgentFixtures: transcript.agentFixtures,
	}
	for _, inputGroup := range transcript.groups {
		projected := group{BarrierBefore: inputGroup.barrierBefore, Concurrent: inputGroup.concurrent}
		for _, input := range inputGroup.steps {
			projected.Steps = append(projected.Steps, step{
				InputKind: input.inputKind, Event: input.Event, Payload: input.Payload,
				ErrorContains: input.ErrorContains, EventID: input.eventID, CreatedAt: input.createdAt,
				SourceAgent: input.sourceAgent, ExcludeFromEmitted: input.excludeFromEmitted,
			})
		}
		manifest.Groups = append(manifest.Groups, projected)
	}
	raw, err := json.Marshal(manifest)
	if err != nil {
		t.Fatalf("marshal immutable catalog transcript: %v", err)
	}
	return raw
}

func catalogReplayPlatformSpecDigest(repoRoot string) (string, error) {
	raw, err := os.ReadFile(filepath.Join(repoRoot, "platform-spec.yaml"))
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(raw)
	return "sha256:" + hex.EncodeToString(sum[:]), nil
}

func requireCatalogTranscriptIdentity(t testing.TB, fixtureRoot string, bundle *runtimecontracts.WorkflowContractBundle, transcript *catalogExecutionTranscript) {
	t.Helper()
	if err := validateCatalogTranscriptIdentity(repoRootFromCatalogE2E(t), fixtureRoot, bundle, transcript); err != nil {
		t.Fatalf("catalog replay transcript admission: %v", err)
	}
}

func validateCatalogTranscriptIdentity(repoRoot, fixtureRoot string, bundle *runtimecontracts.WorkflowContractBundle, transcript *catalogExecutionTranscript) error {
	if transcript == nil {
		return fmt.Errorf("transcript is required")
	}
	wantSpec, err := catalogReplayPlatformSpecDigest(repoRoot)
	if err != nil {
		return fmt.Errorf("read platform spec identity: %w", err)
	}
	wantBundle, err := runtimecontracts.BundleHash(bundle)
	if err != nil {
		return fmt.Errorf("read bundle identity: %w", err)
	}
	relative, err := filepath.Rel(repoRoot, fixtureRoot)
	if err != nil {
		return fmt.Errorf("resolve bundle source identity: %w", err)
	}
	wantSource := "catalog-fixture:" + filepath.ToSlash(relative)
	if err := replayconformance.ValidateIdentity(
		replayconformance.Identity{
			Version:            transcript.version,
			PlatformSpecDigest: transcript.platformSpecDigest,
			BundleHash:         transcript.bundleHash,
			BundleSource:       transcript.bundleSource,
			RunID:              transcript.runID,
		},
		replayconformance.Identity{
			Version:            replayconformance.TranscriptVersion,
			PlatformSpecDigest: wantSpec,
			BundleHash:         wantBundle,
			BundleSource:       wantSource,
			RunID:              catalogRuntimeRunID,
		},
	); err != nil {
		return err
	}
	for groupIndex, group := range transcript.groups {
		if len(group.steps) == 0 {
			return fmt.Errorf("transcript input group %d is empty", groupIndex)
		}
		if err := group.barrierBefore.validate(); err != nil {
			return fmt.Errorf("transcript input group %d barrier: %w", groupIndex, err)
		}
	}
	_, err = catalogReplayRootInputs(transcript)
	return err
}

func (transcript *catalogExecutionTranscript) requireUnchanged(t testing.TB) {
	t.Helper()
	if got := string(catalogTranscriptBytes(t, transcript)); got != string(transcript.frozen) {
		t.Fatalf("fixture %s mutated its immutable execution transcript", transcript.fixtureName)
	}
}

func (transcript *catalogExecutionTranscript) observationBoundary() time.Time {
	var first time.Time
	for _, group := range transcript.groups {
		for _, step := range group.steps {
			if first.IsZero() || step.createdAt.Before(first) {
				first = step.createdAt
			}
		}
	}
	if first.IsZero() {
		return time.Now().UTC().Add(-time.Second)
	}
	return first.UTC().Add(-time.Second)
}

func executeCatalogTranscript(t *testing.T, fixture testcatalog.Fixture, backend catalogRuntimeBackend, transcript *catalogExecutionTranscript) (*runtimeHarness, catalogExecutionResult) {
	t.Helper()
	transcript.requireUnchanged(t)
	h := newRuntimeHarnessFromTranscript(t, fixture.Root, backend, transcript.runtimeStart, transcript)
	h.seedEntityFields(transcript.expected)
	result := catalogExecutionResult{}
	for groupIndex, group := range transcript.groups {
		switch group.barrierBefore.Kind {
		case "":
		case catalogBarrierRunTerminal:
			h.waitForRunTerminal(catalogRuntimePublishTimeout)
		case catalogBarrierAutomaticEventCount:
			h.waitForCatalogAutomaticEventCount(group.barrierBefore.Event, group.barrierBefore.Minimum, catalogRuntimePublishTimeout)
		default:
			t.Fatalf("unsupported admitted transcript barrier %q", group.barrierBefore.Kind)
		}
		if group.concurrent {
			h.publishConcurrentAndWait(group.steps, catalogRuntimePublishTimeout)
			for _, step := range group.steps {
				result.outcomes = append(result.outcomes, h.captureCatalogStepOutcome(step, nil))
			}
		} else {
			for _, step := range group.steps {
				err := h.publishRuntimeEventResultForStep(step, catalogRuntimePublishTimeout, true)
				wantErr := strings.TrimSpace(step.ErrorContains)
				if wantErr == "" && err != nil {
					t.Fatalf("Publish(%s): %v", step.Event, err)
				}
				if wantErr != "" && (err == nil || !strings.Contains(err.Error(), wantErr)) {
					t.Fatalf("Publish(%s) error = %v, want error containing %q", step.Event, err, wantErr)
				}
				result.outcomes = append(result.outcomes, h.captureCatalogStepOutcome(step, err))
			}
		}
		if groupIndex+1 >= len(transcript.groups) || transcript.groups[groupIndex+1].barrierBefore.Kind != catalogBarrierAutomaticEventCount {
			h.waitForCatalogStoreQuiescence(catalogRuntimePublishTimeout)
		}
	}
	h.waitForExpectedEmittedEvents(transcript.expected, catalogRuntimePublishTimeout)
	h.waitForCatalogStoreQuiescence(catalogRuntimePublishTimeout)
	assertCatalogRuntimeOutcome(t, h, transcript.expected)
	assertCatalogReplayFixtureOutcome(t, fixture, h)
	h.shutdown()
	projection, err := h.catalogCanonicalProjection(transcript)
	if err != nil {
		t.Fatalf("build catalog canonical projection: %v", err)
	}
	result.projection = projection
	transcript.requireUnchanged(t)
	return h, result
}

func (h *runtimeHarness) waitForCatalogAutomaticEventCount(eventName string, minimum int, timeout time.Duration) {
	h.t.Helper()
	eventName = strings.TrimSpace(eventName)
	if eventName == "" || minimum < 1 {
		h.t.Fatalf("automatic event-count barrier requires event and positive minimum")
	}
	ctx, cancel := context.WithTimeout(h.ctx, timeout)
	defer cancel()
	lister, err := h.catalogOperatorEventLister()
	if err != nil {
		h.t.Fatal(err)
	}
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		h.mu.Lock()
		authoredIDs := cloneCatalogStringSet(h.publishedIDs)
		h.mu.Unlock()
		count := 0
		opts := operatorread.OperatorEventListOptions{
			Filter: operatorread.OperatorEventListFilter{RunID: catalogRuntimeRunID, EventName: eventName},
			Limit:  minimum,
		}
		for {
			page, err := lister.ListOperatorEvents(ctx, opts)
			if err != nil {
				h.t.Fatalf("load automatic events for transcript barrier: %v", err)
			}
			for _, full := range page.Events {
				eventID := strings.TrimSpace(full.EventID)
				if _, authored := authoredIDs[eventID]; authored {
					continue
				}
				event, err := full.EventSnapshot()
				if err != nil {
					h.t.Fatalf("read automatic event %s for transcript barrier: %v", eventID, err)
				}
				if event.AdmissionClass() != events.EventAdmissionRootIngress {
					count++
				}
			}
			if count >= minimum || strings.TrimSpace(page.NextCursor) == "" {
				break
			}
			opts.Cursor = page.NextCursor
		}
		if count >= minimum {
			return
		}
		select {
		case <-ctx.Done():
			h.t.Fatalf("wait for at least %d automatic %s events: %v (observed=%d)", minimum, eventName, ctx.Err(), count)
		case <-ticker.C:
		}
	}
}

func reopenCatalogTranscript(t *testing.T, fixture testcatalog.Fixture, transcript *catalogExecutionTranscript, h *runtimeHarness, baseline catalogExecutionResult) {
	t.Helper()
	transcript.requireUnchanged(t)
	reopened := h.reopenFromTranscript(transcript)
	defer reopened.shutdown()
	reopened.waitForExpectedEmittedEvents(transcript.expected, catalogRuntimePublishTimeout)
	reopened.waitForCatalogStoreQuiescence(catalogRuntimePublishTimeout)
	assertCatalogRuntimeOutcome(t, reopened, transcript.expected)
	assertCatalogReplayFixtureOutcome(t, fixture, reopened)
	reopened.shutdown()
	projection, err := reopened.catalogCanonicalProjection(transcript)
	if err != nil {
		t.Fatalf("build reopened catalog canonical projection: %v", err)
	}
	if string(projection) != string(baseline.projection) {
		t.Fatalf("%s reopen changed canonical projection\nbefore: %s\nafter: %s", h.backend, baseline.projection, projection)
	}
	transcript.requireUnchanged(t)
}

func assertCatalogReplayFixtureOutcome(t testing.TB, fixture testcatalog.Fixture, h *runtimeHarness) {
	t.Helper()
	if fixture.HasClaim("catalog.runtime.flow_composition") {
		assertDynamicFlowInstanceReceiverSelectedNodeDelivery(t, h, "worker/work.assign", "worker/w-001", "task-handler")
	}
}

func (h *runtimeHarness) waitForCatalogStoreQuiescence(timeout time.Duration) {
	h.t.Helper()
	ctx, cancel := context.WithTimeout(h.ctx, timeout)
	defer cancel()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		var (
			report operatorread.RunDebugReport
			err    error
		)
		if h.pg != nil {
			report, err = h.pg.LoadRunDebugReport(ctx, catalogRuntimeRunID, operatorread.RunDebugQueryOptions{})
		} else if h.sqlite != nil {
			report, err = h.sqlite.LoadRunDebugReport(ctx, catalogRuntimeRunID, operatorread.RunDebugQueryOptions{})
		} else {
			h.t.Fatal("catalog selected store is required for quiescence")
		}
		if err != nil {
			h.t.Fatalf("load catalog test quiescence: %v", err)
		}
		if report.TestQuiescence.Ready {
			return
		}
		select {
		case <-ctx.Done():
			h.t.Fatalf("wait for catalog test quiescence: %v (last=%#v)", ctx.Err(), report.TestQuiescence)
		case <-ticker.C:
		}
	}
}

func (h *runtimeHarness) captureCatalogStepOutcome(step catalogTriggerStep, publishErr error) catalogStepOutcome {
	out := catalogStepOutcome{EventID: step.eventID, Accepted: publishErr == nil}
	if publishErr != nil {
		failure := runtimefailures.FromError(publishErr, "cataloge2e", "publish_transcript_step").Failure
		out.Failure = &failure
	}
	receipt, err := h.loadCatalogReceipt(step.eventID)
	if err != nil {
		h.t.Fatalf("load transcript receipt for %s: %v", step.eventID, err)
	}
	out.Receipt = receipt
	return out
}

func (h *runtimeHarness) loadCatalogReceipt(eventID string) (*catalogReceiptOutcome, error) {
	var subscriberID, outcome string
	var sideEffects, rawFailure []byte
	query := `
		SELECT subscriber_id, outcome, COALESCE(side_effects::text, ''), COALESCE(failure, 'null'::jsonb)
		FROM event_receipts
		WHERE event_id = $1::uuid
		  AND subscriber_type = 'platform'
		  AND (subscriber_id = 'pipeline' OR subscriber_id LIKE 'pipeline:%')
		ORDER BY processed_at DESC
		LIMIT 1`
	if h.backend == catalogBackendSQLite {
		query = `
			SELECT subscriber_id, outcome, COALESCE(side_effects, ''), COALESCE(failure, 'null')
			FROM event_receipts
			WHERE event_id = ?
			  AND subscriber_type = 'platform'
			  AND (subscriber_id = 'pipeline' OR subscriber_id LIKE 'pipeline:%')
			ORDER BY processed_at DESC
			LIMIT 1`
	}
	err := h.db.QueryRowContext(h.ctx, query, strings.TrimSpace(eventID)).Scan(&subscriberID, &outcome, &sideEffects, &rawFailure)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	out := &catalogReceiptOutcome{SubscriberID: strings.TrimSpace(subscriberID), Outcome: strings.TrimSpace(strings.ToLower(outcome))}
	if len(sideEffects) > 0 && string(sideEffects) != "null" {
		canonical, err := canonicalJSONBytes(sideEffects)
		if err != nil {
			return nil, fmt.Errorf("canonicalize receipt side effects: %w", err)
		}
		out.SideEffects = canonical
	}
	if len(rawFailure) > 0 && string(rawFailure) != "null" && string(rawFailure) != "{}" {
		failure, err := runtimefailures.UnmarshalEnvelope(rawFailure)
		if err != nil {
			return nil, fmt.Errorf("decode receipt failure: %w", err)
		}
		out.Failure = &failure
	}
	return out, nil
}

func (h *runtimeHarness) catalogCanonicalProjection(transcript *catalogExecutionTranscript) ([]byte, error) {
	eventsByID, err := h.catalogOperatorEvents()
	if err != nil {
		return nil, err
	}
	roots, err := catalogReplayRootInputs(transcript)
	if err != nil {
		return nil, err
	}
	return replayconformance.Project(eventsByID, transcript.runID, roots)
}

func catalogReplayRootInputs(transcript *catalogExecutionTranscript) ([]replayconformance.RootInput, error) {
	if transcript == nil {
		return nil, fmt.Errorf("catalog replay transcript is required")
	}
	roots := []replayconformance.RootInput{}
	for _, group := range transcript.groups {
		for _, step := range group.steps {
			payload, err := json.Marshal(step.Payload)
			if err != nil {
				return nil, fmt.Errorf("marshal transcript root %s payload: %w", step.eventID, err)
			}
			roots = append(roots, replayconformance.RootInput{
				Kind: step.inputKind, EventID: step.eventID, CreatedAt: step.createdAt,
				EventType: step.Event, Payload: payload, SourceAgent: step.sourceAgent,
				Accepted: strings.TrimSpace(step.ErrorContains) == "",
			})
		}
	}
	if err := replayconformance.ValidateRootInputs(roots); err != nil {
		return nil, err
	}
	return roots, nil
}

func (h *runtimeHarness) catalogOperatorEvents() (map[string]operatorread.OperatorEventFull, error) {
	ctx, cancel := context.WithTimeout(testAuthorActivityContext(context.Background()), catalogRuntimePublishTimeout)
	defer cancel()
	lister, err := h.catalogOperatorEventLister()
	if err != nil {
		return nil, err
	}
	return loadCatalogOperatorEvents(ctx, lister)
}

func (h *runtimeHarness) catalogOperatorEventLister() (catalogOperatorEventLister, error) {
	if h.pg != nil {
		return h.pg, nil
	}
	if h.sqlite != nil {
		return h.sqlite, nil
	}
	return nil, fmt.Errorf("catalog selected store is required")
}

type catalogOperatorEventLister = replayconformance.EventLister

func loadCatalogOperatorEvents(ctx context.Context, lister catalogOperatorEventLister) (map[string]operatorread.OperatorEventFull, error) {
	return replayconformance.LoadOperatorEvents(ctx, lister, catalogRuntimeRunID)
}

func canonicalJSONBytes(raw []byte) ([]byte, error) {
	return replayconformance.CanonicalJSON(raw)
}

func TestCatalogReplayClean_SelectedStores(t *testing.T) {
	for _, fixture := range catalogReplayCleanFixtures(t) {
		fixture := fixture
		t.Run(fixture.Name, func(t *testing.T) {
			transcript := buildCatalogExecutionTranscript(t, fixture)
			sourceHarness, source := executeCatalogTranscript(t, fixture, catalogBackendPostgres, transcript)
			reopenCatalogTranscript(t, fixture, transcript, sourceHarness, source)
			postgresHarness, postgres := executeCatalogTranscript(t, fixture, catalogBackendPostgres, transcript)
			reopenCatalogTranscript(t, fixture, transcript, postgresHarness, postgres)
			sqliteHarness, sqlite := executeCatalogTranscript(t, fixture, catalogBackendSQLite, transcript)
			reopenCatalogTranscript(t, fixture, transcript, sqliteHarness, sqlite)
			assertCatalogExecutionResultEqual(t, "postgres", source, postgres)
			assertCatalogExecutionResultEqual(t, "sqlite", source, sqlite)
		})
	}
}

func TestCatalogReplayCleanCensus(t *testing.T) {
	fixtures := catalogReplayCleanFixtures(t)
	if len(fixtures) != 93 {
		t.Fatalf("replay-clean fixtures = %d, want 93", len(fixtures))
	}

	repo := repoRootFromCatalogE2E(t)
	for _, relative := range []string{
		"internal/runtime/cataloge2e/tier5_lifecycle_e2e_test.go",
		"internal/runtime/cataloge2e/tier6_event_loop_e2e_test.go",
		"internal/runtime/cataloge2e/tier12_runtime_fork_e2e_test.go",
		"internal/runtime/cataloge2e/tier12_runtime_tools_e2e_test.go",
	} {
		assertCatalogReplayPrimitivesAbsent(t, filepath.Join(repo, filepath.FromSlash(relative)))
	}
}

func catalogReplayCleanFixtures(t testing.TB) []testcatalog.Fixture {
	t.Helper()
	inventory := catalogInventory(t)
	if len(inventory.Fixtures) != 155 {
		t.Fatalf("catalog fixtures = %d, want 155", len(inventory.Fixtures))
	}

	runtimeCount := 0
	excluded := map[string]bool{}
	fixtures := make([]testcatalog.Fixture, 0, 93)
	for _, fixture := range inventory.Fixtures {
		if fixture.Metadata.Disposition != testcatalog.DispositionRuntime {
			continue
		}
		runtimeCount++
		_, isExcluded := catalogReplayCleanExclusions[fixture.RelativePath]
		applicable := catalogReplayCleanApplicable(t, fixture)
		switch {
		case isExcluded && applicable:
			t.Fatalf("catalog replay exclusion %s is still classified applicable", fixture.RelativePath)
		case isExcluded:
			excluded[fixture.RelativePath] = true
		case !applicable:
			t.Fatalf("runtime fixture %s is outside replay-clean without an explicit disposition", fixture.RelativePath)
		default:
			fixtures = append(fixtures, fixture)
		}
	}
	if runtimeCount != 98 {
		t.Fatalf("runtime fixtures = %d, want 98", runtimeCount)
	}
	if len(fixtures) != 93 || len(excluded) != 5 {
		t.Fatalf("replay-clean census = %d applicable + %d excluded, want 93 + 5", len(fixtures), len(excluded))
	}
	for path, disposition := range catalogReplayCleanExclusions {
		if !excluded[path] {
			t.Fatalf("catalog replay exclusion %s (%s) is missing from the runtime census", path, disposition)
		}
	}
	return fixtures
}

func assertCatalogReplayPrimitivesAbsent(t testing.TB, path string) {
	t.Helper()
	parsed, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
	if err != nil {
		t.Fatalf("parse replay exclusion proof %s: %v", path, err)
	}
	forbidden := map[string]struct{}{
		"buildCatalogExecutionTranscript": {},
		"executeCatalogTranscript":        {},
		"reopenCatalogTranscript":         {},
	}
	ast.Inspect(parsed, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		ident, ok := call.Fun.(*ast.Ident)
		if !ok {
			return true
		}
		if _, blocked := forbidden[ident.Name]; blocked {
			t.Errorf("replay-excluded proof %s calls %s", path, ident.Name)
		}
		return true
	})
}

func catalogReplayCleanApplicable(t testing.TB, fixture testcatalog.Fixture) bool {
	t.Helper()
	if fixture.RelativePath == "tests/tier6-event-loop/test-cross-entity-concurrent" {
		return false
	}
	if fixture.HasClaim("catalog.runtime.selected_contract_fork") || fixture.HasClaim("catalog.runtime.flow_data_access_tools") {
		return false
	}
	var expected catalogExpectedDocument
	loadYAML(t, fixture.ExpectedPath, &expected)
	return !expected.Trigger.Boot && strings.TrimSpace(expected.Expected.BootResult) == ""
}

func assertCatalogExecutionResultEqual(t testing.TB, phase string, source, destination catalogExecutionResult) {
	t.Helper()
	sourceOutcomes, err := json.Marshal(source.outcomes)
	if err != nil {
		t.Fatalf("marshal source outcomes: %v", err)
	}
	destinationOutcomes, err := json.Marshal(destination.outcomes)
	if err != nil {
		t.Fatalf("marshal %s outcomes: %v", phase, err)
	}
	if string(sourceOutcomes) != string(destinationOutcomes) {
		t.Fatalf("%s step outcomes differ\nsource: %s\n%s: %s", phase, sourceOutcomes, phase, destinationOutcomes)
	}
	if string(source.projection) != string(destination.projection) {
		t.Fatalf("%s canonical projection differs\nsource: %s\n%s: %s", phase, source.projection, phase, destination.projection)
	}
}
