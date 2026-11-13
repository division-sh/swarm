package releasee2e

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/division-sh/swarm/tests/support/testutil/llmapi"
	"github.com/division-sh/swarm/tests/support/testutil/telegramapi"
)

// The partial-feed cut is deliberately observable, not scheduler-controlled.
// A missed cut fails this arm; it never retries the scenario or changes timing.
func TestCompiledProcessFullLifecycleRetainedStateBothStores(t *testing.T) {
	if profile, full := goldenContinuousProofProfile(t); !full {
		t.Skipf("joined retained-state comparison requires full profile, got %q", profile)
	}
	dsn := strings.TrimSpace(os.Getenv(goldenPostgresEnv))
	if dsn == "" {
		t.Fatalf("%s is required", goldenPostgresEnv)
	}
	base := goldenReleaseRoot(t)
	binary := buildReleaseBinary(t, base)
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			var reference []string
			for _, arm := range []string{"uninterrupted", "graceful", "kill"} {
				if !t.Run(arm, func(t *testing.T) {
					store := goldenSQLiteStore(filepath.Join(base, backend, arm))
					if backend == "postgres" {
						store = goldenPostgresStore(t, dsn)
					}
					result := runJoinedRetainedState(t, binary, filepath.Join(base, backend, arm), store, arm)
					if arm == "uninterrupted" {
						reference = result
					} else if reference == nil {
						t.Fatal("uninterrupted reference did not complete; no equivalence credit")
					} else if !reflect.DeepEqual(reference, result) {
						t.Fatalf("retained semantic result differs\nreference=%s\n%s=%s", reference, arm, result)
					}
				}) {
					t.Log("remaining arms not run: this store has no completed comparison control")
					break
				}
			}
		})
	}
}

type joinedRetainedScenario struct {
	t          *testing.T
	ctx        context.Context
	process    *releaseServeProcess
	spec       releaseProcessSpec
	binary     string
	runID      string
	seedID     string
	card       fullLifecycleCard
	session    goldenConversation
	firstTurn  goldenTurn
	events     map[string]string
	memoryFlow string
	timerDue   time.Time
	model      *llmapi.Server
	telegram   *telegramapi.Double
	unblock    func()
	entered    <-chan []byte
}

func runJoinedRetainedState(t *testing.T, binary, root string, store goldenStoreSelection, arm string) []string {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()
	held := make(chan []byte, 1)
	release := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	defer unblock()
	var mu sync.Mutex
	counts := map[string]int{}
	var interruptedBody []byte
	model := llmapi.New(t, llmapi.OpenAICompatible, "/v1/chat/completions", "joined-model-key", func(request llmapi.Request) (llmapi.Reply, error) {
		var body struct {
			Model string `json:"model"`
		}
		if err := json.Unmarshal(request.Body, &body); err != nil {
			return llmapi.Reply{}, err
		}
		mu.Lock()
		counts[body.Model]++
		ordinal := counts[body.Model]
		if body.Model == "memory" && ordinal == 2 {
			interruptedBody = append([]byte(nil), request.Body...)
		}
		priorBody := interruptedBody
		mu.Unlock()
		switch body.Model {
		case "retry":
			return llmapi.Reply{StatusCode: 429, ErrorBody: `{"error":{"message":"synthetic rate limit before generation"}}`}, nil
		case "dead":
			return llmapi.Reply{StatusCode: 403, ErrorBody: `{"error":{"message":"synthetic model access refused"}}`}, nil
		case "memory":
			if ordinal == 2 {
				held <- priorBody
				<-release
			} else if ordinal == 3 && arm == "kill" && !bytes.Equal(priorBody, request.Body) {
				return llmapi.Reply{}, fmt.Errorf("interrupted read-only request changed on reissue")
			}
			if arm == "kill" && ordinal >= 3 {
				ordinal--
			}
		default:
			return llmapi.Reply{}, fmt.Errorf("unexpected model %q", body.Model)
		}
		arguments, _ := json.Marshal(map[string]string{"key": "memory", "text": "Remembered cedar-731."})
		return llmapi.Reply{ID: fmt.Sprint(request.Number), Model: body.Model, Text: "Remembered cedar-731.", InputTokens: 4, OutputTokens: 4, TotalTokens: 8,
			Tools: []llmapi.ToolCall{{ID: fmt.Sprintf("memory-reply-%d", ordinal), Name: "emit_memory_replied", Arguments: arguments}}}, nil
	})
	telegram := &telegramapi.Double{}
	proxyEnv := joinedTelegramTransport(t, root, telegram)
	source := writeJoinedRetainedSource(t, root)
	config := filepath.Join(root, ".swarm", "swarm.yaml")
	writeReleaseFile(t, config, strings.Replace(goldenRuntimeConfig(store), "  backend: claude_cli\n",
		fmt.Sprintf("  backend: openai_compatible\n  openai_compatible: {base_url: %q}\n  models:\n    memory: {openai_compatible: memory}\n    retry: {openai_compatible: retry}\n    dead: {openai_compatible: dead}\n    regular: {openai_compatible: unused-investigator}\n", model.URL+"/v1"), 1)+
		"connection:\n  api_token_file: api-token\nchannels:\n  hitl: {provider: telegram, account: '420079'}\n")
	writeReleaseFile(t, filepath.Join(root, "api-token"), goldenAPIToken+"\n")
	env := append(goldenProcessEnv(t, root, store.passwordEnv, 0), proxyEnv...)
	env = append(env, "SWARM_CREDENTIALS_FILE="+filepath.Join(root, "credentials.json"))
	for name, value := range map[string]string{"OPENAI_COMPATIBLE_API_KEY": "joined-model-key", "telegram.telegram_bot_token": "joined-bot-token", "telegram.webhook_signing_secret": "joined-signing-secret"} {
		set := runReleaseCommand(t, goldenStartupTimeout, root, env, value+"\n", binary, "--config", config, "secrets", "set", name, "--stdin")
		if set.err != nil {
			t.Fatalf("synthetic credential setup: %v\n%s", set.err, set.output)
		}
	}
	s := &joinedRetainedScenario{t: t, ctx: ctx, binary: binary, telegram: telegram, model: model, unblock: unblock, entered: held, events: map[string]string{}}
	s.spec = releaseProcessSpec{BinaryPath: binary, WorkingDir: root, ConfigPath: config, Source: source, Store: store.name,
		TokenFile: "api-token", Token: goldenAPIToken, Env: env, DefaultExecutionSelection: true, ShutdownGrace: goldenShutdownGrace,
		RedactValues: []string{"joined-model-key", "joined-bot-token", "joined-signing-secret"}}
	verified := runReleaseCommand(t, goldenStartupTimeout, root, env, "", binary, "verify", source, "--config", config, "--portable", "--json")
	assertFullLifecycleVerifySuccess(t, verified)
	s.start()
	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("joined process evidence:\n%s", s.process.output.String())
		}
	})
	joinedRegisterTelegram(t, s)
	// Registered Telegram pickup is common setup, before business work or an arm.
	if err := s.process.stopAndWait(goldenShutdownGrace); err != nil {
		t.Fatalf("registration setup shutdown: %v", err)
	}
	s.start()
	s.runID = waitForFullLifecycleStandingRun(t, s.process.rpc, "", "").RunID
	owner := base64.RawURLEncoding.EncodeToString([]byte("control")) + "." + base64.RawURLEncoding.EncodeToString([]byte("owner"))
	s.seedID = s.publish("control.seed", "memory", "seed")
	s.waitReceiver(s.seedID, owner, "node", "delivered")
	reviewSeed := s.publish("control.seed", "review", "seed-review")
	s.waitReceiver(reviewSeed, owner, "node", "delivered")
	s.publish("control.review", "review", "open-card")
	finishedSeed := s.publish("control.seed", "finished", "seed-finished")
	s.waitReceiver(finishedSeed, owner, "node", "delivered")
	finished := s.publish("control.finish", "finished", "finish")
	s.waitReceiver(finished, owner, "node", "delivered")
	s.card = waitForFullLifecycleCard(t, s.process.rpc, s.runID, "continuity")
	first := s.publish("work.requested", "memory", "memory-first")
	s.waitDelivery(first, "worker", "delivered")
	s.session = s.memorySession(1)
	initialTurns := s.memoryTurns()
	if len(initialTurns) != 1 || initialTurns[0].TriggerEventID != first || !initialTurns[0].ParseOK {
		t.Fatalf("initial memory turn not committed: %+v", initialTurns)
	}
	s.firstTurn = initialTurns[0]
	retry := s.publish("retry.requested", "memory", "retry")
	s.waitDelivery(retry, "retry-worker", "failed")
	dead := s.publish("dead.requested", "memory", "dead")
	s.waitDelivery(dead, "dead-worker", "dead_letter")
	second := s.publish("work.requested", "memory", "memory-second")
	select {
	case <-s.entered:
	case <-ctx.Done():
		t.Fatal("provider request boundary not reached")
	}
	imported := runReleaseCommand(t, goldenStartupTimeout, root, env, "", binary, "run", "start", "--connect", s.process.apiBase,
		"--run-id", s.runID, "--idempotency-key", "joined-feed", "--data", "item.registered="+filepath.Join(source, "data/items.jsonl"), "--no-follow")
	if imported.err != nil {
		t.Fatalf("public feed import: %v\n%s", imported.err, imported.output)
	}
	if err := pollReleaseCondition(ctx, 10*time.Millisecond, func() (bool, error) {
		feed, err := readNumericFeed(ctx, s.process.rpc, s.runID)
		if err != nil {
			return false, err
		}
		if feed.Cursor >= feed.Cardinality {
			return false, fmt.Errorf("cut not reached: feed completed before partial observation: %+v", feed)
		}
		return feed.Cursor > 0, nil
	}); err != nil {
		t.Fatal(err)
	}
	pauseFullLifecycleRun(t, s.process.rpc, s.runID)
	feed, err := readNumericFeed(ctx, s.process.rpc, s.runID)
	if err != nil || feed.Status != "open" || feed.Cardinality != 100 || feed.Cursor <= 0 || feed.Cursor >= feed.Cardinality || feed.Owed <= 0 {
		t.Fatalf("cut not reached: post-pause feed must be partial with work owed: %+v, %v", feed, err)
	}
	if feed.Runtime.Reason != "run_paused" || feed.Runtime.Eligible == nil || *feed.Runtime.Eligible {
		t.Fatalf("partial feed is not parked by exact run control: %+v", feed)
	}
	t.Logf("partial-feed cut reached before %s: cursor=%d/%d owed=%d status=%s", arm, feed.Cursor, feed.Cardinality, feed.Owed, feed.Status)
	before := s.checkpoint(retry, dead, second)
	t.Logf("all eight populations witnessed before %s: cursor=%d/100 owed=%d run=%s card=%s session=%s", arm, feed.Cursor, feed.Owed, s.runID, s.card.CardID, s.session.SessionID)
	switch arm {
	case "uninterrupted":
		unblock()
	case "graceful":
		stopped := make(chan error, 1)
		go func() { stopped <- s.process.signalAndWait(syscall.SIGTERM, goldenShutdownGrace+5*time.Second) }()
		if err := pollReleaseCondition(ctx, 10*time.Millisecond, func() (bool, error) {
			request, err := http.NewRequestWithContext(ctx, http.MethodGet, s.process.apiBase+"/readyz", nil)
			if err != nil {
				return false, err
			}
			response, err := http.DefaultClient.Do(request)
			if err != nil {
				return false, err
			}
			defer response.Body.Close()
			return response.StatusCode == http.StatusServiceUnavailable, nil
		}); err != nil {
			t.Fatalf("shutdown readiness boundary not reached: %v", err)
		}
		unblock()
		if err := <-stopped; err != nil {
			t.Fatalf("accepted provider work failed graceful drain: %v\n%s", err, s.process.output.String())
		}
		s.start()
	case "kill":
		if err := s.process.killAndWait(5 * time.Second); err != nil {
			t.Fatal(err)
		}
		unblock()
		s.start()
	}
	after, err := readNumericFeed(ctx, s.process.rpc, s.runID)
	if err != nil || after.Key != feed.Key || after.Cursor != feed.Cursor || after.Owed != feed.Owed || after.BundleHash != feed.BundleHash || after.Status != feed.Status {
		t.Fatalf("interruption changed parked feed: before=%+v after=%+v err=%v", feed, after, err)
	}
	if got := s.stableCheckpoint(retry, dead); !reflect.DeepEqual(before, got) {
		t.Fatalf("retained identities/state changed while paused:\nbefore=%s\nafter=%s", before, got)
	}
	continueFullLifecycleRun(t, s.process.rpc, s.runID)
	s.waitDelivery(second, "worker", "delivered")
	s.memorySession(2)
	decideFullLifecycleCard(t, s.process.rpc, s.card, "joined-decision")
	assertFullLifecycleCardDecided(t, s.process.rpc, s.card.CardID)
	third := s.publish("work.requested", "memory", "memory-third")
	s.waitDelivery(third, "worker", "delivered")
	if current := s.memorySession(3); current.SessionID != s.session.SessionID {
		t.Fatal("restart replaced the retained memory session")
	}
	turns := s.memoryTurns()
	triggers := []string{first, second, third}
	if arm == "kill" {
		triggers = []string{first, second, second, third}
	}
	if len(turns) != len(triggers) || turns[0] != s.firstTurn {
		t.Fatalf("memory turn history differs: %s", joinedJSON(turns))
	}
	for i, eventID := range triggers {
		turn := turns[i]
		if turn.Ordinal != i+1 || turn.TriggerEventID != eventID || turn.TriggerEventType != "work.requested" {
			t.Fatalf("memory turn %d lost exact trigger/ordinal: %s", i, joinedJSON(turn))
		}
		if arm == "kill" && i == 1 {
			f := turn.Failure
			if turn.ParseOK || f == nil || f.SchemaVersion != "platform.failure/v1" || f.Class != "platform.outcome_uncertain" ||
				f.Component != "external-effects" || f.Operation != "startup_reconcile" || f.Detail.Code != "effect_recovery_outcome_unconfirmed" {
				t.Fatalf("interrupted turn lost its exact recovery history: %s", joinedJSON(turn))
			}
		} else if !turn.ParseOK || turn.Failure != nil {
			t.Fatalf("business turn failed: %s", joinedJSON(turn))
		}
	}
	emissions := map[string]int{}
	events, err := listGoldenEvents(ctx, s.process.rpc, s.runID)
	if err != nil {
		t.Fatal(err)
	}
	for _, event := range events {
		if event.EventName == s.memoryFlow+"/memory.replied" {
			emissions[event.SourceEventID]++
		}
	}
	if !reflect.DeepEqual(emissions, map[string]int{first: 1, second: 1, third: 1}) {
		t.Fatalf("accepted memory emissions missing/repeated: %v", emissions)
	}
	if err := pollReleaseCondition(ctx, 10*time.Millisecond, func() (bool, error) {
		current, err := readNumericFeed(ctx, s.process.rpc, s.runID)
		if err != nil || current.Cursor != 100 || current.Owed != 0 {
			return false, err
		}
		entities, err := readGoldenEntities(ctx, s.process.rpc, s.runID)
		registry := 0
		for _, entity := range entities {
			if strings.HasPrefix(entity.FlowInstance, "registry/") {
				registry++
			}
		}
		if err != nil || registry != 100 {
			return false, err
		}
		events, err := listGoldenEvents(ctx, s.process.rpc, s.runID)
		for _, event := range events {
			if event.EventID == retry || event.EventID == dead || strings.HasPrefix(event.EventName, "platform.") {
				continue
			}
			for _, delivery := range event.Deliveries {
				if !delivery.Terminal {
					return false, nil
				}
				if delivery.Status != "delivered" {
					return false, fmt.Errorf("business delivery failed: %+v", delivery)
				}
			}
		}
		return true, err
	}); err != nil {
		t.Fatal(err)
	}
	var paused map[string]any
	if err := s.process.rpc.call(ctx, "run.pause", map[string]any{"run_id": s.runID, "idempotency_key": "joined-final-pause"}, &paused); err != nil {
		t.Fatal(err)
	}
	waitForFullLifecycleRunStatus(t, s.process.rpc, s.runID, "paused")
	result := s.semanticResult(retry, dead)
	mu.Lock()
	want := 3
	if arm == "kill" {
		want++
	}
	if counts["memory"] != want {
		t.Errorf("provider requests=%d, want %d including only exact interrupted reissue", counts["memory"], want)
	}
	mu.Unlock()
	if err := s.process.stopAndWait(goldenShutdownGrace); err != nil {
		t.Fatalf("final process join: %v\n%s", err, s.process.output.String())
	}
	return result
}

func (s *joinedRetainedScenario) start() {
	s.process = startReleaseServe(s.t, s.spec)
	if err := s.process.waitReady(s.ctx); err != nil {
		s.t.Fatalf("joined scenario readiness: %v\n%s", err, s.process.output.String())
	}
}

func (s *joinedRetainedScenario) publish(name, key, identity string) string {
	s.t.Helper()
	var result struct {
		EventID string `json:"event_id"`
	}
	params := map[string]any{"event_name": name, "run_id": s.runID, "payload": map[string]any{"key": key}, "idempotency_key": identity}
	if name == "work.requested" {
		note := "Recall the marker from the previous conversation."
		if identity == "memory-first" {
			note = "Remember cedar-731."
		}
		params["payload"] = map[string]any{"key": key, "note": note}
	}
	if s.seedID != "" {
		params["source_event_id"] = s.seedID
	}
	if err := s.process.rpc.call(s.ctx, "event.publish", params, &result); err != nil || result.EventID == "" {
		s.t.Fatalf("publish %s: %v\n%s", name, err, s.process.output.String())
	}
	s.events[result.EventID] = identity
	return result.EventID
}

func (s *joinedRetainedScenario) memoryTurns() []goldenTurn {
	s.t.Helper()
	var all []goldenTurn
	cursor := ""
	seen := map[string]bool{}
	for {
		var page struct {
			Turns      []goldenTurn `json:"turns"`
			NextCursor string       `json:"next_cursor"`
		}
		params := map[string]any{"session_id": s.session.SessionID, "limit": 1}
		if cursor != "" {
			params["cursor"] = cursor
		}
		if err := s.process.rpc.call(s.ctx, "conversation.list_turns", params, &page); err != nil {
			s.t.Fatal(err)
		}
		all = append(all, page.Turns...)
		if page.NextCursor == "" {
			break
		}
		if seen[page.NextCursor] {
			s.t.Fatal("memory turn pagination repeated its cursor")
		}
		seen[page.NextCursor], cursor = true, page.NextCursor
	}
	sort.Slice(all, func(i, j int) bool { return all[i].Ordinal < all[j].Ordinal })
	return all
}

func (s *joinedRetainedScenario) waitDelivery(eventID, subscriber, status string) {
	s.waitReceiver(eventID, subscriber, "agent", status)
}

func (s *joinedRetainedScenario) waitReceiver(eventID, subscriber, kind, status string) {
	s.t.Helper()
	var last goldenEvent
	if err := pollReleaseCondition(s.ctx, 10*time.Millisecond, func() (bool, error) {
		var event goldenEvent
		if err := s.process.rpc.call(s.ctx, "event.get", map[string]any{"event_id": eventID}, &event); err != nil {
			return false, err
		}
		last = event
		for _, delivery := range event.Deliveries {
			if delivery.SubscriberID == subscriber && delivery.SubscriberType == kind {
				if delivery.Terminal && delivery.Status != status {
					return false, fmt.Errorf("unexpected terminal delivery: %+v", delivery)
				}
				return delivery.Status == status, nil
			}
		}
		return false, nil
	}); err != nil {
		s.t.Fatalf("delivery %s/%s/%s: %v; last=%+v\n%s", eventID, subscriber, status, err, last, s.process.output.String())
	}
}

func (s *joinedRetainedScenario) memorySession(turns int) goldenConversation {
	s.t.Helper()
	var result goldenConversation
	if err := pollReleaseCondition(s.ctx, 10*time.Millisecond, func() (bool, error) {
		for _, session := range listGoldenConversations(s.t, s.ctx, s.process.rpc, s.runID) {
			if session.AgentID == "worker" && session.TurnCount == turns {
				result = session
				return true, nil
			}
		}
		return false, nil
	}); err != nil {
		s.t.Fatalf("memory session with %d turns: %v", turns, err)
	}
	return result
}

func (s *joinedRetainedScenario) checkpoint(retry, dead, held string) []string {
	s.t.Helper()
	card := waitForFullLifecycleCard(s.t, s.process.rpc, s.runID, "continuity")
	if card.CardID != s.card.CardID || card.CardContentHash != s.card.CardContentHash || card.Status != "pending" {
		s.t.Fatal("pending card identity/content changed before cut")
	}
	s.memorySession(1)
	var event goldenEvent
	if err := s.process.rpc.call(s.ctx, "event.get", map[string]any{"event_id": held}, &event); err != nil {
		s.t.Fatal(err)
	}
	live := false
	for _, d := range event.Deliveries {
		live = live || d.SubscriberID == "worker" && !d.Terminal && d.Status == "in_progress"
	}
	if !live {
		s.t.Fatalf("held provider delivery is not in flight while paused: %+v", event)
	}
	return s.stableCheckpoint(retry, dead)
}

func (s *joinedRetainedScenario) stableCheckpoint(retry, dead string) []string {
	s.t.Helper()
	var rows []string
	header := readPublicClockRun(s.t, s.process, s.runID)
	if header.Status != "paused" || len(header.ClockSchedules) != 1 || header.ClockSchedules[0]["status"] != "active" {
		s.t.Fatalf("paused run/armed recurring clock missing: %+v", header)
	}
	rows = append(rows, joinedJSON(header))
	card := waitForFullLifecycleCard(s.t, s.process.rpc, s.runID, "continuity")
	rows = append(rows, joinedJSON(card))
	turns := s.memoryTurns()
	if len(turns) == 0 || turns[0] != s.firstTurn {
		s.t.Fatalf("paused read lost the exact initial memory turn: %+v", turns)
	}
	rows = append(rows, joinedJSON(turns[0]))
	stages := map[string]bool{}
	for _, entity := range listGoldenEntities(s.t, s.ctx, s.process.rpc, s.runID) {
		if strings.HasPrefix(entity.FlowInstance, "control/") {
			var detail struct {
				Fields struct {
					Key string `json:"key"`
				} `json:"fields"`
			}
			if err := s.process.rpc.call(s.ctx, "entity.get", map[string]any{"entity_id": entity.EntityID, "run_id": s.runID}, &detail); err != nil {
				s.t.Fatal(err)
			}
			if detail.Fields.Key == "memory" {
				s.memoryFlow = entity.FlowInstance
			}
			stages[entity.CurrentState] = true
			rows = append(rows, joinedJSON(entity))
		}
	}
	if !stages["waiting"] || !stages["review"] || !stages["done"] {
		s.t.Fatalf("several stages including finished are not retained: %v", stages)
	}
	rows = append(rows, joinedJSON(s.memoryTimer()))
	for _, id := range []string{retry, dead} {
		var event goldenEvent
		if err := s.process.rpc.call(s.ctx, "event.get", map[string]any{"event_id": id}, &event); err != nil {
			s.t.Fatal(err)
		}
		for _, delivery := range event.Deliveries {
			if delivery.SubscriberType != "agent" {
				continue
			}
			if id == retry && (delivery.Status != "failed" || delivery.Terminal || !delivery.RetryScheduled) || id == dead && (delivery.Status != "dead_letter" || !delivery.Terminal || len(event.DeadLetters) == 0) {
				s.t.Fatalf("retry/dead-letter checkpoint missing: %+v", event)
			}
			rows = append(rows, joinedJSON(delivery))
		}
	}
	rows = append(rows, joinedChannelReadback(s.t, s))
	sort.Strings(rows)
	return rows
}

func (s *joinedRetainedScenario) semanticResult(retry, dead string) []string {
	s.t.Helper()
	rows := []string{joinedJSON([]any{"memory", s.memoryTranscript()}), "clock:24h:active", "run:paused", "card:continuity:approve"}
	identities := map[string]string{s.runID: "run", s.card.CardID: "card:continuity", s.session.SessionID: "memory-session"}
	entities := listGoldenEntities(s.t, s.ctx, s.process.rpc, s.runID)
	fields := map[string]any{}
	reviewEntity, reviewFlow := "", ""
	for _, entity := range entities {
		var detail struct {
			Fields json.RawMessage `json:"fields"`
		}
		if err := s.process.rpc.call(s.ctx, "entity.get", map[string]any{"entity_id": entity.EntityID, "run_id": s.runID}, &detail); err != nil {
			s.t.Fatal(err)
		}
		fieldValues := joinedDecode(s.t, detail.Fields)
		fields[entity.EntityID] = fieldValues
		label := "root"
		if object, ok := fieldValues.(map[string]any); ok {
			if key, ok := object["item_id"].(string); ok {
				label = "registry:" + key
			} else if key, ok := object["key"].(string); ok {
				label = "control:" + key
				if key == "review" {
					reviewEntity, reviewFlow = entity.EntityID, entity.FlowInstance
				}
			}
		}
		identities[entity.EntityID] = "entity:" + label
		identities[entity.FlowInstance] = "path:" + label
	}
	for _, entity := range entities {
		rows = append(rows, joinedJSON(joinedNormalize([]any{entity.EntityType, entity.FlowInstance, entity.CurrentState, fields[entity.EntityID]}, identities)))
	}
	var card struct {
		DecisionCard struct {
			Anchor   map[string]any  `json:"anchor"`
			Snapshot json.RawMessage `json:"snapshot"`
		} `json:"decision_card"`
	}
	if err := s.process.rpc.call(s.ctx, "mailbox.get", map[string]any{"mailbox_id": s.card.CardID}, &card); err != nil {
		s.t.Fatal(err)
	}
	anchor := card.DecisionCard.Anchor
	if reviewEntity == "" || anchor["entity_id"] != reviewEntity || anchor["flow_instance"] != reviewFlow || anchor["stage"] != "review" {
		s.t.Fatalf("card is not anchored to the exact review instance: %v", anchor)
	}
	for _, field := range []string{"flow_instance_id", "stage_activation_id"} {
		id, _ := anchor[field].(string)
		if id == "" {
			s.t.Fatalf("card lost %s", field)
		}
		identities[id] = "card-anchor:" + field
	}
	identities[s.card.CardContentHash] = "frozen-card-content"
	rows = append(rows, joinedJSON(joinedNormalize(joinedDecode(s.t, card.DecisionCard.Snapshot), identities)))
	events, err := listGoldenEvents(s.ctx, s.process.rpc, s.runID)
	if err != nil {
		s.t.Fatal(err)
	}
	for _, event := range events {
		label := s.events[event.EventID]
		if label == "" {
			label = event.EventName + ":" + identities[event.EntityID]
			if item, ok := event.Payload["item_id"].(string); ok {
				label += ":" + item
			}
		}
		identities[event.EventID] = "event:" + label
	}
	for _, event := range events {
		if strings.HasPrefix(event.EventName, "platform.") {
			continue
		}
		if event.SourceEventID != "" && identities[event.SourceEventID] == "" {
			s.t.Fatalf("event has an unobserved causal predecessor: %+v", event)
		}
		var exact struct {
			Payload                  json.RawMessage `json:"payload"`
			OperatorReferenceEventID string          `json:"operator_reference_event_id"`
		}
		if err := s.process.rpc.call(s.ctx, "event.get", map[string]any{"event_id": event.EventID}, &exact); err != nil {
			s.t.Fatal(err)
		}
		if exact.OperatorReferenceEventID != "" && identities[exact.OperatorReferenceEventID] == "" {
			s.t.Fatalf("event has an unobserved operator reference: %s", event.EventID)
		}
		payload := joinedNormalize(joinedDecode(s.t, exact.Payload), identities)
		rows = append(rows, joinedJSON([]any{"event", identities[event.EventID], event.EventName, payload, identities[event.SourceEventID], identities[exact.OperatorReferenceEventID], event.NoDelivery}))
		for _, delivery := range event.Deliveries {
			reason := ""
			if event.EventID == retry && (delivery.Status != "failed" || !delivery.RetryScheduled) || event.EventID == dead && delivery.Status != "dead_letter" {
				s.t.Fatalf("retained failure changed at comparison: %+v", event)
			}
			if event.EventID == retry || event.EventID == dead {
				reason = delivery.ReasonCode
			}
			rows = append(rows, joinedJSON(joinedNormalize([]any{identities[event.EventID], event.EventName, payload, identities[event.SourceEventID], delivery.SubscriberType, delivery.SubscriberID, delivery.Target.FlowInstance, delivery.Status, delivery.Terminal, reason}, identities)))
		}
	}
	header := readPublicClockRun(s.t, s.process, s.runID)
	if header.Status != "paused" || len(header.ClockSchedules) != 1 || header.ClockSchedules[0]["every"] != "24h0m0s" || header.ClockSchedules[0]["status"] != "active" {
		s.t.Fatalf("final recurring-clock/run state changed: %+v", header)
	}
	timer := s.memoryTimer()
	due := timer.NextWakeAt.UTC().Format(time.RFC3339Nano)
	identities[due] = "memory-workflow-original-due"
	rows = append(rows, joinedJSON(joinedNormalize([]any{"workflow_timer", timer.FlowInstance, timer.Stage, timer.Work.Timers, timer.Work.DueTimers, due}, identities)))
	_, sends := s.telegram.Counts()
	acknowledgements := 0
	for i := 0; i < sends; i++ {
		if s.telegram.Delivery(i)["text"] == "Swarm operator successfully connected" {
			acknowledgements++
		}
	}
	if acknowledgements != 1 {
		s.t.Fatalf("registered channel acknowledgement sends=%d, want 1 without restart resend", acknowledgements)
	}
	rows = append(rows, fmt.Sprintf("channel:telegram:420079:registered:ack=1:other_sends=%d", sends-acknowledgements))
	sort.Strings(rows)
	return rows
}

type joinedMemoryTimer struct {
	FlowInstance string `json:"flow_instance"`
	Stage        string `json:"stage"`
	Work         struct {
		Timers    int `json:"timers"`
		DueTimers int `json:"due_timers"`
	} `json:"work"`
	NextWakeAt *time.Time `json:"next_wake_at"`
}

func (s *joinedRetainedScenario) memoryTimer() joinedMemoryTimer {
	s.t.Helper()
	var diagnosis struct {
		Instances []joinedMemoryTimer `json:"instances"`
	}
	if err := s.process.rpc.call(s.ctx, "run.diagnose", map[string]any{"run_id": s.runID}, &diagnosis); err != nil {
		s.t.Fatal(err)
	}
	for _, timer := range diagnosis.Instances {
		if s.memoryFlow == "" || timer.FlowInstance != s.memoryFlow {
			continue
		}
		if timer.Stage != "waiting" || timer.Work.Timers != 1 || timer.Work.DueTimers != 0 || timer.NextWakeAt == nil || !timer.NextWakeAt.After(time.Now()) {
			s.t.Fatalf("memory-instance workflow timer changed: %+v", timer)
		}
		if s.timerDue.IsZero() {
			s.timerDue = *timer.NextWakeAt
		}
		if !s.timerDue.Equal(*timer.NextWakeAt) {
			s.t.Fatalf("memory workflow due changed: before=%s after=%s", s.timerDue, *timer.NextWakeAt)
		}
		return timer
	}
	s.t.Fatal("armed memory-instance workflow-timer witness unavailable while paused")
	return joinedMemoryTimer{}
}

func (s *joinedRetainedScenario) memoryTranscript() []any {
	s.t.Helper()
	var transcript []any
	var recorded [][]any
	for _, request := range s.model.Requests() {
		body := joinedDecode(s.t, request.Body).(map[string]any)
		if body["model"] == "memory" {
			recorded = append(recorded, body["messages"].([]any))
		}
	}
	turns := s.memoryTurns()
	if len(recorded) != len(turns) {
		s.t.Fatalf("recorded memory requests=%d, turns=%d", len(recorded), len(turns))
	}
	history := []any{map[string]any{"role": "system", "content": joinedMemoryPrompt}}
	for i, turn := range turns {
		if !turn.ParseOK {
			continue
		} // The caller separately asserts the exact kill-only failed attempt.
		var detail struct {
			Turn struct {
				TurnID  string `json:"turn_id"`
				Ordinal int    `json:"ordinal"`
				Output  string `json:"assistant_visible_output"`
			} `json:"turn"`
			Rendering struct {
				Status string          `json:"status"`
				Source string          `json:"source"`
				Input  json.RawMessage `json:"input"`
			} `json:"provider_rendering"`
		}
		if err := s.process.rpc.call(s.ctx, "conversation.get_turn", map[string]any{"session_id": s.session.SessionID, "turn_id": turn.TurnID}, &detail); err != nil {
			s.t.Fatal(err)
		}
		if detail.Turn.TurnID != turn.TurnID || detail.Turn.Ordinal != turn.Ordinal || detail.Turn.Output != "Remembered cedar-731." || detail.Rendering.Status != "recorded" || detail.Rendering.Source != "recorded_provider_request" {
			s.t.Fatalf("public logical transcript missing/changed: %s", joinedJSON(detail))
		}
		note := "Recall the marker from the previous conversation."
		if len(transcript) == 0 {
			note = "Remember cedar-731."
		}
		input := map[string]any{"key": "memory", "note": note}
		history = append(history, map[string]any{"role": "user", "content": joinedJSON(input)})
		publicStart := len(history) - 1
		if len(transcript) > 0 {
			publicStart-- // The public suffix includes the preceding accepted tool result.
		}
		if !reflect.DeepEqual(recorded[i], history) || !reflect.DeepEqual(joinedDecode(s.t, detail.Rendering.Input), history[publicStart:]) {
			s.t.Fatalf("recorded/public memory transcript changed: wire=%s public=%s want=%s", joinedJSON(recorded[i]), detail.Rendering.Input, joinedJSON(history))
		}
		callID := fmt.Sprintf("memory-reply-%d", len(transcript)+1)
		arguments := joinedJSON(map[string]any{"key": "memory", "text": detail.Turn.Output})
		history = append(history, map[string]any{"role": "assistant", "content": detail.Turn.Output, "tool_calls": []any{map[string]any{"id": callID, "type": "function", "function": map[string]any{"name": "emit_memory_replied", "arguments": arguments}}}},
			map[string]any{"role": "tool", "tool_call_id": callID, "content": "ok"})
		transcript = append(transcript, []any{len(transcript) + 1, s.events[turn.TriggerEventID], "user", input, "assistant", detail.Turn.Output})
	}
	if len(transcript) != 3 {
		s.t.Fatalf("logical transcript length=%d, want 3", len(transcript))
	}
	return transcript
}

func joinedDecode(t *testing.T, raw json.RawMessage) any {
	t.Helper()
	var value any
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := decoder.Decode(&value); err != nil {
		t.Fatalf("canonical public JSON: %v", err)
	}
	return value
}

func joinedNormalize(value any, identities map[string]string) any {
	switch v := value.(type) {
	case string:
		if identity, found := identities[v]; found {
			return identity
		}
	case []any:
		for i := range v {
			v[i] = joinedNormalize(v[i], identities)
		}
	case map[string]any:
		for key, field := range v {
			v[key] = joinedNormalize(field, identities)
		}
	}
	return value
}

func joinedJSON(value any) string {
	encoded, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	return string(encoded)
}
