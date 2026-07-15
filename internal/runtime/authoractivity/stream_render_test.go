package authoractivity

import (
	"bytes"
	"encoding/json"
	"regexp"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
)

func TestHumanRendererBankedAuthorStoryGrammar(t *testing.T) {
	base := time.Date(2026, 7, 14, 19, 8, 9, 0, time.UTC)
	ingress := occurrenceFromDraft(testDraft(KindInboundReceived, "received", base), 1)
	ingress.Projection = Projection{
		SubjectType: "entity", SubjectID: uuid.NewString(), Provider: "telegram",
		AuthorSubjectType: "chat", AuthorSubjectID: "123456",
	}
	entity := occurrenceFromDraft(testDraft(KindEntityLifecycle, "created", base), 2)
	entity.Projection = Projection{
		SubjectType: "entity", SubjectID: uuid.NewString(), NewState: "active",
		AuthorSubjectType: "chat", AuthorSubjectID: "123456",
	}
	emitted := occurrenceFromDraft(testDraft(KindEventEmitted, "emitted", base.Add(52*time.Second)), 3)
	emitted.Projection = Projection{EventType: "phrase.completed", ProducerType: "agent", ProducerID: "drafter"}
	emitted.AuthorSafeSummary = "how are you"
	delivered := occurrenceFromDraft(testDraft(KindDeliveryLifecycle, "delivered", base.Add(53*time.Second)), 4)
	delivered.Projection = Projection{SubjectType: "agent", SubjectID: "telegram-sender", EventType: "phrase.completed"}
	delivered.AuthorSafeSummary = "how are you"
	failed := occurrenceFromDraft(testDraft(KindDeliveryLifecycle, "failed", base.Add(56*time.Second)), 5)
	failed.RunID = "99e0d8c2-4e75-4e55-a17c-2b887c2a6f31"
	failed.Projection = Projection{SubjectType: "agent", SubjectID: "telegram-normalizer", EventType: "message.received"}
	failed.Failure = testFailure(t)

	var out bytes.Buffer
	if err := Render(&out, []Occurrence{ingress, entity, emitted, delivered, failed}, RenderOptions{Mode: RenderPlain, Width: 120}); err != nil {
		t.Fatal(err)
	}
	want := "19:08:09  telegram → message received (chat 123456)\n" +
		"19:08:09  chat 123456 created · stage active\n" +
		"19:09:01  drafter → phrase.completed \"how are you\"\n" +
		"19:09:02  telegram-sender ✓ sent \"how are you\"\n" +
		"19:09:05  telegram-normalizer ✗ failed — internal error\n" +
		"          └ swarm logs --run 99e0d8c2-4e75-4e55-a17c-2b887c2a6f31 --level error\n"
	if out.String() != want {
		t.Fatalf("author story output:\n%s\nwant:\n%s", out.String(), want)
	}
}

func TestHumanRendererCompressesContiguousFailuresAndFlushes(t *testing.T) {
	base := time.Date(2026, 7, 14, 19, 9, 5, 0, time.UTC)
	makeFailure := func(sequence int64, offset time.Duration) Occurrence {
		occurrence := occurrenceFromDraft(testDraft(KindDeliveryLifecycle, "failed", base.Add(offset)), sequence)
		occurrence.RunID = "99e0d8c2-4e75-4e55-a17c-2b887c2a6f31"
		occurrence.Projection = Projection{SubjectType: "agent", SubjectID: "telegram-normalizer", EventType: "message.received"}
		occurrence.Failure = testFailure(t)
		return occurrence
	}
	renderer := NewHumanRenderer(RenderOptions{Mode: RenderPlain, Width: 120})
	page := []Occurrence{
		makeFailure(1, 0), makeFailure(2, time.Second), makeFailure(3, 2*time.Second), makeFailure(4, 3*time.Second),
		makeFailure(5, 4*time.Second), makeFailure(6, 5*time.Second), makeFailure(7, 6*time.Second),
	}
	rendered, renderer, err := renderer.PreparePage(page)
	if err != nil {
		t.Fatal(err)
	}
	text := string(rendered)
	for _, want := range []string{
		"19:09:05  telegram-normalizer ✗ failed — internal error\n          └ swarm logs --run 99e0d8c2-4e75-4e55-a17c-2b887c2a6f31 --level error\n",
		"19:09:06  telegram-normalizer ✗ failed — internal error (2nd time)\n",
		"19:09:11  telegram-normalizer ✗ failed — internal error (×5 in 6s)\n",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("compressed output missing %q:\n%s", want, text)
		}
	}
	if strings.Count(text, "└ swarm logs") != 1 {
		t.Fatalf("compressed failures repeated diagnostic route:\n%s", text)
	}

	pending, renderer, err := renderer.PreparePage([]Occurrence{makeFailure(8, 7*time.Second), makeFailure(9, 8*time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 0 {
		t.Fatalf("pending repeats rendered early: %q", pending)
	}
	flushed, _, err := renderer.PrepareFlush()
	if err != nil {
		t.Fatal(err)
	}
	if got, want := string(flushed), "19:09:13  telegram-normalizer ✗ failed — internal error (×2 in 2s)\n"; got != want {
		t.Fatalf("shutdown flush = %q, want %q", got, want)
	}
}

func TestHumanRendererFlushesWhenRepetitionWindowClosesDuringSilence(t *testing.T) {
	base := time.Date(2026, 7, 14, 19, 9, 5, 0, time.UTC)
	makeFailure := func(sequence int64, offset time.Duration) Occurrence {
		occurrence := occurrenceFromDraft(testDraft(KindDeliveryLifecycle, "failed", base.Add(offset)), sequence)
		occurrence.RunID = "99e0d8c2-4e75-4e55-a17c-2b887c2a6f31"
		occurrence.Projection = Projection{SubjectType: "agent", SubjectID: "telegram-normalizer", EventType: "message.received"}
		occurrence.Failure = testFailure(t)
		return occurrence
	}
	renderer := NewHumanRenderer(RenderOptions{Mode: RenderPlain, Width: 120})
	_, renderer, err := renderer.PreparePage([]Occurrence{
		makeFailure(1, 0), makeFailure(2, time.Second), makeFailure(3, 2*time.Second), makeFailure(4, 3*time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}
	before, unchanged, err := renderer.PrepareWindowClose(base.Add(3*time.Minute - time.Nanosecond))
	if err != nil {
		t.Fatal(err)
	}
	if len(before) != 0 {
		t.Fatalf("window flushed early: %q", before)
	}
	flushed, renderer, err := unchanged.PrepareWindowClose(base.Add(3 * time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if got, want := string(flushed), "19:09:08  telegram-normalizer ✗ failed — internal error (×2 in 3s)\n"; got != want {
		t.Fatalf("window close flush = %q, want %q", got, want)
	}
	again, _, err := renderer.PrepareWindowClose(base.Add(6 * time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if len(again) != 0 {
		t.Fatalf("closed window flushed twice: %q", again)
	}
}

func TestHumanRendererHiddenOccurrenceBreaksContiguousFailureGroup(t *testing.T) {
	base := time.Date(2026, 7, 14, 19, 9, 5, 0, time.UTC)
	makeFailure := func(sequence int64, offset time.Duration) Occurrence {
		occurrence := occurrenceFromDraft(testDraft(KindDeliveryLifecycle, "failed", base.Add(offset)), sequence)
		occurrence.RunID = "99e0d8c2-4e75-4e55-a17c-2b887c2a6f31"
		occurrence.Projection = Projection{SubjectType: "agent", SubjectID: "telegram-normalizer", EventType: "message.received"}
		occurrence.Failure = testFailure(t)
		return occurrence
	}
	hidden := occurrenceFromDraft(testDraft(KindTurnToolCompleted, "completed", base.Add(3*time.Second)), 4)
	renderer := NewHumanRenderer(RenderOptions{Mode: RenderPlain, Width: 120})
	rendered, renderer, err := renderer.PreparePage([]Occurrence{
		makeFailure(1, 0), makeFailure(2, time.Second), makeFailure(3, 2*time.Second), hidden, makeFailure(5, 4*time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}
	text := string(rendered)
	if !strings.Contains(text, "19:09:07  telegram-normalizer ✗ failed — internal error (×1 in 2s)") {
		t.Fatalf("hidden occurrence did not flush pending contiguous group:\n%s", text)
	}
	if strings.Contains(text, "tool completed") {
		t.Fatalf("hidden occurrence was rendered:\n%s", text)
	}
	if !strings.Contains(text, "19:09:09  telegram-normalizer ✗ failed — internal error\n") || strings.Contains(text, "19:09:09  telegram-normalizer ✗ failed — internal error (") {
		t.Fatalf("post-hidden failure did not start a new group:\n%s", text)
	}
	flushed, _, err := renderer.PrepareFlush()
	if err != nil {
		t.Fatal(err)
	}
	if len(flushed) != 0 {
		t.Fatalf("new first occurrence left a suppressed aggregate: %q", flushed)
	}
}

func TestNDJSONNeverCompressesRepetitions(t *testing.T) {
	base := time.Date(2026, 7, 14, 19, 9, 5, 0, time.UTC)
	occurrences := make([]Occurrence, 0, 7)
	for i := 0; i < 7; i++ {
		occurrence := occurrenceFromDraft(testDraft(KindDeliveryLifecycle, "failed", base.Add(time.Duration(i)*time.Second)), int64(i+1))
		occurrence.Projection = Projection{SubjectType: "agent", SubjectID: "normalizer", EventType: "message.received"}
		occurrence.Failure = testFailure(t)
		occurrences = append(occurrences, occurrence)
	}
	var out bytes.Buffer
	if err := Render(&out, occurrences, RenderOptions{Mode: RenderNDJSON}); err != nil {
		t.Fatal(err)
	}
	lines := bytes.Split(bytes.TrimSpace(out.Bytes()), []byte("\n"))
	if len(lines) != len(occurrences) {
		t.Fatalf("NDJSON lines = %d, want %d", len(lines), len(occurrences))
	}
	for _, line := range lines {
		var occurrence Occurrence
		if err := json.Unmarshal(line, &occurrence); err != nil {
			t.Fatal(err)
		}
	}
}

func TestTTYColorIsAdditiveAndWidthCountsVisibleText(t *testing.T) {
	occurrence := occurrenceFromDraft(testDraft(KindDeliveryLifecycle, "failed", time.Date(2026, 7, 14, 19, 9, 5, 0, time.UTC)), 1)
	occurrence.RunID = "99e0d8c2-4e75-4e55-a17c-2b887c2a6f31"
	occurrence.Projection = Projection{SubjectType: "agent", SubjectID: "telegram-normalizer", EventType: "message.received"}
	occurrence.Failure = testFailure(t)
	plainOpts := RenderOptions{Mode: RenderPlain, Width: 44}
	ttyOpts := RenderOptions{Mode: RenderTTY, Width: 44, Palette: Palette{
		Time: ansi("2"), Subject: ansi("1"), Identity: ansi("36"), SubjectIdentity: ansi("1;36"),
		Success: ansi("32"), Warning: ansi("33"), Failure: ansi("31"),
	}}
	var plain, tty bytes.Buffer
	if err := Render(&plain, []Occurrence{occurrence}, plainOpts); err != nil {
		t.Fatal(err)
	}
	if err := Render(&tty, []Occurrence{occurrence}, ttyOpts); err != nil {
		t.Fatal(err)
	}
	stripped := regexp.MustCompile(`\x1b\[[0-9;]*m`).ReplaceAllString(tty.String(), "")
	if stripped != plain.String() {
		t.Fatalf("ANSI changed text:\nplain=%q\ntty=%q\nstripped=%q", plain.String(), tty.String(), stripped)
	}
	for _, line := range strings.Split(strings.TrimSuffix(stripped, "\n"), "\n") {
		if got := utf8.RuneCountInString(line); got > 44 {
			t.Fatalf("visible line width = %d, line %q", got, line)
		}
	}
}

func TestTTYPaletteStylesTypedSemanticTokensOnly(t *testing.T) {
	base := time.Date(2026, 7, 14, 19, 8, 9, 0, time.UTC)
	ingress := occurrenceFromDraft(testDraft(KindInboundReceived, "received", base), 1)
	ingress.Projection = Projection{
		SubjectType: "entity", SubjectID: uuid.NewString(), Provider: "telegram",
		AuthorSubjectType: "chat", AuthorSubjectID: "123456",
	}
	delivered := occurrenceFromDraft(testDraft(KindDeliveryLifecycle, "delivered", base.Add(time.Second)), 2)
	delivered.Projection = Projection{SubjectType: "agent", SubjectID: "telegram-sender", EventType: "phrase.completed"}
	failed := occurrenceFromDraft(testDraft(KindDeliveryLifecycle, "failed", base.Add(2*time.Second)), 3)
	failed.Projection = Projection{SubjectType: "agent", SubjectID: "telegram-normalizer", EventType: "phrase.completed"}
	failed.Failure = testFailure(t)

	var out bytes.Buffer
	err := Render(&out, []Occurrence{ingress, delivered, failed}, RenderOptions{Mode: RenderTTY, Width: 120, Palette: Palette{
		Time: ansi("2"), Subject: ansi("1"), Identity: ansi("36"), SubjectIdentity: ansi("1;36"),
		Success: func(value string) string { return strings.ReplaceAll(value, "✓", ansi("32")("✓")) },
		Warning: ansi("33"), Failure: ansi("31"),
	}})
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSuffix(out.String(), "\n"), "\n")
	if got, want := lines[0], "\x1b[2m19:08:09\x1b[0m  \x1b[1mtelegram\x1b[0m → message received (\x1b[36mchat 123456\x1b[0m)"; got != want {
		t.Fatalf("ingress palette = %q, want %q", got, want)
	}
	if !strings.Contains(lines[1], "\x1b[1;36mtelegram-sender\x1b[0m \x1b[32m✓\x1b[0m sent") {
		t.Fatalf("success palette = %q", lines[1])
	}
	if !strings.HasPrefix(lines[2], "\x1b[31m19:08:11  telegram-normalizer ✗ failed — internal error\x1b[0m") || strings.Contains(lines[2], "\x1b[36m") || strings.Contains(lines[2], "\x1b[32m") {
		t.Fatalf("failure palette was not one reserved red token: %q", lines[2])
	}
}

func TestHumanSubjectsDoNotExposeInternalEntityEventOrActivityIDs(t *testing.T) {
	internalID := "4d674b5e-f137-4239-8d83-6b33ee6901f5"
	base := time.Date(2026, 7, 14, 19, 9, 5, 0, time.UTC)
	entity := occurrenceFromDraft(testDraft(KindEntityLifecycle, "created", base), 1)
	entity.Projection = Projection{SubjectType: "entity", SubjectID: internalID, NewState: "active"}
	dead := occurrenceFromDraft(testDraft(KindDeadLetterRecorded, "recorded", base.Add(time.Second)), 2)
	dead.Projection = Projection{SubjectType: "event", SubjectID: internalID, EventType: "message.received"}
	dead.Failure = testFailure(t)
	activity := occurrenceFromDraft(testDraft(KindActivityLifecycle, "failed", base.Add(2*time.Second)), 3)
	activity.Projection = Projection{SubjectType: "activity", SubjectID: internalID, Activity: "normalize-message"}
	activity.Failure = testFailure(t)
	platform := occurrenceFromDraft(testDraft(KindPlatformSignal, "event_quarantined", base.Add(3*time.Second)), 4)
	platform.Projection = Projection{SubjectType: "entity", SubjectID: internalID, EventType: "message.received"}
	var out bytes.Buffer
	if err := Render(&out, []Occurrence{entity, dead, activity, platform}, RenderOptions{Mode: RenderPlain, Width: 120}); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(out.String(), internalID) {
		t.Fatalf("human story leaked internal identity: %s", out.String())
	}
	for _, want := range []string{"entity created · stage active", "message.received ✗ event failed", "normalize-message ✗ failed", "entity event quarantined"} {
		if !strings.Contains(out.String(), want) {
			t.Fatalf("human story missing %q: %s", want, out.String())
		}
	}
}

func TestNormalizeAuthorSafeSummaryIsSingleLineUnicodeCapped(t *testing.T) {
	got, err := NormalizeAuthorSafeSummary("  hello\n\tworld \x00 " + strings.Repeat("界", 30))
	if err != nil {
		t.Fatal(err)
	}
	if strings.ContainsAny(got, "\n\t\x00") || utf8.RuneCountInString(got) != 24 || !strings.HasPrefix(got, "hello world ") {
		t.Fatalf("normalized summary = %q (%d runes)", got, utf8.RuneCountInString(got))
	}
}

func occurrenceFromDraft(draft Draft, sequence int64) Occurrence {
	return Occurrence{
		OccurrenceID: draft.OccurrenceID, Sequence: sequence, Kind: draft.Kind, Version: draft.Version,
		Transition: draft.Transition, SourceOwner: draft.SourceOwner, SourceIdentity: draft.SourceIdentity,
		DedupKey: draft.DedupKey, OccurredAt: draft.OccurredAt, RunID: draft.RunID, EntityID: draft.EntityID,
		AgentID: draft.AgentID, FlowID: draft.FlowID, Scope: draft.Scope, AuthorSafeSummary: draft.AuthorSafeSummary,
		Projection: draft.Projection, Failure: draft.Failure,
	}
}

func ansi(code string) func(string) string {
	return func(value string) string { return "\x1b[" + code + "m" + value + "\x1b[0m" }
}
