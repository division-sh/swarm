package serveapp

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/attemptgeneration"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/store/storetest"
)

// These are diagnostic projections, not historical execution or write authority.
// Raw event payloads, fields, and configuration bodies never enter the report.
type h2CancelTimer struct {
	ID       string                                   `json:"timer_id"`
	Run      string                                   `json:"run_id"`
	Entity   string                                   `json:"entity_id"`
	Flow     string                                   `json:"flow_scope_key"`
	Instance string                                   `json:"flow_instance"`
	Status   string                                   `json:"status"`
	Created  string                                   `json:"created_at"`
	Due      string                                   `json:"fire_at"`
	Fired    *string                                  `json:"fired_at"`
	Ref      timeridentity.WorkflowTimerActivationRef `json:"activation"`
}

type h2CancelHeader struct {
	Revision int64
	Present  bool
	Entity   string
	Flow     string
	Instance string
	Stage    string
	Entered  string
	Latest   h2CancelTransition
}

type h2CancelTransition struct {
	TransitionID   string    `json:"transition_id"`
	From           string    `json:"from"`
	To             string    `json:"to"`
	TriggerEventID string    `json:"trigger_event_id"`
	FiredAt        time.Time `json:"fired_at"`
}

type h2CancelHistory map[string]map[string][]storetest.H2CommittedFactEvidence

type h2CancelSibling struct {
	Revision, CreationRevision int64
	Timer                      h2CancelTimer
	Entry                      h2CancelHeader
}

type h2Cancellation struct {
	Run                           string
	Revision, Predecessor         int64
	Before, After                 h2CancelTimer
	HeaderBefore, HeaderAfter     h2CancelHeader
	TriggerID, TriggerType        string
	Occurrence                    *timeridentity.WorkflowTimerOccurrenceRef
	SiblingsBefore, SiblingsAfter []h2CancelSibling
	Site                          string
	SiteEvidence                  string
	Incomplete                    []string
}

type h2CancellationReport struct {
	Run                 string
	Complete, Truncated bool
	Rows, Bytes         int
	Cancellations       []h2Cancellation
	Errors              []string
	Limitations         string
}

func issue2564H2CaptureCancellation(t *testing.T, rt issue2564H2Fixture, run string, closed bool, retained issue2564H2Snapshot) {
	t.Helper()
	// This starts only after the original accounting deadline has failed. It is
	// synchronous and detached from that expired proof context, with its own cap.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	t.Logf("H2_CANCEL_LIVE_INVENTORY run=%s closed_expected=%v label=last_successful_read_not_cancellation_cut timers=%s", run, closed, h2CaptureJSON(retained.Timers))
	evidence, err := storetest.ObserveH2CommittedHistory(ctx, rt.selected, run)
	report := h2CancellationCuts(ctx, run, evidence)
	if err != nil {
		report.Complete = false
		report.Errors = append(report.Errors, "native_read: "+err.Error())
	}
	t.Logf("H2_CANCEL_COMMITTED_PROVENANCE %s", h2CaptureJSON(report))
}

func h2CaptureJSON(value any) string {
	raw, err := json.Marshal(value)
	if err != nil {
		return fmt.Sprintf("diagnostic_encoding_error=%q", err.Error())
	}
	return string(raw)
}

func h2CancellationCuts(ctx context.Context, run string, evidence storetest.H2CommittedHistoryEvidence) h2CancellationReport {
	out := h2CancellationReport{Run: run, Complete: evidence.Complete, Truncated: evidence.Truncated, Rows: len(evidence.Facts), Bytes: evidence.Bytes,
		Limitations: "committed facts only; rolled-back attempts and runtime call frames UNOBSERVED; revision is run fact revision, not header revision; stage entry is creation-cut metadata, not a synthesized stage-entry identity"}
	if evidence.RunID != run {
		out.Complete = false
		out.Errors = append(out.Errors, "exact run mismatch")
		return out
	}
	facts := slices.Clone(evidence.Facts)
	slices.SortFunc(facts, func(a, b storetest.H2CommittedFactEvidence) int {
		if a.Revision < b.Revision {
			return -1
		}
		if a.Revision > b.Revision {
			return 1
		}
		if c := strings.Compare(a.Family, b.Family); c != 0 {
			return c
		}
		return strings.Compare(a.Key, b.Key)
	})
	history := h2CancelHistory{}
	for _, fact := range facts {
		if history[fact.Family] == nil {
			history[fact.Family] = map[string][]storetest.H2CommittedFactEvidence{}
		}
		history[fact.Family][fact.Key] = append(history[fact.Family][fact.Key], fact)
	}
	previous := map[string]storetest.H2CommittedFactEvidence{}
	for _, fact := range facts {
		if err := ctx.Err(); err != nil {
			out.Errors = append(out.Errors, "reconstruction: "+err.Error())
			break
		}
		if fact.Revision <= 0 || fact.Key == "" {
			out.Errors = append(out.Errors, "invalid committed coordinate")
			continue
		}
		if fact.Family != "timers" {
			continue
		}
		before, known := previous[fact.Key]
		previous[fact.Key] = fact
		if !fact.Present {
			continue
		}
		after, err := h2DecodeCancelTimer(run, fact)
		if err != nil {
			out.Errors = append(out.Errors, err.Error())
			continue
		}
		if after.Status != "cancelled" {
			continue
		}
		old, beforeErr := h2DecodeCancelTimer(run, before)
		if known && before.Present && beforeErr == nil && old.Status == "cancelled" {
			continue
		}
		if len(out.Cancellations) == 64 {
			out.Truncated = true
			break
		}
		cut := h2Cancellation{Run: run, Revision: fact.Revision, Predecessor: before.Revision, Before: old, After: after, Site: "UNKNOWN"}
		if !known || !before.Present || beforeErr != nil {
			cut.Incomplete = append(cut.Incomplete, "cancellation predecessor missing/deleted/invalid")
		}
		cut.HeaderBefore = h2CancelHeaderAt(history, after.Entity, fact.Revision-1, &cut.Incomplete)
		cut.HeaderAfter = h2CancelHeaderAt(history, after.Entity, fact.Revision, &cut.Incomplete)
		cut.SiblingsBefore = h2CancelSiblings(ctx, run, history, after, fact.Revision-1, &cut.Incomplete)
		cut.SiblingsAfter = h2CancelSiblings(ctx, run, history, after, fact.Revision, &cut.Incomplete)
		h2CancelTrigger(history, &cut)
		out.Cancellations = append(out.Cancellations, cut)
		if len(cut.Incomplete) != 0 {
			out.Complete = false
		}
	}
	if len(out.Errors) != 0 || out.Truncated {
		out.Complete = false
	}
	return out
}

func h2DecodeCancelTimer(run string, fact storetest.H2CommittedFactEvidence) (h2CancelTimer, error) {
	var wire struct {
		h2CancelTimer
		Name string `json:"timer_name"`
	}
	if !fact.Present {
		return h2CancelTimer{}, fmt.Errorf("timer %s absent", fact.Key)
	}
	if err := json.Unmarshal(fact.Fact, &wire); err != nil {
		return h2CancelTimer{}, err
	}
	timer := wire.h2CancelTimer
	var ok bool
	timer.Ref, ok = timeridentity.ParseWorkflowTimerActivationTaskID(wire.Name)
	if !ok || timer.Run != run || timer.ID != fact.Key || timer.Ref.ActivationID != timer.ID || timer.Entity == "" || timer.Instance == "" {
		return h2CancelTimer{}, fmt.Errorf("timer identity mismatch at %s/%d", fact.Key, fact.Revision)
	}
	return timer, nil
}

// Only the three closed diagnostic families are scanned. A tombstone wins over
// an older present fact; there is no fallback to a live/latest table read.
func h2CancelFactAt(history h2CancelHistory, family, key string, revision int64) (storetest.H2CommittedFactEvidence, bool) {
	var found storetest.H2CommittedFactEvidence
	ok := false
	for _, fact := range history[family][key] {
		if fact.Revision > revision {
			break
		}
		if fact.Family == family && fact.Key == key {
			found, ok = fact, true
		}
	}
	return found, ok
}

func h2CancelHeaderAt(history h2CancelHistory, entity string, revision int64, incomplete *[]string) h2CancelHeader {
	fact, ok := h2CancelFactAt(history, "entity_metadata", entity, revision)
	out := h2CancelHeader{Revision: fact.Revision, Present: fact.Present}
	if !ok || !fact.Present {
		*incomplete = append(*incomplete, fmt.Sprintf("header missing/deleted for %s at %d", entity, revision))
		return out
	}
	var wire struct {
		Entity   string `json:"entity_id"`
		Flow     string `json:"flow_template"`
		Instance string `json:"flow_instance"`
		Stage    string `json:"current_state"`
		Entered  string `json:"entered_state_at"`
		Config   struct {
			History []h2CancelTransition `json:"transition_history"`
		} `json:"flow_config"`
	}
	if err := json.Unmarshal(fact.Fact, &wire); err != nil || wire.Entity != entity {
		*incomplete = append(*incomplete, "header identity/decode failure")
		return out
	}
	out.Entity, out.Flow, out.Instance, out.Stage, out.Entered = wire.Entity, wire.Flow, wire.Instance, wire.Stage, wire.Entered
	if len(wire.Config.History) != 0 {
		out.Latest = wire.Config.History[0]
	}
	return out
}

func h2CancelSiblings(ctx context.Context, run string, history h2CancelHistory, target h2CancelTimer, revision int64, incomplete *[]string) []h2CancelSibling {
	keys := map[string]bool{}
	for key := range history["timers"] {
		keys[key] = true
	}
	var out []h2CancelSibling
	ordered := mapKeysH2(keys)
	slices.Sort(ordered)
	for _, key := range ordered {
		if err := ctx.Err(); err != nil {
			*incomplete = append(*incomplete, "sibling reconstruction: "+err.Error())
			break
		}
		fact, _ := h2CancelFactAt(history, "timers", key, revision)
		if !fact.Present {
			continue
		}
		timer, err := h2DecodeCancelTimer(run, fact)
		if err != nil {
			*incomplete = append(*incomplete, err.Error())
			continue
		}
		if timer.Entity != target.Entity || timer.Instance != target.Instance || timer.Flow != target.Flow || timer.Ref.DeclarationKey != target.Ref.DeclarationKey {
			continue
		}
		if len(out) == 256 {
			*incomplete = append(*incomplete, "sibling bound 256 reached")
			break
		}
		sibling := h2CancelSibling{Revision: fact.Revision, Timer: timer}
		for _, created := range history["timers"][key] {
			if created.Family == "timers" && created.Key == key && created.Present && created.Revision <= revision {
				sibling.CreationRevision = created.Revision
				sibling.Entry = h2CancelHeaderAt(history, timer.Entity, created.Revision, incomplete)
				break
			}
		}
		out = append(out, sibling)
	}
	return out
}

func mapKeysH2(keys map[string]bool) []string {
	out := make([]string, 0, len(keys))
	for key := range keys {
		out = append(out, key)
	}
	return out
}

func h2CancelTrigger(history h2CancelHistory, cut *h2Cancellation) {
	header := cut.HeaderAfter
	if !header.Present || header.Revision != cut.Revision || header.Entity != cut.After.Entity || header.Instance != cut.After.Instance || header.Flow != cut.After.Flow {
		cut.Incomplete = append(cut.Incomplete, "no exact co-committed matching transition header")
		return
	}
	cut.TriggerID = header.Latest.TriggerEventID
	fact, ok := h2CancelFactAt(history, "events", cut.TriggerID, cut.Revision)
	if !ok || !fact.Present {
		cut.Incomplete = append(cut.Incomplete, "trigger event missing/deleted at cancellation cut")
		return
	}
	var event struct {
		ID   string `json:"event_id"`
		Run  string `json:"run_id"`
		Type string `json:"event_name"`
		Task string `json:"task_id"`
	}
	if err := json.Unmarshal(fact.Fact, &event); err != nil || event.ID != cut.TriggerID || event.Run != cut.Run {
		cut.Incomplete = append(cut.Incomplete, "trigger event identity/decode failure")
		return
	}
	cut.TriggerType = event.Type
	if event.Type == "platform.stage_timer" {
		occurrence, valid := timeridentity.ParseWorkflowTimerOccurrenceTaskID(event.Task)
		if !valid || timeridentity.WorkflowTimerOccurrenceEventID(occurrence) != event.ID {
			cut.Incomplete = append(cut.Incomplete, "trigger occurrence identity failure")
			return
		}
		cut.Occurrence = &occurrence
	}
	// This is an inference for this fixed H2 source, never an observed frame.
	declarationStage := map[string]string{"stage:hub:hub.h2.s1_to_s2": "s1", "stage:hub:hub.h2.s2_to_s1": "s2"}[cut.After.Ref.DeclarationKey]
	if cut.After.Flow == "hub" && declarationStage != "" && header.Latest.From == declarationStage && header.Latest.To != declarationStage &&
		cut.Before.Status == "active" && cut.After.Ref.Cause == timeridentity.WorkflowTimerActivationCauseTransition && cut.After.Ref.Generation == (attemptgeneration.Generation{}) &&
		cut.HeaderBefore.Stage == declarationStage && header.Stage == header.Latest.To && (event.Type == "hub.close" || cut.Occurrence != nil) {
		cut.Site = "DERIVED: retainWorkflowTimerActivations stage-exit candidate"
		cut.SiteEvidence = "fixed H2 declaration leaves owning stage; active->cancelled timer and exact transition header share committed revision; exact trigger retained; initial/loop cleanup excluded by creation cause/empty generation; call frame UNOBSERVED"
	}
}
