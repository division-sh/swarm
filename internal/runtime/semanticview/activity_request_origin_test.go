package semanticview

import (
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/google/uuid"
)

// These are detached-carriage controls. Native ancestry, artifact loading and
// durable proposals must separately pass through their real owners.
func activityOriginCarriageFixture(t *testing.T, originalDecision, selectedDecision string) (OriginalLoopCarriage, ActivityRequestOrigin, identity.ExecutableNode) {
	t.Helper()
	declaration := func(decision, hash string) *originalLoopCarriage {
		bundle, _ := loopCarriageBundle(t)
		handler := bundle.Nodes["controller"].EventHandlers["start"]
		handler.Activity = contracts.ActivitySpec{ID: "write", Tool: "send"}
		if decision != "" {
			handler.Activity.Approval = &contracts.ActivityApprovalSpec{Decision: decision}
		}
		bundle.Nodes["controller"].EventHandlers["start"] = handler
		state, err := compileLoopCarriage(Wrap(bundle))
		if err != nil {
			t.Fatal(err)
		}
		state.bundleHash = hash
		return state
	}
	originalHash := "bundle-v2:sha256:" + strings.Repeat("a", 64)
	selectedHash := "bundle-v2:sha256:" + strings.Repeat("b", 64)
	originalFact, err := correlation.NewSourceArtifactFact(originalHash)
	if err != nil {
		t.Fatal(err)
	}
	selectedFact, err := correlation.NewSourceArtifactFact(selectedHash)
	if err != nil {
		t.Fatal(err)
	}
	_, node := loopCarriageBundle(t)
	run, requestID := uuid.NewString(), uuid.NewString()
	request := eventtest.RuntimeControl(requestID, "platform.activity_requested", "activity-runtime", "", []byte(`{"input":{"value":7}}`), 0,
		run, "", events.EventEnvelope{}, time.Now().UTC().Truncate(time.Microsecond))
	parent := ActivityRequestCut{RunID: run, RequestID: requestID, Revision: 4, Source: originalFact}
	child := ActivityRequestCut{RunID: uuid.NewString(), RequestID: uuid.NewString(), Revision: 6, Source: selectedFact,
		ParentPointKind: "event", ParentPointEventID: requestID, ParentPointRevision: 4}
	grandchild := ActivityRequestCut{RunID: uuid.NewString(), RequestID: uuid.NewString(), Revision: 8, Source: selectedFact,
		ParentPointKind: "event", ParentPointEventID: child.RequestID, ParentPointRevision: 6}
	state := declaration(selectedDecision, selectedHash)
	state.activityHistory = map[string]*originalLoopCarriage{originalHash: declaration(originalDecision, originalHash)}
	return OriginalLoopCarriage{state: state}, ActivityRequestOrigin{Request: request, Cuts: []ActivityRequestCut{grandchild, child, parent}}, node
}

func TestActivityOriginApprovalFollowsProducingDeclarationAcrossReplacementHops(t *testing.T) {
	for _, original := range []string{"", "approve-write"} {
		selected := "selected-approval"
		if original != "" {
			selected = ""
		}
		t.Run("original_"+original, func(t *testing.T) {
			carriage, origin, node := activityOriginCarriageFixture(t, original, selected)
			prepared, err := carriage.WithActivityOrigin(origin)
			if err != nil {
				t.Fatal(err)
			}
			got, err := prepared.ResolveOriginalActivityApproval(origin, node, "start", "write", "send")
			if err != nil || got != original {
				t.Fatalf("inherited approval=%q want origin=%q, selected=%q err=%v", got, original, selected, err)
			}
			if role, loop, err := prepared.ResolveOriginalActivity(origin, node, "start", "write", "send"); err != nil || !loop || role.LoopID() != "revision" {
				t.Fatalf("origin approval transport changed captured loop ownership: role=%+v loop=%t err=%v", role, loop, err)
			}
			if _, err := carriage.ResolveOriginalActivityApproval(origin, node, "start", "write", "send"); err == nil {
				t.Fatal("unprepared carriage acquired origin evidence by alias")
			}
			origin.Cuts[0].Revision++
			if _, err := prepared.ResolveOriginalActivityApproval(origin, node, "start", "write", "send"); err == nil {
				t.Fatal("caller mutation changed the retained preparation evidence")
			}

			// A genuinely new child request has no inherited request binding.
			fresh := origin
			fresh.Request = eventtest.RuntimeControl(uuid.NewString(), "platform.activity_requested", "activity-runtime", "", []byte(`{"input":{"value":7}}`), 0,
				origin.Cuts[0].RunID, "", events.EventEnvelope{}, origin.Request.CreatedAt())
			fresh.Cuts = []ActivityRequestCut{{RunID: fresh.Request.RunID(), RequestID: fresh.Request.ID(), Revision: 9, Source: origin.Cuts[0].Source}}
			prepared, err = prepared.WithActivityOrigin(fresh)
			if err != nil {
				t.Fatal(err)
			}
			got, err = prepared.ResolveOriginalActivityApproval(fresh, node, "start", "write", "send")
			if err != nil || got != selected {
				t.Fatalf("new child request inherited ancestor approval: got=%q want=%q err=%v", got, selected, err)
			}
		})
	}
}

func TestActivityOriginRefusesMissingCrossedCyclicAndChangedPreparation(t *testing.T) {
	carriage, original, node := activityOriginCarriageFixture(t, "approve-write", "")
	prepared, err := carriage.WithActivityOrigin(original)
	if err != nil {
		t.Fatal(err)
	}
	for _, fault := range []string{"absent_cuts", "missing_artifact", "different_artifact", "foreign_run", "foreign_request", "changed_revision", "changed_parent_point", "crossed_parent_cut", "cycle", "changed_payload", "foreign_site"} {
		t.Run(fault, func(t *testing.T) {
			origin := original
			origin.Cuts = slices.Clone(original.Cuts)
			handler := "start"
			switch fault {
			case "absent_cuts":
				origin.Cuts = nil
			case "missing_artifact":
				origin.Cuts[2].Source = correlation.SourceArtifactFact{}
			case "different_artifact":
				origin.Cuts[2].Source = origin.Cuts[0].Source
			case "foreign_run":
				origin.Cuts[2].RunID = uuid.NewString()
			case "foreign_request":
				origin.Cuts[2].RequestID = uuid.NewString()
			case "changed_revision":
				origin.Cuts[0].Revision++
			case "changed_parent_point":
				origin.Cuts[0].ParentPointEventID = uuid.NewString()
			case "crossed_parent_cut":
				origin.Cuts[0].ParentPointRevision++
			case "cycle":
				origin.Cuts[1].RunID, origin.Cuts[1].RequestID = origin.Cuts[0].RunID, origin.Cuts[0].RequestID
			case "changed_payload":
				origin.Request = eventtest.RuntimeControl(original.Request.ID(), original.Request.Type(), "activity-runtime", "", []byte(`{"input":{"value":8}}`), 0,
					original.Request.RunID(), "", events.EventEnvelope{}, original.Request.CreatedAt())
			case "foreign_site":
				handler = "foreign"
			}
			if _, err := prepared.ResolveOriginalActivityApproval(origin, node, handler, "write", "send"); err == nil {
				t.Fatalf("%s became approval evidence", fault)
			}
		})
	}
}
