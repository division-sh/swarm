package serveapp

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/store/storetest"
)

type issue2353H2StageCut struct {
	Entity, Instance, Stage string
	Revision                int64
	Transition              pipeline.WorkflowTransitionRecord
}

func issue2353H2StageCuts(rows []storetest.H2TransitionCutsEvidence) ([]issue2353H2StageCut, error) {
	var out []issue2353H2StageCut
	for _, row := range rows {
		var fact struct {
			Instance string `json:"flow_instance"`
			Stage    string `json:"current_state"`
			Config   struct {
				History []pipeline.WorkflowTransitionRecord `json:"transition_history"`
			} `json:"flow_config"`
		}
		if err := json.Unmarshal(row.Fact, &fact); err != nil {
			return nil, err
		}
		cut := issue2353H2StageCut{Entity: row.EntityID, Instance: fact.Instance, Stage: fact.Stage, Revision: row.Revision}
		if len(fact.Config.History) != 0 {
			cut.Transition = fact.Config.History[0]
		}
		out = append(out, cut)
	}
	slices.SortFunc(out, func(a, b issue2353H2StageCut) int {
		if a.Revision < b.Revision {
			return -1
		}
		if a.Revision > b.Revision {
			return 1
		}
		return 0
	})
	return out, nil
}

func issue2353H2BindCommitStage(commit storetest.H2CounterCommitEvidence, cuts []issue2353H2StageCut, hub issue2564H2Hub) (issue2353H2StageCut, error) {
	if commit.Revision <= 0 || commit.EntityID != hub.Entity || commit.EventID == "" || commit.MutationID == "" {
		return issue2353H2StageCut{}, fmt.Errorf("counter commit lacks exact identity/revision: %+v", commit)
	}
	var latest issue2353H2StageCut
	for _, cut := range cuts {
		if cut.Entity == commit.EntityID && cut.Revision <= commit.Revision && cut.Revision > latest.Revision {
			latest = cut
		}
	}
	if latest.Instance != hub.Instance || (latest.Stage != "s1" && latest.Stage != "s2") {
		return latest, fmt.Errorf("counter commit has no exact constructed stage: commit=%+v cut=%+v", commit, latest)
	}
	if (commit.Path == "c1" && latest.Stage != "s1") || (commit.Path == "c2" && latest.Stage != "s2") {
		return latest, fmt.Errorf("counter effect disagrees with committed stage: commit=%+v cut=%+v", commit, latest)
	}
	return latest, nil
}

func issue2353H2ObserveCommitStages(t *testing.T, rt issue2564H2Fixture, runID string, accepted map[string]string) {
	t.Helper()
	commits, err := storetest.ObserveH2CounterCommits(context.Background(), rt.selected, runID)
	if err != nil {
		t.Fatal(err)
	}
	evidence, err := storetest.ObserveH2WorkloadSnapshot(context.Background(), rt.selected, runID)
	if err != nil {
		t.Fatal(err)
	}
	cuts, err := issue2353H2StageCuts(evidence.TransitionCuts)
	if err != nil {
		t.Fatal(err)
	}
	entryRevisions := map[string]int64{}
	for _, cut := range cuts {
		event := cut.Transition.TriggerEventID
		if event != "" && entryRevisions[event] == 0 {
			entryRevisions[event] = cut.Revision
			t.Logf("H2_STAGE_COMMIT entity=%s instance=%s commit_revision=%d event=%s from=%s to=%s", cut.Entity, cut.Instance, cut.Revision, event, cut.Transition.From, cut.Transition.To)
		}
	}
	snapshot, err := issue2564H2Read(context.Background(), rt, runID)
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]map[string]int64{}
	for _, commit := range commits {
		hubID, bump := accepted[commit.EventID]
		if !bump {
			continue
		}
		cut, err := issue2353H2BindCommitStage(commit, cuts, snapshot.Hubs[hubID])
		if err != nil {
			t.Fatal(err)
		}
		if seen[commit.EventID] == nil {
			seen[commit.EventID] = map[string]int64{}
		}
		if seen[commit.EventID][commit.Path] != 0 {
			t.Fatalf("duplicated H2 counter commit: %+v", commit)
		}
		seen[commit.EventID][commit.Path] = commit.Revision
		t.Logf("H2_COMMIT_STAGE hub=%s event=%s mutation=%s path=%s commit_revision=%d header_revision=%d stage=%s entry_commit_revision=%d transition_event=%s transition_from=%s transition_to=%s", hubID, commit.EventID, commit.MutationID, commit.Path, commit.Revision, cut.Revision, cut.Stage, entryRevisions[cut.Transition.TriggerEventID], cut.Transition.TriggerEventID, cut.Transition.From, cut.Transition.To)
	}
	for event, hubID := range accepted {
		paths := seen[event]
		stageRevision := paths["c1"] + paths["c2"]
		if paths["count"] <= 0 || stageRevision != paths["count"] || (paths["c1"] > 0 && paths["c2"] > 0) {
			t.Fatalf("H2 bump lacks one coupled commit: event=%s hub=%s paths=%v", event, hubID, paths)
		}
		selections, err := storetest.ReadHandlerSelectionStorage(context.Background(), rt.selected, event)
		if err != nil || len(selections) != 1 {
			t.Fatalf("H2 exact committed bump selection: event=%s rows=%+v err=%v", event, selections, err)
		}
		label := "in_s2"
		if paths["c1"] > 0 {
			label = "in_s1"
		}
		if selections[0].Context != "handler_rules" || selections[0].Disposition != "selected" || selections[0].FlowPath != "hub" || selections[0].Family != "handler_rule" || selections[0].DisplayLabel != label {
			t.Fatalf("H2 exact rule does not agree with committed stage effect: event=%s paths=%v selection=%+v", event, paths, selections[0])
		}
		t.Logf("H2_BUMP_SELECTION hub=%s event=%s commit_revision=%d delivery=%s context=%s disposition=%s flow=%s family=%s semantic_path=%s label=%s", hubID, event, stageRevision, selections[0].DeliveryID, selections[0].Context, selections[0].Disposition, selections[0].FlowPath, selections[0].Family, selections[0].SemanticPath, selections[0].DisplayLabel)
	}
}

func TestIssue2353H2CommitStageDiagnosticRejectsWrongCuts(t *testing.T) {
	hub := issue2564H2Hub{Entity: "entity", Instance: "hub/h05"}
	commit := storetest.H2CounterCommitEvidence{MutationID: "mutation", EntityID: hub.Entity, EventID: "bump", Path: "c2", Revision: 12}
	cuts := []issue2353H2StageCut{
		{Entity: hub.Entity, Instance: hub.Instance, Stage: "s1", Revision: 3},
		{Entity: hub.Entity, Instance: hub.Instance, Stage: "s2", Revision: 10},
		{Entity: hub.Entity, Instance: hub.Instance, Stage: "s1", Revision: 13},
	}
	if cut, err := issue2353H2BindCommitStage(commit, cuts, hub); err != nil || cut.Revision != 10 {
		t.Fatalf("committed predecessor, not later/current snapshot, must own the bump: %+v %v", cut, err)
	}
	for _, name := range []string{"wrong_stage", "future_only", "foreign_instance", "missing_revision"} {
		t.Run(name, func(t *testing.T) {
			probe, rows := commit, slices.Clone(cuts)
			switch name {
			case "wrong_stage":
				probe.Path = "c1"
			case "future_only":
				rows = rows[2:]
			case "foreign_instance":
				rows[1].Instance = "hub/h06"
			case "missing_revision":
				probe.Revision = 0
			}
			if _, err := issue2353H2BindCommitStage(probe, rows, hub); err == nil {
				t.Fatal("invalid stage/revision evidence accepted")
			}
		})
	}
}
