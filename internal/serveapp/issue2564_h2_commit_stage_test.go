package serveapp

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"testing"

	"github.com/division-sh/swarm/internal/store/storetest"
)

type issue2564H2StageCut struct {
	Entity, Instance, Stage string
	Revision                int64
}

func issue2564H2StageCuts(rows []storetest.H2TransitionCutsEvidence) ([]issue2564H2StageCut, error) {
	var out []issue2564H2StageCut
	for _, row := range rows {
		var fact struct {
			Instance string `json:"flow_instance"`
			Stage    string `json:"current_state"`
		}
		if err := json.Unmarshal(row.Fact, &fact); err != nil {
			return nil, err
		}
		cut := issue2564H2StageCut{Entity: row.EntityID, Instance: fact.Instance, Stage: fact.Stage, Revision: row.Revision}
		out = append(out, cut)
	}
	return out, nil
}

func issue2564H2BindCommitStage(commit storetest.H2CounterCommitEvidence, cuts []issue2564H2StageCut, hub issue2564H2Hub) (issue2564H2StageCut, error) {
	if commit.Revision <= 0 || commit.EntityID != hub.Entity || commit.EventID == "" || commit.MutationID == "" {
		return issue2564H2StageCut{}, fmt.Errorf("counter commit lacks exact identity/revision: %+v", commit)
	}
	var latest issue2564H2StageCut
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

func issue2564H2CommittedStageEvidence(t *testing.T, rt issue2564H2Fixture, runID string, accepted map[string]string) {
	t.Helper()
	commits, err := storetest.ObserveH2CounterCommits(context.Background(), rt.selected, runID)
	if err != nil {
		t.Fatal(err)
	}
	evidence, err := storetest.ObserveH2WorkloadSnapshot(context.Background(), rt.selected, runID)
	if err != nil {
		t.Fatal(err)
	}
	cuts, err := issue2564H2StageCuts(evidence.TransitionCuts)
	if err != nil {
		t.Fatal(err)
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
		_, err := issue2564H2BindCommitStage(commit, cuts, snapshot.Hubs[hubID])
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
		if err := issue2564H2ValidateRule(selections[0], label); err != nil {
			t.Fatalf("H2 event=%s: %v", event, err)
		}
	}
	t.Logf("H2 exact committed-stage/rule evidence: bumps=%d", len(accepted))
}

func issue2564H2ValidateRule(selection storetest.HandlerSelectionStorageEvidence, label string) error {
	if selection.Context != "handler_rules" || selection.Disposition != "selected" || selection.FlowPath != "hub" || selection.Family != "handler_rule" || selection.DisplayLabel != label {
		return fmt.Errorf("H2 exact rule does not agree with committed stage effect: want=%s selection=%+v", label, selection)
	}
	return nil
}

func TestIssue2564H2CommitStageRejectsWrongEvidence(t *testing.T) {
	hub := issue2564H2Hub{Entity: "entity", Instance: "hub/h05"}
	commit := storetest.H2CounterCommitEvidence{MutationID: "mutation", EntityID: hub.Entity, EventID: "bump", Path: "c2", Revision: 12}
	cuts := []issue2564H2StageCut{
		{Entity: hub.Entity, Instance: hub.Instance, Stage: "s1", Revision: 3},
		{Entity: hub.Entity, Instance: hub.Instance, Stage: "s2", Revision: 10},
		{Entity: hub.Entity, Instance: hub.Instance, Stage: "s1", Revision: 13},
	}
	if cut, err := issue2564H2BindCommitStage(commit, cuts, hub); err != nil || cut.Revision != 10 {
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
			if _, err := issue2564H2BindCommitStage(probe, rows, hub); err == nil {
				t.Fatal("invalid stage/revision evidence accepted")
			}
		})
	}
	selection := storetest.HandlerSelectionStorageEvidence{Context: "handler_rules", Disposition: "selected", FlowPath: "hub", Family: "handler_rule", DisplayLabel: "in_s2"}
	if err := issue2564H2ValidateRule(selection, "in_s2"); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"wrong_rule", "wrong_context", "wrong_flow", "not_selected", "wrong_family"} {
		t.Run(name, func(t *testing.T) {
			probe := selection
			switch name {
			case "wrong_rule":
				probe.DisplayLabel = "in_s1"
			case "wrong_context":
				probe.Context = "transition"
			case "wrong_flow":
				probe.FlowPath = "foreign"
			case "not_selected":
				probe.Disposition = "rejected"
			case "wrong_family":
				probe.Family = "transition"
			}
			if issue2564H2ValidateRule(probe, "in_s2") == nil {
				t.Fatal("wrong committed rule accepted")
			}
		})
	}
}
