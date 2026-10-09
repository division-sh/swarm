package testplanning

import (
	"encoding/json"
	"testing"
)

func TestMasterReplayRejectsUnobservedOrForcedPush(t *testing.T) {
	event := map[string]any{"ref": "refs/heads/master", "before": "predecessor", "after": "current", "forced": false, "repository": map[string]string{"full_name": "division-sh/swarm"}}
	raw, _ := json.Marshal(event)
	if err := CheckMasterPush(raw, "division-sh/swarm", "refs/heads/master", "current"); err != nil {
		t.Fatal(err)
	}
	for _, mutation := range []string{"missing_forced", "forced", "created", "deleted", "before", "zero_before", "ref", "after", "repository", "malformed"} {
		t.Run(mutation, func(t *testing.T) {
			var candidate map[string]any
			_ = json.Unmarshal(raw, &candidate)
			switch mutation {
			case "missing_forced":
				delete(candidate, "forced")
			case "forced", "created", "deleted":
				candidate[mutation] = true
			case "before":
				delete(candidate, "before")
			case "zero_before":
				candidate["before"] = "0000000000000000000000000000000000000000"
			case "ref":
				candidate["ref"] = "refs/heads/main"
			case "after":
				candidate["after"] = "other"
			case "repository":
				candidate["repository"] = map[string]string{"full_name": "other/repo"}
			}
			data, _ := json.Marshal(candidate)
			if mutation == "malformed" {
				data = []byte("{")
			}
			if CheckMasterPush(data, "division-sh/swarm", "refs/heads/master", "current") == nil {
				t.Fatal("unverified push accepted")
			}
		})
	}
}

func TestMasterReplayRequiresExactTreeCurrentTierAndAttempt(t *testing.T) {
	plan, err := BuildPlan(testPolicy(), WeightModel{Version: WeightModelVersion, SourceRunID: "fixture"}, []string{"module/catalog"}, ProfileCore, "fixture", "execution")
	if err != nil {
		t.Fatal(err)
	}
	var pr MergedPR
	pr.Number = 7
	pr.Merged, pr.State, pr.MergeCommitSHA, pr.Body = true, "closed", "master", "CI-Tier: core"
	pr.Base.Ref, pr.Head.SHA, pr.Head.Repo.FullName = "master", "branch", "division-sh/swarm"
	run := QualifiedRun{ID: 123, RunAttempt: 2, HeadSHA: "branch", Event: "pull_request", Name: "CI", Status: "completed", Conclusion: "success"}
	var checks []MergeCheck
	var protected []ProtectedCheck
	for i, name := range []string{"Required test summary", "SQLite local smoke"} {
		check := MergeCheck{Name: name, HeadSHA: "branch", Status: "completed", Conclusion: "success", DetailsURL: "https://github.com/division-sh/swarm/actions/runs/123/job/" + []string{"40", "41"}[i]}
		check.App.ID = 15368
		checks = append(checks, check)
		protected = append(protected, ProtectedCheck{Context: name, AppID: 15368})
		run.Jobs = append(run.Jobs, MergeJob{ID: int64(40 + i), RunID: 123, RunAttempt: 2, HeadSHA: "branch", Name: name, Status: "completed", Conclusion: "success"})
	}
	if err := ValidateMasterReplay("division-sh/swarm", "refs/heads/master", "master", "tree", "tree", []MergedPR{pr}, run, checks, protected, plan); err != nil {
		t.Fatal(err)
	}
	for _, mutation := range []string{"main", "direct", "ambiguous", "unmerged", "foreign_repo", "merge_sha", "tree", "new_tier", "malformed_tier", "failed", "attempt", "foreign_head", "duplicate_job", "wrong_app", "wrong_protection", "wrong_run", "missing_check", "duplicate_check", "invalid_plan"} {
		t.Run(mutation, func(t *testing.T) {
			candidate := pr
			prs, reference, tree := []MergedPR{candidate}, "refs/heads/master", "tree"
			raw, _ := json.Marshal(run)
			var r QualifiedRun
			_ = json.Unmarshal(raw, &r)
			r.Jobs = append([]MergeJob(nil), run.Jobs...)
			cs, protection := append([]MergeCheck(nil), checks...), append([]ProtectedCheck(nil), protected...)
			p := plan
			switch mutation {
			case "main":
				reference = "refs/heads/main"
			case "direct":
				prs = nil
			case "ambiguous":
				prs = append(prs, pr)
			case "unmerged":
				prs[0].Merged = false
			case "foreign_repo":
				prs[0].Head.Repo.FullName = "foreign/repo"
			case "merge_sha":
				prs[0].MergeCommitSHA = "old"
			case "tree":
				tree = "different"
			case "new_tier":
				prs[0].Body = "CI-Tier: lifecycle"
			case "malformed_tier":
				prs[0].Body = "CI-Tier: unknown"
			case "failed":
				r.Conclusion = "failure"
			case "attempt":
				r.Jobs[0].RunAttempt--
			case "foreign_head":
				r.Jobs[0].HeadSHA = "other"
			case "duplicate_job":
				r.Jobs = append(r.Jobs, r.Jobs[0])
			case "wrong_app":
				cs[0].App.ID++
			case "wrong_protection":
				protection[0].AppID++
			case "wrong_run":
				cs[0].DetailsURL = "https://github.com/division-sh/swarm/actions/runs/122/job/40"
			case "missing_check":
				cs = cs[:1]
			case "duplicate_check":
				cs = append(cs, cs[0])
			case "invalid_plan":
				p.Digest = "stale"
			}
			if ValidateMasterReplay("division-sh/swarm", reference, "master", "tree", tree, prs, r, cs, protection, p) == nil {
				t.Fatal("uncertain master replay accepted")
			}
		})
	}
}
