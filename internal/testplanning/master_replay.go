package testplanning

import (
	"encoding/json"
	"fmt"
	"net/url"
	"strconv"
	"strings"
)

func CheckMasterPush(raw []byte, repository, ref, sha string) error {
	var event struct {
		Ref        string `json:"ref"`
		Before     string `json:"before"`
		After      string `json:"after"`
		Forced     *bool  `json:"forced"`
		Deleted    bool   `json:"deleted"`
		Created    bool   `json:"created"`
		Repository struct {
			FullName string `json:"full_name"`
		} `json:"repository"`
	}
	if err := json.Unmarshal(raw, &event); err != nil {
		return err
	}
	if event.Repository.FullName != repository || event.Ref != ref || event.After != sha || event.Forced == nil || *event.Forced || event.Deleted || event.Created || event.Before == "" || strings.Trim(event.Before, "0") == "" {
		return fmt.Errorf("not an ordinary observed push of the exact master source")
	}
	return nil
}

type MergedPR struct {
	Number         int    `json:"number"`
	Merged         bool   `json:"merged"`
	State          string `json:"state"`
	MergeCommitSHA string `json:"merge_commit_sha"`
	Body           string `json:"body"`
	Base           struct {
		Ref string `json:"ref"`
	} `json:"base"`
	Head struct {
		SHA  string `json:"sha"`
		Repo struct {
			FullName string `json:"full_name"`
		} `json:"repo"`
	} `json:"head"`
}

type QualifiedRun struct {
	ID         int64      `json:"id"`
	RunAttempt int        `json:"run_attempt"`
	HeadSHA    string     `json:"head_sha"`
	Event      string     `json:"event"`
	Name       string     `json:"name"`
	Status     string     `json:"status"`
	Conclusion string     `json:"conclusion"`
	Jobs       []MergeJob `json:"-"`
}

type MergeJob struct {
	ID          int64  `json:"id"`
	RunID       int64  `json:"run_id"`
	RunAttempt  int    `json:"run_attempt"`
	HeadSHA     string `json:"head_sha"`
	Name        string `json:"name"`
	Status      string `json:"status"`
	Conclusion  string `json:"conclusion"`
	CheckRunURL string `json:"check_run_url"`
}

type ProtectedCheck struct {
	Context string `json:"context"`
	AppID   int64  `json:"app_id"`
}

type MergeCheck struct {
	Name       string `json:"name"`
	HeadSHA    string `json:"head_sha"`
	Status     string `json:"status"`
	Conclusion string `json:"conclusion"`
	DetailsURL string `json:"details_url"`
	App        struct {
		ID int64 `json:"id"`
	} `json:"app"`
}

// ValidateMasterReplay accepts observed execution evidence, not a merge-policy
// exception. The caller obtains every fact from read-only GitHub APIs and Git.
func ValidateMasterReplay(repository, ref, masterSHA, masterTree, planTree string, prs []MergedPR, run QualifiedRun, checks []MergeCheck, protection []ProtectedCheck, plan RunPlan) error {
	pr, err := MergedAssociation(repository, ref, masterSHA, prs)
	if err != nil {
		return err
	}
	if masterTree == "" || planTree != masterTree {
		return fmt.Errorf("qualified execution tree differs from pushed master")
	}
	if err := plan.Validate(); err != nil {
		return err
	}
	required, _ := CITier(pr.Body)
	if plan.Venue != VenueCI || TierRank(plan.Profile) < TierRank(required) {
		return fmt.Errorf("qualified plan is thinner than current merged PR requirement")
	}
	if run.ID <= 0 || run.RunAttempt <= 0 || run.HeadSHA != pr.Head.SHA || run.Event != "pull_request" || run.Name != "CI" || run.Status != "completed" || run.Conclusion != "success" {
		return fmt.Errorf("latest PR qualification is not successful at this exact head")
	}
	return validateMergedChecks(repository, run, checks, protection)
}

func MergedAssociation(repository, ref, masterSHA string, prs []MergedPR) (MergedPR, error) {
	if repository != "division-sh/swarm" || ref != "refs/heads/master" || len(prs) != 1 {
		return MergedPR{}, fmt.Errorf("not one trusted master merge association")
	}
	pr := prs[0]
	if pr.Number <= 0 || pr.Head.SHA == "" || !pr.Merged || pr.State != "closed" || pr.Base.Ref != "master" || pr.MergeCommitSHA != masterSHA || pr.Head.Repo.FullName != repository {
		return MergedPR{}, fmt.Errorf("association is not the exact merged repository PR")
	}
	return pr, nil
}

func validateMergedChecks(repository string, run QualifiedRun, checks []MergeCheck, protection []ProtectedCheck) error {
	expected := map[string]bool{"Required test summary": false, "SQLite local smoke": false}
	if len(protection) != len(expected) {
		return fmt.Errorf("protected check set changed")
	}
	for _, check := range protection {
		seen, ok := expected[check.Context]
		if !ok || seen || check.AppID != 15368 {
			return fmt.Errorf("required context/App changed")
		}
		expected[check.Context] = true
	}
	for name := range expected {
		expected[name] = false
	}
	for _, check := range checks {
		seen, ok := expected[check.Name]
		if !ok || seen || check.App.ID != 15368 || check.HeadSHA != run.HeadSHA || check.Status != "completed" || check.Conclusion != "success" {
			return fmt.Errorf("required check has wrong context/App/head/outcome")
		}
		if err := validateMergedJob(repository, run, check); err != nil {
			return err
		}
		expected[check.Name] = true
	}
	for _, seen := range expected {
		if !seen {
			return fmt.Errorf("missing required check")
		}
	}
	return nil
}

func validateMergedJob(repository string, run QualifiedRun, check MergeCheck) error {
	link, err := url.Parse(check.DetailsURL)
	parts := strings.Split(strings.Trim(linkPath(link), "/"), "/")
	if err != nil || link == nil || link.Scheme != "https" || link.Host != "github.com" || len(parts) != 7 || strings.Join(parts[:2], "/") != repository || parts[2] != "actions" || parts[3] != "runs" || parts[4] != strconv.FormatInt(run.ID, 10) || parts[5] != "job" {
		return fmt.Errorf("required check is from another workflow run")
	}
	count := 0
	for _, job := range run.Jobs {
		if job.Name != check.Name {
			continue
		}
		if strconv.FormatInt(job.ID, 10) != parts[6] || job.RunID != run.ID || job.RunAttempt != run.RunAttempt || job.HeadSHA != run.HeadSHA || job.Status != "completed" || job.Conclusion != "success" {
			return fmt.Errorf("required job is from another run/attempt/head")
		}
		count++
	}
	if count != 1 {
		return fmt.Errorf("missing/duplicate required job")
	}
	return nil
}

func linkPath(link *url.URL) string {
	if link == nil {
		return ""
	}
	return link.Path
}
