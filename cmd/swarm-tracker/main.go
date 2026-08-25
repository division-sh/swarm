// Command swarm-tracker is the board-hygiene and sequencing tool for the
// division-sh/swarm issue tracker.
//
// GitHub remains the single source of truth for every fact it can hold
// (state, labels, milestone, agent). The facts GitHub has no field for —
// blockers and delivery score — live in a machine-readable "## tracker"
// block inside the issue body itself, so every agent and machine sees the
// same truth and nothing is stored locally.
//
//	swarm-tracker check                     board lint: missing labels/milestones,
//	                                        unblocked issues, stale musts, phantoms
//	swarm-tracker assign --issue 2340 --agent a --blockers 321,123 --score 50
//	swarm-tracker graph                     ready-now work ordered by unlocked score
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

const repo = "division-sh/swarm"

type label struct {
	Name string `json:"name"`
}

type milestone struct {
	Title string `json:"title"`
}

type issue struct {
	Number    int        `json:"number"`
	Title     string     `json:"title"`
	Body      string     `json:"body"`
	Labels    []label    `json:"labels"`
	Milestone *milestone `json:"milestone"`
	State     string     `json:"state"`
	UpdatedAt time.Time  `json:"updatedAt"`

	// Parsed from the "## tracker" body block.
	Blockers []int
	Score    int
}

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	var err error
	switch os.Args[1] {
	case "check":
		err = runCheck(os.Args[2:])
	case "assign":
		err = runAssign(os.Args[2:])
	case "graph":
		err = runGraph(os.Args[2:])
	default:
		usage()
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "swarm-tracker:", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, `usage:
  swarm-tracker check [--stale-days N] [--phantom-days N]
  swarm-tracker assign --issue N --agent X [--blockers N,N,...] [--score N]
  swarm-tracker graph [--agent X]`)
}

// --- gh plumbing -----------------------------------------------------------

func gh(args ...string) ([]byte, error) {
	cmd := exec.Command("gh", args...)
	var out, errb bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &errb
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("gh %s: %v: %s", strings.Join(args, " "), err, errb.String())
	}
	return out.Bytes(), nil
}

func fetchOpenIssues() ([]issue, error) {
	raw, err := gh("issue", "list", "--repo", repo, "--state", "open", "--limit", "500",
		"--json", "number,title,body,labels,milestone,state,updatedAt")
	if err != nil {
		return nil, err
	}
	var issues []issue
	if err := json.Unmarshal(raw, &issues); err != nil {
		return nil, fmt.Errorf("parse issue list: %w", err)
	}
	for i := range issues {
		issues[i].Blockers, issues[i].Score = parseTrackerBlock(issues[i].Body)
	}
	return issues, nil
}

func fetchIssueStates(numbers []int) (map[int]string, error) {
	states := map[int]string{}
	for _, n := range numbers {
		raw, err := gh("issue", "view", strconv.Itoa(n), "--repo", repo, "--json", "state")
		if err != nil {
			return nil, err
		}
		var v struct {
			State string `json:"state"`
		}
		if err := json.Unmarshal(raw, &v); err != nil {
			return nil, err
		}
		states[n] = v.State
	}
	return states, nil
}

// --- tracker block ---------------------------------------------------------

var (
	blockRe    = regexp.MustCompile(`(?s)## tracker\s*\x60\x60\x60yaml\n(.*?)\x60\x60\x60`)
	blockersRe = regexp.MustCompile(`blockers:\s*\[([0-9,\s]*)\]`)
	scoreRe    = regexp.MustCompile(`score:\s*([0-9]+)`)
)

func parseTrackerBlock(body string) (blockers []int, score int) {
	m := blockRe.FindStringSubmatch(body)
	if m == nil {
		return nil, 0
	}
	if bm := blockersRe.FindStringSubmatch(m[1]); bm != nil {
		for _, part := range strings.Split(bm[1], ",") {
			part = strings.TrimSpace(part)
			if part == "" {
				continue
			}
			if n, err := strconv.Atoi(part); err == nil {
				blockers = append(blockers, n)
			}
		}
	}
	if sm := scoreRe.FindStringSubmatch(m[1]); sm != nil {
		score, _ = strconv.Atoi(sm[1])
	}
	return blockers, score
}

func renderTrackerBlock(blockers []int, score int) string {
	parts := make([]string, len(blockers))
	for i, b := range blockers {
		parts[i] = strconv.Itoa(b)
	}
	return fmt.Sprintf("## tracker\n```yaml\nblockers: [%s]\nscore: %d\n```", strings.Join(parts, ", "), score)
}

func upsertTrackerBlock(body string, blockers []int, score int) string {
	block := renderTrackerBlock(blockers, score)
	if blockRe.MatchString(body) {
		return blockRe.ReplaceAllString(body, block)
	}
	if strings.TrimSpace(body) == "" {
		return block
	}
	return strings.TrimRight(body, "\n") + "\n\n" + block
}

// --- labels ----------------------------------------------------------------

func labelSet(is issue) map[string]bool {
	set := map[string]bool{}
	for _, l := range is.Labels {
		set[strings.ToLower(l.Name)] = true
	}
	return set
}

func hasPrefix(set map[string]bool, prefix string) bool {
	for name := range set {
		if strings.HasPrefix(name, prefix) {
			return true
		}
	}
	return false
}

func agentOf(is issue) string {
	for _, l := range is.Labels {
		name := strings.ToLower(l.Name)
		if strings.HasPrefix(name, "agent:") && name != "agent:unassigned" {
			return strings.TrimPrefix(name, "agent:")
		}
		if strings.HasPrefix(name, "agent-") {
			return strings.TrimPrefix(name, "agent-")
		}
	}
	return ""
}

func isMust(set map[string]bool) bool {
	return set["tier:must"] || set["priority:p0"] || set["priority:p1"]
}

// --- check -----------------------------------------------------------------

func runCheck(args []string) error {
	fs := flag.NewFlagSet("check", flag.ExitOnError)
	staleDays := fs.Int("stale-days", 30, "flag must/P0/P1 issues untouched this many days")
	phantomDays := fs.Int("phantom-days", 10, "flag agent-assigned issues untouched this many days")
	fs.Parse(args)

	issues, err := fetchOpenIssues()
	if err != nil {
		return err
	}
	open := map[int]issue{}
	for _, is := range issues {
		open[is.Number] = is
	}

	// Collect every referenced blocker not in the open set: closed or missing.
	var external []int
	seen := map[int]bool{}
	for _, is := range issues {
		for _, b := range is.Blockers {
			if _, ok := open[b]; !ok && !seen[b] {
				external = append(external, b)
				seen[b] = true
			}
		}
	}
	externalStates, err := fetchIssueStates(external)
	if err != nil {
		return err
	}

	section := func(title string, lines []string) {
		if len(lines) == 0 {
			return
		}
		fmt.Printf("\n%s (%d)\n", title, len(lines))
		for _, l := range lines {
			fmt.Println("  " + l)
		}
	}

	var missingMilestone, missingAgent, missingComplexity, missingPriority []string
	var unblocked, staleMusts, phantoms, cycles, unassignedMusts []string

	now := time.Now()
	for _, is := range issues {
		set := labelSet(is)
		ref := fmt.Sprintf("#%-5d %s", is.Number, is.Title)
		if is.Milestone == nil {
			missingMilestone = append(missingMilestone, ref)
		}
		if !hasPrefix(set, "agent") {
			missingAgent = append(missingAgent, ref)
		}
		if !hasPrefix(set, "complexity:") {
			missingComplexity = append(missingComplexity, ref)
		}
		if !hasPrefix(set, "priority:") {
			missingPriority = append(missingPriority, ref)
		}
		if isMust(set) && agentOf(is) == "" {
			unassignedMusts = append(unassignedMusts, ref)
		}
		if isMust(set) && now.Sub(is.UpdatedAt) > time.Duration(*staleDays)*24*time.Hour {
			staleMusts = append(staleMusts,
				fmt.Sprintf("%s  (untouched %dd)", ref, int(now.Sub(is.UpdatedAt).Hours()/24)))
		}
		if a := agentOf(is); a != "" && now.Sub(is.UpdatedAt) > time.Duration(*phantomDays)*24*time.Hour {
			phantoms = append(phantoms,
				fmt.Sprintf("%s  (agent:%s, untouched %dd)", ref, a, int(now.Sub(is.UpdatedAt).Hours()/24)))
		}
		if len(is.Blockers) > 0 {
			allClear := true
			for _, b := range is.Blockers {
				if _, stillOpen := open[b]; stillOpen {
					allClear = false
				} else if externalStates[b] != "CLOSED" {
					allClear = false // missing/unknown: not proof of unblocked
				}
			}
			if allClear {
				unblocked = append(unblocked, fmt.Sprintf("%s  (blockers all closed: %v)", ref, is.Blockers))
			}
		}
	}

	for _, cyc := range findCycles(issues) {
		cycles = append(cycles, fmt.Sprintf("%v", cyc))
	}

	section("UNBLOCKED — blockers all closed, ready to start", unblocked)
	section("UNASSIGNED MUSTS — P0/P1/tier:must with no owner", unassignedMusts)
	section("STALE MUSTS — tier:must/P0/P1 going forgotten", staleMusts)
	section("PHANTOM ASSIGNMENTS — assigned but silent", phantoms)
	section("BLOCKER CYCLES", cycles)
	section("missing milestone", missingMilestone)
	section("missing agent label", missingAgent)
	section("missing complexity", missingComplexity)
	section("missing priority", missingPriority)

	fmt.Printf("\n%d open issues checked.\n", len(issues))
	return nil
}

// --- assign ----------------------------------------------------------------

func runAssign(args []string) error {
	fs := flag.NewFlagSet("assign", flag.ExitOnError)
	num := fs.Int("issue", 0, "issue number")
	agent := fs.String("agent", "", "agent (a, b, c, ... or ds, ds2)")
	blockersFlag := fs.String("blockers", "", "comma-separated blocker issue numbers")
	score := fs.Int("score", 0, "delivery score")
	fs.Parse(args)
	if *num == 0 {
		return fmt.Errorf("assign requires --issue")
	}
	provided := map[string]bool{}
	fs.Visit(func(f *flag.Flag) { provided[f.Name] = true })

	var blockers []int
	if *blockersFlag != "" {
		for _, part := range strings.Split(*blockersFlag, ",") {
			n, err := strconv.Atoi(strings.TrimSpace(part))
			if err != nil {
				return fmt.Errorf("bad blocker %q", part)
			}
			blockers = append(blockers, n)
		}
	}

	raw, err := gh("issue", "view", strconv.Itoa(*num), "--repo", repo, "--json", "body,labels")
	if err != nil {
		return err
	}
	var cur issue
	if err := json.Unmarshal(raw, &cur); err != nil {
		return err
	}
	curBlockers, curScore := parseTrackerBlock(cur.Body)
	if !provided["blockers"] {
		blockers = curBlockers
	}
	if !provided["score"] {
		*score = curScore
	}

	editArgs := []string{"issue", "edit", strconv.Itoa(*num), "--repo", repo}
	agentLabel := "(unchanged)"
	if provided["agent"] {
		// One agent label at a time: drop any existing agent labels first.
		agentLabel = "agent:" + strings.ToUpper(*agent)
		if len(*agent) > 1 { // ds, ds2 style lanes keep their case
			agentLabel = "agent:" + *agent
		}
		editArgs = append(editArgs, "--add-label", agentLabel)
		for _, l := range cur.Labels {
			low := strings.ToLower(l.Name)
			if (strings.HasPrefix(low, "agent:") || strings.HasPrefix(low, "agent-")) && !strings.EqualFold(l.Name, agentLabel) {
				editArgs = append(editArgs, "--remove-label", l.Name)
			}
		}
	}

	body := upsertTrackerBlock(cur.Body, blockers, *score)
	tmp, err := os.CreateTemp("", "tracker-body-*.md")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	if _, err := tmp.WriteString(body); err != nil {
		return err
	}
	tmp.Close()
	editArgs = append(editArgs, "--body-file", tmp.Name())

	if _, err := gh(editArgs...); err != nil {
		return err
	}
	fmt.Printf("#%d → %s  blockers=%v score=%d\n", *num, agentLabel, blockers, *score)
	return nil
}

// --- graph -----------------------------------------------------------------

func runGraph(args []string) error {
	fs := flag.NewFlagSet("graph", flag.ExitOnError)
	agentFilter := fs.String("agent", "", "only show issues for this agent")
	fs.Parse(args)

	issues, err := fetchOpenIssues()
	if err != nil {
		return err
	}
	open := map[int]issue{}
	for _, is := range issues {
		open[is.Number] = is
	}

	// blockedBy edges restricted to OPEN blockers; closed blockers are satisfied.
	dependents := map[int][]int{} // blocker -> issues it blocks
	openBlockers := map[int][]int{}
	for _, is := range issues {
		for _, b := range is.Blockers {
			if _, ok := open[b]; ok {
				openBlockers[is.Number] = append(openBlockers[is.Number], b)
				dependents[b] = append(dependents[b], is.Number)
			}
		}
	}

	// Unlocked score: own score plus everything transitively waiting on this issue.
	memo := map[int]int{}
	var unlocked func(n int, visiting map[int]bool) int
	unlocked = func(n int, visiting map[int]bool) int {
		if v, ok := memo[n]; ok {
			return v
		}
		if visiting[n] {
			return 0 // cycle guard; reported separately
		}
		visiting[n] = true
		total := open[n].Score
		for _, d := range dependents[n] {
			total += unlocked(d, visiting)
		}
		delete(visiting, n)
		memo[n] = total
		return total
	}

	type row struct {
		is       issue
		unlocked int
	}
	var ready, blocked []row
	for _, is := range issues {
		if *agentFilter != "" && !strings.EqualFold(agentOf(is), *agentFilter) {
			continue
		}
		r := row{is: is, unlocked: unlocked(is.Number, map[int]bool{})}
		if len(openBlockers[is.Number]) == 0 && (is.Score > 0 || len(dependents[is.Number]) > 0) {
			ready = append(ready, r)
		} else if len(openBlockers[is.Number]) > 0 {
			blocked = append(blocked, r)
		}
	}
	sort.Slice(ready, func(i, j int) bool { return ready[i].unlocked > ready[j].unlocked })
	sort.Slice(blocked, func(i, j int) bool { return blocked[i].unlocked > blocked[j].unlocked })

	fmt.Println("READY NOW — sorted by unlocked delivery score")
	for _, r := range ready {
		fmt.Printf("  #%-5d score=%-4d unlocks=%-5d %s  %s\n",
			r.is.Number, r.is.Score, r.unlocked, pad(agentOf(r.is)), r.is.Title)
	}
	fmt.Println("\nBLOCKED — waiting on open work")
	for _, r := range blocked {
		fmt.Printf("  #%-5d score=%-4d unlocks=%-5d %s  %s  ⟵ %v\n",
			r.is.Number, r.is.Score, r.unlocked, pad(agentOf(r.is)), r.is.Title, openBlockers[r.is.Number])
	}
	for _, cyc := range findCycles(issues) {
		fmt.Printf("\nCYCLE: %v\n", cyc)
	}
	return nil
}

func pad(agent string) string {
	if agent == "" {
		agent = "-"
	}
	return fmt.Sprintf("[%-4s]", agent)
}

// --- cycles ----------------------------------------------------------------

func findCycles(issues []issue) [][]int {
	open := map[int]issue{}
	for _, is := range issues {
		open[is.Number] = is
	}
	var cycles [][]int
	state := map[int]int{} // 0 unvisited, 1 in-stack, 2 done
	var stack []int
	var visit func(n int)
	visit = func(n int) {
		state[n] = 1
		stack = append(stack, n)
		for _, b := range open[n].Blockers {
			if _, ok := open[b]; !ok {
				continue
			}
			switch state[b] {
			case 0:
				visit(b)
			case 1:
				for i, s := range stack {
					if s == b {
						cycles = append(cycles, append([]int{}, stack[i:]...))
						break
					}
				}
			}
		}
		stack = stack[:len(stack)-1]
		state[n] = 2
	}
	for n := range open {
		if state[n] == 0 {
			visit(n)
		}
	}
	return cycles
}
