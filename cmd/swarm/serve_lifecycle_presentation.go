package main

import (
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/division-sh/swarm/internal/packs"
	"github.com/division-sh/swarm/internal/runtime"
	runtimebootverify "github.com/division-sh/swarm/internal/runtime/bootverify"
	storebackend "github.com/division-sh/swarm/internal/store/backendselection"
)

type serveLifecycleWorkspaceFact struct {
	Bundle   string
	Decision workspaceBackendSelection
}

type serveLifecycleIngressFact struct {
	Provider      string
	URL           string
	SigningSecret string
	SigningBound  bool
	BundleHash    string
	Subject       packs.Subject
}

type serveLifecycleReadyFacts struct {
	ProjectName string
	BundleCount int
	FlowCount   int
	AgentCount  int
	ToolCount   int
	APIListener string
	MCPListener string
	ReadyAfter  time.Duration
	Standing    []serveLifecycleIngressFact
}

// serveLifecyclePresenter is the sole human presentation owner for serve
// boot, readiness, standing ingress, and shutdown facts.
type serveLifecyclePresenter struct {
	mu sync.Mutex

	out     io.Writer
	dev     bool
	verbose bool

	bootEvents map[int]runtime.BootProgressEvent
	store      *storebackend.Selection
	workspaces []serveLifecycleWorkspaceFact
	warnings   []runtimebootverify.Finding
	notes      []string

	ready         bool
	failed        bool
	runtimeFailed bool
}

func newServeLifecyclePresenter(opts serveOptions) *serveLifecyclePresenter {
	out := opts.Output
	if out == nil {
		out = io.Discard
	}
	return &serveLifecyclePresenter{
		out:        out,
		dev:        opts.Dev,
		verbose:    opts.Verbose,
		bootEvents: map[int]runtime.BootProgressEvent{},
	}
}

func (p *serveLifecyclePresenter) runtimeSink() func(runtime.BootProgressEvent) {
	if p == nil {
		return nil
	}
	return p.observeBoot
}

func (p *serveLifecyclePresenter) boot(step int, name, status, detail string) {
	if p == nil {
		return
	}
	p.observeBoot(runtime.BootProgressEvent{
		Step:   step,
		Total:  runtime.BootProgressTotalSteps,
		Name:   strings.TrimSpace(name),
		Status: strings.TrimSpace(status),
		Detail: strings.TrimSpace(detail),
		At:     time.Now().UTC(),
	})
}

func (p *serveLifecyclePresenter) observeBoot(event runtime.BootProgressEvent) {
	if p == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()

	event.Name = strings.TrimSpace(event.Name)
	event.Status = strings.TrimSpace(event.Status)
	event.Detail = strings.TrimSpace(event.Detail)
	if event.Total == 0 {
		event.Total = runtime.BootProgressTotalSteps
	}
	if event.Status == "" {
		event.Status = "ok"
	}
	p.bootEvents[event.Step] = event
	if strings.EqualFold(event.Status, "failed") {
		if p.failed {
			return
		}
		p.failed = true
		if p.verbose {
			p.writeBootEventLocked(event)
		} else {
			p.writeFailureLocked(event.Name, event.Detail)
		}
		return
	}
	if p.verbose {
		p.writeBootEventLocked(event)
	}
}

func (p *serveLifecyclePresenter) fail(step int, name string, err error) {
	if err == nil {
		return
	}
	p.failWithDiagnostic(step, name, err, nil)
}

func (p *serveLifecyclePresenter) failWithDiagnostic(step int, name string, err error, render func(io.Writer)) {
	if p == nil || err == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.failed {
		return
	}
	p.failed = true
	event := runtime.BootProgressEvent{
		Step:   step,
		Total:  runtime.BootProgressTotalSteps,
		Name:   strings.TrimSpace(name),
		Status: "FAILED",
		Detail: strings.TrimSpace(err.Error()),
		At:     time.Now().UTC(),
	}
	p.bootEvents[step] = event
	if p.verbose {
		p.writeBootEventLocked(event)
	}
	if render != nil {
		p.writeFailureContextLocked()
		render(p.out)
		return
	}
	if !p.verbose {
		p.writeFailureLocked(event.Name, event.Detail)
	}
	p.writeFailureContextLocked()
}

func (p *serveLifecyclePresenter) writeFailureContextLocked() {
	if !p.verbose && p.store != nil {
		if p.store.Backend == storebackend.BackendSQLite {
			fmt.Fprintf(p.out, "  store                      sqlite · %s\n", strings.TrimSpace(p.store.SQLitePath))
		} else {
			fmt.Fprintf(p.out, "  store                      %s · path not applicable\n", p.store.Backend.String())
		}
	}
	for _, detail := range p.workspaceDetailsLocked() {
		if detail == "not required" {
			continue
		}
		fmt.Fprintf(p.out, "  workspace                  %s\n", detail)
	}
}

func (p *serveLifecyclePresenter) recordStore(selection storebackend.Selection) {
	if p == nil {
		return
	}
	p.mu.Lock()
	copy := selection
	p.store = &copy
	p.mu.Unlock()

	detail := "backend=" + selection.Backend.String()
	if selection.Backend == storebackend.BackendSQLite {
		detail += " path=" + strings.TrimSpace(selection.SQLitePath)
	} else {
		detail += " path=not_applicable"
	}
	p.boot(3, "db_connection", "ok", detail)
}

func (p *serveLifecyclePresenter) recordWorkspace(bundle string, decision workspaceBackendSelection) {
	if p == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.workspaces = append(p.workspaces, serveLifecycleWorkspaceFact{
		Bundle:   strings.TrimSpace(bundle),
		Decision: decision,
	})
}

func (p *serveLifecyclePresenter) note(message string) {
	if p == nil || strings.TrimSpace(message) == "" {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.notes = append(p.notes, strings.TrimSpace(message))
}

func (p *serveLifecyclePresenter) recordBootWarnings(report runtimebootverify.Report) {
	if p == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.warnings = append(p.warnings, report.Warnings()...)
}

func (p *serveLifecyclePresenter) readyPresentation(facts serveLifecycleReadyFacts) {
	if p == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.failed || p.ready {
		return
	}
	p.ready = true
	if !p.verbose {
		p.writeConciseReadyLocked(facts)
	} else {
		p.writeResolvedFactsLocked(facts)
	}
	p.writeWarningsLocked()
}

func (p *serveLifecyclePresenter) runtimeFailure(subject string, err error) {
	if p == nil || err == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.runtimeFailed = true
	fmt.Fprintf(p.out, "runtime failed · %s · %s\n", displayServeLifecycleName(subject), strings.TrimSpace(err.Error()))
}

func (p *serveLifecyclePresenter) shutdown(err error) {
	if p == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.ready {
		return
	}
	if err != nil {
		fmt.Fprintf(p.out, "shutdown · failed · %s\n", strings.TrimSpace(err.Error()))
		return
	}
	if p.runtimeFailed {
		return
	}
	fmt.Fprintln(p.out, "shutdown · complete")
}

func (p *serveLifecyclePresenter) writeBootEventLocked(event runtime.BootProgressEvent) {
	detail := strings.TrimSpace(event.Detail)
	if detail != "" {
		detail = "  (" + detail + ")"
	}
	fmt.Fprintf(p.out, "[%d/%d] %-34s %s%s\n", event.Step, runtime.BootProgressTotalSteps, event.Name, event.Status, detail)
}

func (p *serveLifecyclePresenter) writeFailureLocked(name, detail string) {
	fmt.Fprintf(p.out, "serve failed · %s", displayServeLifecycleName(name))
	if strings.TrimSpace(detail) != "" {
		fmt.Fprintf(p.out, " · %s", strings.TrimSpace(detail))
	}
	fmt.Fprintln(p.out)
}

func (p *serveLifecyclePresenter) writeConciseReadyLocked(facts serveLifecycleReadyFacts) {
	command := "swarm serve"
	if p.dev {
		command += " --dev"
	}
	project := strings.TrimSpace(facts.ProjectName)
	if project == "" {
		project = "runtime"
	}
	fmt.Fprintf(p.out, "%s · %s\n\n", command, project)
	bundleLabel := "bundle"
	if facts.BundleCount > 1 {
		bundleLabel = fmt.Sprintf("%d bundles", facts.BundleCount)
	}
	fmt.Fprintf(p.out, "  config · %-16s ready · %d %s · %d %s · %d %s\n",
		bundleLabel,
		facts.FlowCount, serveCountLabel(facts.FlowCount, "flow"),
		facts.AgentCount, serveCountLabel(facts.AgentCount, "agent"),
		facts.ToolCount, serveCountLabel(facts.ToolCount, "tool"),
	)
	p.writeResolvedFactsLocked(facts)
	fmt.Fprintf(p.out, "\n  ready in %s\n", facts.ReadyAfter.Round(time.Millisecond))
	if len(facts.Standing) > 0 {
		fmt.Fprintln(p.out)
	}
	p.writeStandingIngressLocked(facts.Standing)
	fmt.Fprintln(p.out)
}

func (p *serveLifecyclePresenter) writeResolvedFactsLocked(facts serveLifecycleReadyFacts) {
	if p.store != nil {
		if p.store.Backend == storebackend.BackendSQLite {
			fmt.Fprintf(p.out, "  store                      sqlite · %s\n", strings.TrimSpace(p.store.SQLitePath))
		} else {
			fmt.Fprintf(p.out, "  store                      %s · path not applicable\n", p.store.Backend.String())
		}
	}
	for _, detail := range p.workspaceDetailsLocked() {
		fmt.Fprintf(p.out, "  workspace                  %s\n", detail)
	}
	status, detail := p.recoveryDetailLocked()
	fmt.Fprintf(p.out, "  recovery                   %s", status)
	if detail != "" {
		fmt.Fprintf(p.out, " · %s", detail)
	}
	fmt.Fprintln(p.out)
	fmt.Fprintf(p.out, "  listeners                  api %s · mcp %s\n", strings.TrimSpace(facts.APIListener), strings.TrimSpace(facts.MCPListener))
	for _, note := range serveUniqueSortedStrings(p.notes) {
		fmt.Fprintf(p.out, "  note                       %s\n", note)
	}
	if p.verbose {
		p.writeStandingIngressLocked(facts.Standing)
	}
}

func (p *serveLifecyclePresenter) workspaceDetailsLocked() []string {
	seen := map[string]struct{}{}
	details := make([]string, 0, len(p.workspaces))
	for _, fact := range p.workspaces {
		detail := workspaceBackendDecisionDetail(fact.Decision)
		if fact.Bundle != "" && len(p.workspaces) > 1 {
			detail = fact.Bundle + " · " + detail
		}
		if _, ok := seen[detail]; ok {
			continue
		}
		seen[detail] = struct{}{}
		details = append(details, detail)
	}
	sort.Strings(details)
	if len(details) == 0 {
		return []string{"not required"}
	}
	return details
}

func (p *serveLifecyclePresenter) recoveryDetailLocked() (string, string) {
	event, ok := p.bootEvents[7]
	if !ok {
		return "complete", ""
	}
	status := strings.TrimSpace(event.Status)
	if status == "" {
		status = "complete"
	}
	return status, strings.TrimSpace(event.Detail)
}

func (p *serveLifecyclePresenter) writeStandingIngressLocked(facts []serveLifecycleIngressFact) {
	if len(facts) == 0 {
		fmt.Fprintln(p.out, "  standing ingress           none configured")
		return
	}
	sorted := append([]serveLifecycleIngressFact(nil), facts...)
	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i].Provider != sorted[j].Provider {
			return sorted[i].Provider < sorted[j].Provider
		}
		if sorted[i].URL != sorted[j].URL {
			return sorted[i].URL < sorted[j].URL
		}
		return sorted[i].BundleHash < sorted[j].BundleHash
	})
	for _, fact := range sorted {
		fmt.Fprintf(p.out, "  %-27s %s\n", strings.TrimSpace(fact.Provider)+" webhook", strings.TrimSpace(fact.URL))
		if strings.TrimSpace(fact.Subject.ID) != "" {
			fmt.Fprintf(p.out, "  standing ingress admitted: %s\n", packs.RenderSubject(fact.Subject, false))
		}
		if strings.TrimSpace(fact.SigningSecret) == "" {
			continue
		}
		state := "unbound"
		if fact.SigningBound {
			state = "bound"
		}
		fmt.Fprintf(p.out, "  %-27s %s %s\n", "signing", strings.TrimSpace(fact.SigningSecret), state)
	}
}

func (p *serveLifecyclePresenter) writeWarningsLocked() {
	for _, finding := range p.warnings {
		fmt.Fprintln(p.out, runtimebootverify.FormatTypedDiagnosticFinding(runtimebootverify.TypedDiagnosticFinding{
			CheckID:     strings.TrimSpace(finding.CheckID),
			Severity:    strings.TrimSpace(finding.Severity),
			Location:    strings.TrimSpace(finding.Location),
			Message:     strings.TrimSpace(finding.Message),
			Remediation: strings.TrimSpace(finding.Remediation),
			Evidence:    append([]string(nil), finding.Evidence...),
		}, false))
	}
}

func displayServeLifecycleName(value string) string {
	value = strings.TrimSpace(value)
	value = strings.ReplaceAll(value, "_", " ")
	if value == "" {
		return "startup"
	}
	return value
}

func serveCountLabel(count int, singular string) string {
	if count == 1 {
		return singular
	}
	return singular + "s"
}

func serveUniqueSortedStrings(values []string) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}
