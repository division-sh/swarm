package serveapp

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/packs"
	"github.com/division-sh/swarm/internal/runtime"
	runtimebootverify "github.com/division-sh/swarm/internal/runtime/bootverify"
	storebackend "github.com/division-sh/swarm/internal/store/backendselection"
)

type serveLifecycleWorkspaceFact struct {
	Context  string
	Decision cliapp.WorkspaceBackendSelection
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

type serveLifecycleNoticeKind string

const (
	serveLifecycleNoticeNoActiveWork      serveLifecycleNoticeKind = "no_active_work"
	serveLifecycleNoticeActiveWorkCleared serveLifecycleNoticeKind = "active_work_cleared"
	serveLifecycleNoticeUnavailableClosed serveLifecycleNoticeKind = "unavailable_work_closed"
)

type serveLifecycleNotice struct {
	Kind serveLifecycleNoticeKind
}

// serveLifecyclePresenter is the sole human presentation owner for serve
// boot, readiness, standing ingress, and shutdown facts.
type serveLifecyclePresenter struct {
	mu sync.Mutex

	out     io.Writer
	errOut  io.Writer
	dev     bool
	verbose bool

	bootEvents       map[int]runtime.BootProgressEvent
	store            *storebackend.Selection
	workspaces       []serveLifecycleWorkspaceFact
	warnings         []runtimebootverify.Finding
	operatorWarnings []string
	notices          []serveLifecycleNotice

	ready              bool
	failed             bool
	failure            *runtime.BootProgressEvent
	failureDiagnostic  string
	cleanupErr         error
	runtimeFailureErr  error
	runtimeFailureName string
	shutdownErr        error
	warningsWritten    bool
	bootWritten        bool
	terminalWritten    bool
}

func newServeLifecyclePresenter(opts cliapp.ServeOptions) *serveLifecyclePresenter {
	out := opts.Output
	if out == nil {
		out = io.Discard
	}
	errOut := opts.ErrorOutput
	if errOut == nil {
		errOut = out
	}
	return &serveLifecyclePresenter{
		out:        out,
		errOut:     errOut,
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
		copy := event
		p.failure = &copy
	}
}

func (p *serveLifecyclePresenter) fail(step int, name string, err error) {
	if err == nil {
		return
	}
	p.failWithDiagnostic(step, name, err, nil)
}

func (p *serveLifecyclePresenter) failWithDiagnostic(step int, name string, err error, render func(io.Writer) bool) {
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
	copy := event
	p.failure = &copy
	if render != nil {
		var rendered bytes.Buffer
		if render(&rendered) {
			p.failureDiagnostic = rendered.String()
		}
	}
}

func (p *serveLifecyclePresenter) writeFailureContextLocked(out io.Writer) {
	if !p.verbose && p.store != nil {
		if p.store.Backend == storebackend.BackendSQLite {
			fmt.Fprintf(out, "  store                      sqlite · %s\n", strings.TrimSpace(p.store.SQLitePath))
		} else {
			fmt.Fprintf(out, "  store                      %s · path not applicable\n", p.store.Backend.String())
		}
	}
	for _, detail := range p.workspaceDetailsLocked() {
		if detail == "not required" {
			continue
		}
		fmt.Fprintf(out, "  workspace                  %s\n", detail)
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

func (p *serveLifecyclePresenter) recordWorkspace(contextLabel string, decision cliapp.WorkspaceBackendSelection) {
	if p == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.workspaces = append(p.workspaces, serveLifecycleWorkspaceFact{
		Context:  strings.TrimSpace(contextLabel),
		Decision: decision,
	})
	if decision.UnsafeHost {
		p.operatorWarnings = append(p.operatorWarnings, "host workspace lets agents execute on this machine")
	}
}

func (p *serveLifecyclePresenter) recordDefaultAPITokenWarning() {
	if p == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.operatorWarnings = append(p.operatorWarnings, "using the built-in development API token on loopback; configure serve.api_token_file before exposing the listener")
}

func (p *serveLifecyclePresenter) recordBundleMatchDisabledWarning() {
	if p == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.operatorWarnings = append(p.operatorWarnings, "bundle matching is disabled for this startup")
}

func (p *serveLifecyclePresenter) recordAbandonedWork(runs, deliveries, pipelineReceipts int) {
	if p == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	kind := serveLifecycleNoticeNoActiveWork
	if runs > 0 || deliveries > 0 || pipelineReceipts > 0 {
		kind = serveLifecycleNoticeActiveWorkCleared
	}
	p.notices = append(p.notices, serveLifecycleNotice{Kind: kind})
}

func (p *serveLifecyclePresenter) recordClosedUnavailableWork() {
	if p == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.notices = append(p.notices, serveLifecycleNotice{Kind: serveLifecycleNoticeUnavailableClosed})
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
	p.commitReady(facts, nil)
}

func (p *serveLifecyclePresenter) commitReady(facts serveLifecycleReadyFacts, publish func()) bool {
	if p == nil {
		return false
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.failed || p.ready {
		return false
	}
	p.ready = true
	if !p.verbose {
		p.writeConciseReadyLocked(facts)
	} else {
		p.writeBootEventsLocked(p.out, runtime.BootProgressTotalSteps)
		p.writeResolvedFactsLocked(facts)
	}
	p.writeWarningsLocked()
	if publish != nil {
		publish()
	}
	return true
}

func (p *serveLifecyclePresenter) runtimeFailure(subject string, err error) {
	if p == nil || err == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.runtimeFailureErr == nil {
		p.runtimeFailureName = displayServeLifecycleName(subject)
	}
	p.runtimeFailureErr = errors.Join(p.runtimeFailureErr, err)
	if !p.ready && !p.failed {
		p.failed = true
		event := runtime.BootProgressEvent{
			Step:   runtime.BootProgressTotalSteps,
			Total:  runtime.BootProgressTotalSteps,
			Name:   strings.TrimSpace(subject),
			Status: "FAILED",
			At:     time.Now().UTC(),
		}
		p.bootEvents[event.Step] = event
		copy := event
		p.failure = &copy
	}
}

func (p *serveLifecyclePresenter) cleanupFailure(subject string, err error) {
	if p == nil || err == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.cleanupErr = errors.Join(p.cleanupErr, fmt.Errorf("%s: %w", displayServeLifecycleName(subject), err))
}

func (p *serveLifecyclePresenter) shutdown(err error) {
	if p == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if err != nil {
		if p.ready {
			p.shutdownErr = errors.Join(p.shutdownErr, err)
		} else {
			p.cleanupErr = errors.Join(p.cleanupErr, err)
		}
	}
}

func (p *serveLifecyclePresenter) finish() {
	if p == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.terminalWritten {
		return
	}
	p.terminalWritten = true
	p.writeWarningsLocked()

	if p.failed {
		if p.verbose {
			failureStep := runtime.BootProgressTotalSteps
			if p.failure != nil && p.failure.Step > 0 {
				failureStep = p.failure.Step
			}
			p.writeBootEventsLocked(p.errOut, failureStep)
		}
		if p.failureDiagnostic != "" {
			p.writeFailureContextLocked(p.errOut)
			_, _ = io.WriteString(p.errOut, p.failureDiagnostic)
			p.writeCleanupContextLocked()
			return
		}
		name := "startup"
		var primary error
		if p.failure != nil {
			name = p.failure.Name
			if strings.TrimSpace(p.failure.Detail) != "" {
				primary = errors.New(strings.TrimSpace(p.failure.Detail))
			}
		}
		p.writeFailureLocked(p.errOut, name, errors.Join(primary, p.runtimeFailureErr, p.cleanupErr))
		p.writeFailureContextLocked(p.errOut)
		return
	}
	if p.runtimeFailureErr != nil {
		detail := serveLifecycleErrorDetail(errors.Join(p.runtimeFailureErr, p.shutdownErr, p.cleanupErr))
		fmt.Fprintf(p.errOut, "ERROR: runtime failed · %s", p.runtimeFailureName)
		if detail != "" {
			fmt.Fprintf(p.errOut, " · %s", detail)
		}
		fmt.Fprintln(p.errOut)
		return
	}
	if p.ready {
		if err := errors.Join(p.shutdownErr, p.cleanupErr); err != nil {
			fmt.Fprintf(p.errOut, "ERROR: shutdown · failed · %s\n", serveLifecycleErrorDetail(err))
			return
		}
		fmt.Fprintln(p.out, "shutdown · complete")
		return
	}
	if p.cleanupErr != nil {
		fmt.Fprintf(p.errOut, "ERROR: serve failed · cleanup · %s\n", serveLifecycleErrorDetail(p.cleanupErr))
	}
}

func (p *serveLifecyclePresenter) writeBootEventsLocked(out io.Writer, throughStep int) {
	if p.bootWritten {
		return
	}
	p.bootWritten = true
	for step := 1; step <= throughStep && step <= runtime.BootProgressTotalSteps; step++ {
		event, ok := p.bootEvents[step]
		if !ok {
			continue
		}
		event.Step = step
		if canonical := runtime.CanonicalBootProgressName(step); canonical != "" {
			event.Name = canonical
		}
		p.writeBootEventLocked(out, event)
	}
}

func (p *serveLifecyclePresenter) writeBootEventLocked(out io.Writer, event runtime.BootProgressEvent) {
	detail := strings.TrimSpace(event.Detail)
	if detail != "" {
		detail = "  (" + detail + ")"
	}
	fmt.Fprintf(out, "[%d/%d] %-34s %s%s\n", event.Step, runtime.BootProgressTotalSteps, event.Name, event.Status, detail)
}

func (p *serveLifecyclePresenter) writeFailureLocked(out io.Writer, name string, err error) {
	fmt.Fprintf(out, "ERROR: serve failed · %s", displayServeLifecycleName(name))
	if detail := serveLifecycleErrorDetail(err); detail != "" {
		fmt.Fprintf(out, " · %s", detail)
	}
	fmt.Fprintln(out)
}

func (p *serveLifecyclePresenter) writeCleanupContextLocked() {
	if detail := serveLifecycleErrorDetail(errors.Join(p.runtimeFailureErr, p.cleanupErr)); detail != "" {
		fmt.Fprintf(p.errOut, "  cleanup                    %s\n", detail)
	}
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
	for _, notice := range p.notices {
		fmt.Fprintf(p.out, "  recovery action            %s\n", serveLifecycleNoticeDetail(notice))
	}
	if p.verbose {
		p.writeStandingIngressLocked(facts.Standing)
	}
}

func (p *serveLifecyclePresenter) workspaceDetailsLocked() []string {
	contextsByDetail := map[string][]string{}
	for _, fact := range p.workspaces {
		detail := serveLifecycleWorkspaceDetail(fact.Decision)
		contextsByDetail[detail] = append(contextsByDetail[detail], fact.Context)
	}
	if len(contextsByDetail) == 0 {
		return []string{"not required"}
	}
	details := make([]string, 0, len(contextsByDetail))
	for detail, contexts := range contextsByDetail {
		if len(contextsByDetail) > 1 {
			labels := serveUniqueSortedStrings(contexts)
			if len(labels) > 0 {
				detail = strings.Join(labels, ", ") + " · " + detail
			}
		}
		details = append(details, detail)
	}
	sort.Strings(details)
	return details
}

func (p *serveLifecyclePresenter) recoveryDetailLocked() (string, string) {
	event, ok := p.bootEvents[7]
	if !ok {
		return "clean start", ""
	}
	switch strings.TrimSpace(event.Detail) {
	case "recovery_disabled_no_persisted_work", "recovery_enabled_no_persisted_work", "no_active_run":
		return "clean start", ""
	case "recovery_enabled_with_persisted_work":
		return "persisted work recovery enabled", ""
	case "recovery_disabled_with_manager_snapshot_work":
		return "started without manager replay", ""
	}
	switch strings.ToLower(strings.TrimSpace(event.Status)) {
	case "clean_start":
		return "clean start", ""
	case "degraded":
		return "completed with warnings", ""
	case "skipped":
		return "not required", ""
	default:
		return "complete", ""
	}
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
	if p.warningsWritten {
		return
	}
	p.warningsWritten = true
	for _, warning := range serveUniqueSortedStrings(p.operatorWarnings) {
		fmt.Fprintf(p.errOut, "WARNING: %s\n", warning)
	}
	for _, finding := range p.warnings {
		fmt.Fprintln(p.errOut, runtimebootverify.FormatTypedDiagnosticFinding(runtimebootverify.TypedDiagnosticFinding{
			CheckID:     strings.TrimSpace(finding.CheckID),
			Severity:    strings.TrimSpace(finding.Severity),
			Location:    strings.TrimSpace(finding.Location),
			Message:     strings.TrimSpace(finding.Message),
			Remediation: strings.TrimSpace(finding.Remediation),
			Evidence:    append([]string(nil), finding.Evidence...),
		}, false))
	}
}

func serveLifecycleWorkspaceDetail(decision cliapp.WorkspaceBackendSelection) string {
	if decision.NoWorkspace || strings.TrimSpace(decision.Backend) == cliapp.WorkspaceBackendNone {
		return "not required"
	}
	backend := strings.TrimSpace(decision.Backend)
	if backend == "" {
		backend = "unknown"
	}
	agents := make([]string, 0, len(decision.Reasons))
	for _, reason := range decision.Reasons {
		if agent := strings.TrimSpace(reason.AgentID); agent != "" {
			agents = append(agents, agent)
		}
	}
	agents = serveUniqueSortedStrings(agents)
	subject := "agent work"
	verb := "runs"
	if len(agents) == 1 {
		subject = fmt.Sprintf("agent %q", agents[0])
	} else if len(agents) > 1 {
		subject = fmt.Sprintf("%d agents", len(agents))
		verb = "run"
	}
	switch backend {
	case "docker":
		container := "a container"
		if len(agents) > 1 {
			container = "containers"
		}
		return fmt.Sprintf("docker · %s %s in %s", subject, verb, container)
	case "host":
		return fmt.Sprintf("host · %s %s on this machine", subject, verb)
	default:
		return backend
	}
}

func serveLifecycleNoticeDetail(notice serveLifecycleNotice) string {
	switch notice.Kind {
	case serveLifecycleNoticeNoActiveWork:
		return "no active work to clear"
	case serveLifecycleNoticeActiveWorkCleared:
		return "active work cleared for a clean start"
	case serveLifecycleNoticeUnavailableClosed:
		return "unfinished work could not be resumed and was closed"
	default:
		return "completed"
	}
}

func serveLifecycleErrorDetail(err error) string {
	if err == nil {
		return ""
	}
	lines := strings.Split(strings.TrimSpace(err.Error()), "\n")
	parts := make([]string, 0, len(lines))
	for _, line := range lines {
		if line = strings.TrimSpace(line); line != "" {
			parts = append(parts, line)
		}
	}
	return strings.Join(parts, "; ")
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
