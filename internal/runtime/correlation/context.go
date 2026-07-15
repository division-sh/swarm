package correlation

import (
	"context"
	"strings"

	"github.com/division-sh/swarm/internal/events"
)

type inboundEventContextKey struct{}
type runIDContextKey struct{}
type handlerIDContextKey struct{}
type runtimeLineageContextKey struct{}
type bundleFingerprintContextKey struct{}
type bundleSourceFactContextKey struct{}

type BundleSourceFact struct {
	BundleHash        string
	BundleSource      string
	BundleFingerprint string
}

type runtimeInstanceIDContextKey struct{}

func WithRuntimeInstanceID(ctx context.Context, runtimeInstanceID string) context.Context {
	runtimeInstanceID = strings.TrimSpace(runtimeInstanceID)
	if ctx == nil {
		ctx = context.Background()
	}
	if runtimeInstanceID == "" {
		return ctx
	}
	return context.WithValue(ctx, runtimeInstanceIDContextKey{}, runtimeInstanceID)
}

func RuntimeInstanceIDFromContext(ctx context.Context) (string, bool) {
	if ctx == nil {
		return "", false
	}
	runtimeInstanceID, ok := ctx.Value(runtimeInstanceIDContextKey{}).(string)
	runtimeInstanceID = strings.TrimSpace(runtimeInstanceID)
	return runtimeInstanceID, ok && runtimeInstanceID != ""
}

func (f BundleSourceFact) Normalized() BundleSourceFact {
	f.BundleHash = strings.TrimSpace(f.BundleHash)
	f.BundleSource = strings.TrimSpace(f.BundleSource)
	f.BundleFingerprint = strings.TrimSpace(f.BundleFingerprint)
	return f
}

func (f BundleSourceFact) Empty() bool {
	f = f.Normalized()
	return f.BundleHash == "" && f.BundleSource == "" && f.BundleFingerprint == ""
}

type RuntimeLineageRowCategory string

const (
	RuntimeLineageRowCategoryRuntimeContainer RuntimeLineageRowCategory = "runtime_container"
	RuntimeLineageRowCategoryDiagnostic       RuntimeLineageRowCategory = "diagnostic"
	RuntimeLineageRowCategoryPlatformControl  RuntimeLineageRowCategory = "platform_control"
)

type RuntimeLineageClassification string

const (
	RuntimeLineageClassificationForkLocal RuntimeLineageClassification = "fork_local"
	RuntimeLineageClassificationSource    RuntimeLineageClassification = "source"
	RuntimeLineageClassificationOperator  RuntimeLineageClassification = "operator"
)

// RuntimeLineage is the typed causal model runtime producers use before it is
// compiled down to existing persisted event lineage fields.
type RuntimeLineage struct {
	Owner               string
	RunID               string
	SubjectEventID      string
	SubjectEventType    string
	ParentEventID       string
	RowCategory         RuntimeLineageRowCategory
	SelectedForkOwner   string
	Classification      RuntimeLineageClassification
	SelectedForkContext bool
}

func (l RuntimeLineage) Normalized() RuntimeLineage {
	l.Owner = strings.TrimSpace(l.Owner)
	l.RunID = strings.TrimSpace(l.RunID)
	l.SubjectEventID = strings.TrimSpace(l.SubjectEventID)
	l.SubjectEventType = strings.TrimSpace(l.SubjectEventType)
	l.ParentEventID = strings.TrimSpace(l.ParentEventID)
	l.RowCategory = RuntimeLineageRowCategory(strings.TrimSpace(string(l.RowCategory)))
	l.SelectedForkOwner = strings.TrimSpace(l.SelectedForkOwner)
	l.Classification = RuntimeLineageClassification(strings.TrimSpace(string(l.Classification)))
	return l
}

func WithRuntimeLineage(ctx context.Context, lineage RuntimeLineage) context.Context {
	if ctx == nil {
		return nil
	}
	lineage = lineage.Normalized()
	if lineage.Owner == "" &&
		lineage.RunID == "" &&
		lineage.SubjectEventID == "" &&
		lineage.ParentEventID == "" &&
		lineage.RowCategory == "" &&
		lineage.SelectedForkOwner == "" &&
		lineage.Classification == "" &&
		!lineage.SelectedForkContext {
		return ctx
	}
	return context.WithValue(ctx, runtimeLineageContextKey{}, lineage)
}

func RuntimeLineageFromContext(ctx context.Context) (RuntimeLineage, bool) {
	if ctx == nil {
		return RuntimeLineage{}, false
	}
	v := ctx.Value(runtimeLineageContextKey{})
	if v == nil {
		return RuntimeLineage{}, false
	}
	lineage, ok := v.(RuntimeLineage)
	if !ok {
		return RuntimeLineage{}, false
	}
	lineage = lineage.Normalized()
	return lineage, true
}

func WithRuntimeLineageSubject(ctx context.Context, eventID, eventType string) context.Context {
	lineage, ok := RuntimeLineageFromContext(ctx)
	if !ok {
		return ctx
	}
	eventID = strings.TrimSpace(eventID)
	eventType = strings.TrimSpace(eventType)
	if eventID != "" {
		lineage.SubjectEventID = eventID
		if strings.TrimSpace(lineage.ParentEventID) == "" {
			lineage.ParentEventID = eventID
		}
	}
	if eventType != "" {
		lineage.SubjectEventType = eventType
	}
	return WithRuntimeLineage(ctx, lineage)
}

func WithRuntimeDiagnosticLineage(ctx context.Context, eventID, eventType string) context.Context {
	lineage, ok := RuntimeLineageFromContext(ctx)
	if !ok {
		return ctx
	}
	lineage.RowCategory = RuntimeLineageRowCategoryDiagnostic
	eventID = strings.TrimSpace(eventID)
	eventType = strings.TrimSpace(eventType)
	if eventID != "" {
		lineage.SubjectEventID = eventID
		lineage.ParentEventID = eventID
	}
	if eventType != "" {
		lineage.SubjectEventType = eventType
	}
	return WithRuntimeLineage(ctx, lineage)
}

func RuntimeLineageParentForEvent(ctx context.Context, eventID string) string {
	lineage, ok := RuntimeLineageFromContext(ctx)
	if !ok {
		return ""
	}
	eventID = strings.TrimSpace(eventID)
	if parentID := strings.TrimSpace(lineage.ParentEventID); parentID != "" && parentID != eventID {
		return parentID
	}
	if subjectID := strings.TrimSpace(lineage.SubjectEventID); subjectID != "" && subjectID != eventID {
		return subjectID
	}
	return ""
}

func WithInboundEvent(ctx context.Context, evt events.Event) context.Context {
	if ctx == nil {
		return nil
	}
	if lineage, ok := RuntimeLineageFromContext(ctx); ok {
		if eventID := evt.ID(); eventID != "" {
			lineage.SubjectEventID = eventID
			lineage.ParentEventID = eventID
		}
		if eventType := strings.TrimSpace(string(evt.Type())); eventType != "" {
			lineage.SubjectEventType = eventType
		}
		ctx = WithRuntimeLineage(ctx, lineage)
	}
	return context.WithValue(ctx, inboundEventContextKey{}, evt)
}

func InboundEventFromContext(ctx context.Context) (events.Event, bool) {
	if ctx == nil {
		return events.EmptyEvent(), false
	}
	v := ctx.Value(inboundEventContextKey{})
	if v == nil {
		return events.EmptyEvent(), false
	}
	evt, ok := v.(events.Event)
	return evt, ok
}

func WithRunID(ctx context.Context, runID string) context.Context {
	if ctx == nil {
		return nil
	}
	runID = strings.TrimSpace(runID)
	if runID == "" {
		return ctx
	}
	return context.WithValue(ctx, runIDContextKey{}, runID)
}

func RunIDFromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	runID, _ := ctx.Value(runIDContextKey{}).(string)
	return strings.TrimSpace(runID)
}

func WithHandlerID(ctx context.Context, handlerID string) context.Context {
	if ctx == nil {
		return nil
	}
	handlerID = strings.TrimSpace(handlerID)
	if handlerID == "" {
		return ctx
	}
	return context.WithValue(ctx, handlerIDContextKey{}, handlerID)
}

func HandlerIDFromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	handlerID, _ := ctx.Value(handlerIDContextKey{}).(string)
	return strings.TrimSpace(handlerID)
}

func WithBundleFingerprint(ctx context.Context, fingerprint string) context.Context {
	if ctx == nil {
		return nil
	}
	fingerprint = strings.TrimSpace(fingerprint)
	if fingerprint == "" {
		return ctx
	}
	return context.WithValue(ctx, bundleFingerprintContextKey{}, fingerprint)
}

func BundleFingerprintFromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	fingerprint, _ := ctx.Value(bundleFingerprintContextKey{}).(string)
	return strings.TrimSpace(fingerprint)
}

func WithBundleSourceFact(ctx context.Context, fact BundleSourceFact) context.Context {
	if ctx == nil {
		return nil
	}
	fact = fact.Normalized()
	if fact.Empty() {
		return ctx
	}
	ctx = context.WithValue(ctx, bundleSourceFactContextKey{}, fact)
	if fact.BundleFingerprint != "" {
		ctx = WithBundleFingerprint(ctx, fact.BundleFingerprint)
	}
	return ctx
}

func BundleSourceFactFromContext(ctx context.Context) (BundleSourceFact, bool) {
	if ctx == nil {
		return BundleSourceFact{}, false
	}
	fact, ok := ctx.Value(bundleSourceFactContextKey{}).(BundleSourceFact)
	if !ok {
		return BundleSourceFact{}, false
	}
	fact = fact.Normalized()
	if fact.Empty() {
		return BundleSourceFact{}, false
	}
	return fact, true
}

func CorrelateEvent(ctx context.Context, evt events.Event) (context.Context, events.Event) {
	runID := evt.RunID()
	if runID == "" {
		runID = RunIDFromContext(ctx)
	}
	if runID == "" {
		if lineage, ok := RuntimeLineageFromContext(ctx); ok {
			runID = strings.TrimSpace(lineage.RunID)
		}
	}
	if runID == "" {
		if inbound, ok := InboundEventFromContext(ctx); ok {
			runID = inbound.RunID()
		}
	}
	if runID != "" {
		ctx = WithRunID(ctx, runID)
	}

	parentEventID := evt.ParentEventID()
	if parentEventID == "" {
		if parentID := RuntimeLineageParentForEvent(ctx, evt.ID()); parentID != "" {
			parentEventID = parentID
		} else if inbound, ok := InboundEventFromContext(ctx); ok {
			parentID := inbound.ID()
			if parentID != "" && parentID != evt.ID() {
				parentEventID = parentID
			}
		}
	}
	return ctx, events.NewProjectionEvent(
		evt.ID(),
		evt.Type(),
		evt.SourceAgent(),
		evt.TaskID(),
		evt.Payload(),
		evt.ChainDepth(),
		runID,
		parentEventID,
		evt.NormalizedEnvelope(),
		evt.CreatedAt(),
	).WithExecutionMode(evt.ExecutionMode())
}
