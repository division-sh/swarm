package correlation

import (
	"context"
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/events"
	runtimebundleidentity "github.com/division-sh/swarm/internal/runtime/core/bundleidentity"
)

type inboundEventContextKey struct{}
type runIDContextKey struct{}
type handlerIDContextKey struct{}
type runtimeLineageContextKey struct{}
type sourceArtifactFactContextKey struct{}

type SourceArtifactFact struct {
	bundleHash string
}

func NewSourceArtifactFact(bundleHash string) (SourceArtifactFact, error) {
	return DecodeSourceArtifactFact(bundleHash)
}

func DecodeSourceArtifactFact(bundleHash string) (SourceArtifactFact, error) {
	if bundleHash != strings.TrimSpace(bundleHash) {
		return SourceArtifactFact{}, fmt.Errorf("bundle_hash must not contain surrounding whitespace")
	}
	if err := runtimebundleidentity.ValidateCanonicalHash(bundleHash); err != nil {
		return SourceArtifactFact{}, err
	}
	return SourceArtifactFact{bundleHash: bundleHash}, nil
}

func (f SourceArtifactFact) Validate() error {
	if f.bundleHash == "" {
		return fmt.Errorf("source artifact fact is required")
	}
	_, err := DecodeSourceArtifactFact(f.bundleHash)
	return err
}

func (f SourceArtifactFact) BundleHash() string {
	return f.bundleHash
}

func (f SourceArtifactFact) Matches(other SourceArtifactFact) bool {
	return f.Validate() == nil && other.Validate() == nil &&
		f.bundleHash == other.bundleHash
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
	var none events.Event
	if ctx == nil {
		return none, false
	}
	v := ctx.Value(inboundEventContextKey{})
	if v == nil {
		return none, false
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

func WithSourceArtifactFact(ctx context.Context, fact SourceArtifactFact) context.Context {
	if ctx == nil {
		return nil
	}
	if fact.Validate() != nil {
		return ctx
	}
	return context.WithValue(ctx, sourceArtifactFactContextKey{}, fact)
}

func SourceArtifactFactFromContext(ctx context.Context) (SourceArtifactFact, bool) {
	if ctx == nil {
		return SourceArtifactFact{}, false
	}
	fact, ok := ctx.Value(sourceArtifactFactContextKey{}).(SourceArtifactFact)
	if !ok {
		return SourceArtifactFact{}, false
	}
	if fact.Validate() != nil {
		return SourceArtifactFact{}, false
	}
	return fact, true
}
