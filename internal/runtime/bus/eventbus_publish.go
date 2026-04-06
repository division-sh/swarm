package bus

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"swarm/internal/events"
	"swarm/internal/runtime/core/eventidentity"
	runtimecorrelation "swarm/internal/runtime/correlation"
	runtimepipeline "swarm/internal/runtime/pipeline"
)

type pipelineTransitionSchemaCapabilityProvider interface {
	CanonicalEventReceiptsCapability(context.Context) (bool, error)
}

func shouldPersistPipelineReceipt(persisted bool, publishErr error) bool {
	if !persisted {
		return false
	}
	return !errors.Is(publishErr, errAuthoritativeDeliveryIncomplete)
}

func pipelineReceiptStatus(ctx context.Context, publishErr error) (string, string) {
	if publishErr != nil {
		return "error", publishErr.Error()
	}
	if overrideStatus, overrideErr, ok := runtimepipeline.PipelineReceiptOverrideFromContext(ctx); ok {
		return overrideStatus, overrideErr
	}
	return "processed", ""
}

func (eb *EventBus) Publish(ctx context.Context, evt events.Event) (err error) {
	ctx = WithCurrentRuntimeEpoch(ctx)
	if err := ensurePublishEpoch(ctx); err != nil {
		return err
	}
	eb.inFlightPublishes.Add(1)
	defer eb.inFlightPublishes.Add(-1)
	start := time.Now()
	if evt.Type == "" {
		return errors.New("event type is required")
	}
	if !isValidEventTypeName(string(evt.Type)) {
		return fmt.Errorf("invalid event type: %s", strings.TrimSpace(string(evt.Type)))
	}
	if eb.payloadValidator != nil {
		if err := eb.payloadValidator(string(evt.Type), evt.Payload); err != nil {
			return fmt.Errorf("%w for %s: %v", ErrPayloadValidation, strings.TrimSpace(string(evt.Type)), err)
		}
	}
	if evt.ID == "" {
		evt.ID = uuid.NewString()
	}
	if evt.CreatedAt.IsZero() {
		evt.CreatedAt = time.Now()
	}
	ictx, evt := runtimecorrelation.CorrelateEvent(ctx, evt)

	deferredTransitions := make([]runtimepipeline.DeferredPipelineTransition, 0, 8)
	postCommitActions := make([]func(), 0, 8)
	receiptOverride := &runtimepipeline.PipelineReceiptOverride{}
	ictx = runtimepipeline.WithPipelineTransitionCollector(ictx, &deferredTransitions, eb.pipelineTransitionCapability())
	ictx = runtimepipeline.WithPipelinePostCommitActions(ictx, &postCommitActions)
	ictx = runtimepipeline.WithPipelineReceiptOverride(ictx, receiptOverride)
	if txStore, ok := eb.store.(TransactionalEventStore); ok {
		return eb.publishTransactional(ictx, evt, start, &deferredTransitions, txStore)
	}

	persisted := false
	passthrough := true
	deferred := []events.Event{}
	defer func() {
		if !shouldPersistPipelineReceipt(persisted, err) {
			return
		}
		status, errText := pipelineReceiptStatus(ictx, err)
		eb.markPipelineReceipt(ictx, evt.ID, status, errText)
	}()

	// Interceptors execute before fan-out and can consume the event.
	// Deferred events are persisted after the inbound event commits.
	if pass, out, ierr := eb.runInterceptors(ictx, evt); ierr != nil {
		return ierr
	} else {
		passthrough = pass
		deferred = out
	}

	if passthrough {
		if err := eb.routeAndDeliver(ictx, evt); err != nil {
			return err
		}
		persisted = true
	} else {
		if err := eb.persistEventRecord(ictx, evt, nil); err != nil {
			return err
		}
		persisted = true
	}
	eb.logPublished(ictx, evt, int(time.Since(start)/time.Microsecond))
	runtimepipeline.FlushPipelinePostCommitActions(postCommitActions)
	runtimepipeline.FlushDeferredPipelineTransitions(ictx, deferredTransitions)
	for _, d := range deferred {
		if err := eb.publishDeferred(ictx, d); err != nil {
			return err
		}
	}
	return nil
}

func (eb *EventBus) pipelineTransitionCapability() func(context.Context) (bool, error) {
	if eb == nil || eb.store == nil {
		return nil
	}
	if provider, ok := eb.store.(pipelineTransitionSchemaCapabilityProvider); ok && provider != nil {
		return provider.CanonicalEventReceiptsCapability
	}
	return nil
}

func (eb *EventBus) publishTransactional(
	ctx context.Context,
	evt events.Event,
	start time.Time,
	deferredTransitions *[]runtimepipeline.DeferredPipelineTransition,
	txStore TransactionalEventStore,
) error {
	ctx = WithCurrentRuntimeEpoch(ctx)
	if err := ensurePublishEpoch(ctx); err != nil {
		return err
	}
	tx, err := txStore.BeginEventTx(ctx)
	if err != nil {
		return fmt.Errorf("begin publish tx: %w", err)
	}
	postCommitActions := make([]func(), 0, 8)
	txctx := runtimepipeline.WithPipelineSQLTxContext(ctx, tx)
	txctx = runtimepipeline.WithPipelinePostCommitActions(txctx, &postCommitActions)
	receiptOverride := &runtimepipeline.PipelineReceiptOverride{}
	txctx = runtimepipeline.WithPipelineReceiptOverride(txctx, receiptOverride)
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	if err := txStore.AppendEventTx(txctx, tx, evt); err != nil {
		return fmt.Errorf("persist event: %w", err)
	}

	inboundPlan, err := eb.deliveryPlanner.Plan(txctx, evt)
	if err != nil {
		return err
	}
	eb.recordPublishDiagnostic(txctx, evt, inboundPlan)
	if len(inboundPlan.PersistedRecipients) > 0 {
		if err := txStore.InsertEventDeliveriesTx(txctx, tx, evt.ID, inboundPlan.PersistedRecipients); err != nil {
			return fmt.Errorf("persist event deliveries: %w", err)
		}
	}

	passthrough, deferred, err := eb.runInterceptors(txctx, evt)
	if err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit publish tx: %w", err)
	}
	committed = true
	runtimepipeline.FlushPipelinePostCommitActions(postCommitActions)
	if deferredTransitions != nil {
		runtimepipeline.FlushDeferredPipelineTransitions(ctx, *deferredTransitions)
	}

	if passthrough {
		if len(inboundPlan.Recipients) > 0 {
			if err := eb.deliverToAgents(ctx, evt, inboundPlan.PersistedRecipients); err != nil {
				return err
			}
			eb.logDelivery(ctx, evt, inboundPlan.Recipients, inboundPlan.ExtraDetail)
		}
		if inboundPlan.BlockedByCycle && inboundPlan.CycleEscalation != nil {
			if err := eb.publishDeferred(ctx, *inboundPlan.CycleEscalation); err != nil {
				return err
			}
		}
		if strings.TrimSpace(inboundPlan.ContradictionReason) != "" {
			_ = eb.emitContradiction(ctx, evt, inboundPlan.ContradictionReason)
		}
	}
	eb.logPublished(ctx, evt, int(time.Since(start)/time.Microsecond))

	for _, d := range deferred {
		if err := eb.publishDeferred(ctx, d); err != nil {
			return err
		}
	}
	status, errText := pipelineReceiptStatus(txctx, nil)
	if err := txStore.UpsertPipelineReceiptTx(ctx, nil, evt.ID, status, errText); err != nil {
		return fmt.Errorf("persist pipeline receipt: %w", err)
	}
	return nil
}

func txTableExists(ctx context.Context, tx *sql.Tx, table string) bool {
	if tx == nil || strings.TrimSpace(table) == "" {
		return false
	}
	var ok bool
	if err := tx.QueryRowContext(ctx, `SELECT to_regclass($1) IS NOT NULL`, "public."+strings.TrimSpace(table)).Scan(&ok); err != nil {
		return false
	}
	return ok
}

func (eb *EventBus) runInterceptors(ctx context.Context, evt events.Event) (bool, []events.Event, error) {
	eb.mu.RLock()
	interceptors := append([]EventInterceptor(nil), eb.interceptors...)
	provider := eb.interceptorProvider
	eb.mu.RUnlock()
	if provider != nil {
		for _, it := range provider() {
			if it != nil {
				interceptors = append(interceptors, it)
			}
		}
	}
	if len(interceptors) == 0 {
		return true, nil, nil
	}
	passthrough := true
	deferred := make([]events.Event, 0, 4)
	for _, it := range interceptors {
		pass, out, err := it.Intercept(ctx, evt)
		if err != nil {
			return true, nil, err
		}
		if !pass {
			passthrough = false
		}
		for _, d := range out {
			if d.ID == "" {
				d.ID = uuid.NewString()
			}
			if d.CreatedAt.IsZero() {
				d.CreatedAt = time.Now()
			}
			deferred = append(deferred, d)
		}
	}
	return passthrough, deferred, nil
}

func (eb *EventBus) publishDeferred(ctx context.Context, evt events.Event) (err error) {
	ctx = WithCurrentRuntimeEpoch(ctx)
	if err := ensurePublishEpoch(ctx); err != nil {
		return err
	}
	receiptOverride := &runtimepipeline.PipelineReceiptOverride{}
	ctx = runtimepipeline.WithPipelineReceiptOverride(ctx, receiptOverride)
	eb.inFlightPublishes.Add(1)
	defer eb.inFlightPublishes.Add(-1)
	if evt.Type == "" {
		return errors.New("deferred event type is required")
	}
	if !isValidEventTypeName(string(evt.Type)) {
		return fmt.Errorf("invalid deferred event type: %s", strings.TrimSpace(string(evt.Type)))
	}
	if evt.ID == "" {
		evt.ID = uuid.NewString()
	}
	if evt.CreatedAt.IsZero() {
		evt.CreatedAt = time.Now()
	}
	ctx, evt = runtimecorrelation.CorrelateEvent(ctx, evt)
	if strings.TrimSpace(evt.SourceAgent) == "" {
		evt.SourceAgent = "runtime"
	}
	persisted := false
	defer func() {
		if !shouldPersistPipelineReceipt(persisted, err) {
			return
		}
		status, errText := pipelineReceiptStatus(ctx, err)
		eb.markPipelineReceipt(ctx, evt.ID, status, errText)
	}()
	plan, err := eb.deliveryPlanner.Plan(ctx, evt)
	if err != nil {
		return err
	}
	eb.recordPublishDiagnostic(ctx, evt, plan)
	if err := eb.persistEventRecord(ctx, evt, plan.PersistedRecipients); err != nil {
		return err
	}
	persisted = true
	passthrough, deferred, err := eb.runInterceptors(ctx, evt)
	if err != nil {
		return err
	}
	if passthrough {
		if len(plan.Recipients) > 0 {
			if err := eb.deliverToAgents(ctx, evt, plan.PersistedRecipients); err != nil {
				return err
			}
			eb.logDelivery(ctx, evt, plan.Recipients, plan.ExtraDetail)
		}
		if plan.BlockedByCycle && plan.CycleEscalation != nil {
			if err := eb.publishDeferred(ctx, *plan.CycleEscalation); err != nil {
				return err
			}
		}
		if strings.TrimSpace(plan.ContradictionReason) != "" {
			_ = eb.emitContradiction(ctx, evt, plan.ContradictionReason)
		}
	}
	eb.logPublished(ctx, evt, 0)
	for _, d := range deferred {
		if err := eb.publishDeferred(ctx, d); err != nil {
			return err
		}
	}
	return nil
}

func (eb *EventBus) publishDeferredNoIntercept(ctx context.Context, evt events.Event) (err error) {
	ctx = WithCurrentRuntimeEpoch(ctx)
	if err := ensurePublishEpoch(ctx); err != nil {
		return err
	}
	receiptOverride := &runtimepipeline.PipelineReceiptOverride{}
	ctx = runtimepipeline.WithPipelineReceiptOverride(ctx, receiptOverride)
	eb.inFlightPublishes.Add(1)
	defer eb.inFlightPublishes.Add(-1)
	if evt.Type == "" {
		return errors.New("deferred event type is required")
	}
	if !isValidEventTypeName(string(evt.Type)) {
		return fmt.Errorf("invalid deferred event type: %s", strings.TrimSpace(string(evt.Type)))
	}
	if evt.ID == "" {
		evt.ID = uuid.NewString()
	}
	if evt.CreatedAt.IsZero() {
		evt.CreatedAt = time.Now()
	}
	if strings.TrimSpace(evt.SourceAgent) == "" {
		evt.SourceAgent = "runtime"
	}
	persisted := false
	defer func() {
		if !shouldPersistPipelineReceipt(persisted, err) {
			return
		}
		status, errText := pipelineReceiptStatus(ctx, err)
		eb.markPipelineReceipt(ctx, evt.ID, status, errText)
	}()
	if err := eb.routeAndDeliver(ctx, evt); err != nil {
		return err
	}
	persisted = true
	eb.logPublished(ctx, evt, 0)
	return nil
}

func (eb *EventBus) logPublished(ctx context.Context, evt events.Event, durationUS int) {
	eb.logRuntime(ctx, "debug", "Event was published to the event bus", "eventbus", "published", evt.ID, string(evt.Type), evt.SourceAgent, evt.EntityID(), "", nil, map[string]any{
		"type":            string(evt.Type),
		"source":          evt.SourceAgent,
		"parent_event_id": strings.TrimSpace(evt.ParentEventID),
	}, "", durationUS)
}

func (eb *EventBus) routeAndDeliver(ctx context.Context, evt events.Event) error {
	plan, err := eb.deliveryPlanner.Plan(ctx, evt)
	if err != nil {
		return err
	}
	eb.recordPublishDiagnostic(ctx, evt, plan)
	if err := eb.persistEventRecord(ctx, evt, plan.PersistedRecipients); err != nil {
		return err
	}
	if len(plan.Recipients) > 0 {
		if err := eb.deliverToAgents(ctx, evt, plan.PersistedRecipients); err != nil {
			return err
		}
		eb.logDelivery(ctx, evt, plan.Recipients, plan.ExtraDetail)
	}
	if plan.BlockedByCycle && plan.CycleEscalation != nil {
		if err := eb.publishDeferred(ctx, *plan.CycleEscalation); err != nil {
			return err
		}
	}
	if strings.TrimSpace(plan.ContradictionReason) != "" {
		_ = eb.emitContradiction(ctx, evt, plan.ContradictionReason)
	}
	return nil
}

func (eb *EventBus) persistEventRecord(ctx context.Context, evt events.Event, recipients []string) error {
	recipients = uniqueStrings(recipients)
	if atomicStore, ok := eb.store.(AtomicEventPersistence); ok {
		if err := atomicStore.PersistEventWithDeliveries(ctx, evt, recipients); err != nil {
			return fmt.Errorf("persist event transaction: %w", err)
		}
		return nil
	}
	if err := eb.store.AppendEvent(ctx, evt); err != nil {
		return fmt.Errorf("persist event: %w", err)
	}
	if len(recipients) == 0 {
		return nil
	}
	if err := eb.store.InsertEventDeliveries(ctx, evt.ID, recipients); err != nil {
		return fmt.Errorf("persist event deliveries: %w", err)
	}
	return nil
}

func (eb *EventBus) logDelivery(ctx context.Context, evt events.Event, recipients []string, extra map[string]any) {
	detail := map[string]any{
		"recipients_count": len(recipients),
		"parent_event_id":  strings.TrimSpace(evt.ParentEventID),
	}
	for k, v := range extra {
		detail[k] = v
	}
	eb.logRuntime(ctx, "debug", "Event was delivered to recipients", "eventbus", "delivered", evt.ID, string(evt.Type), "", evt.EntityID(), "", nil, detail, "", 0)
}

func subscriberIDs(in []Subscriber) []string {
	if len(in) == 0 {
		return nil
	}
	out := make([]string, 0, len(in))
	for _, subscriber := range in {
		id := strings.TrimSpace(subscriber.ID)
		if id == "" {
			continue
		}
		out = append(out, id)
	}
	return uniqueStrings(out)
}

func publishDiagnosticRecipientMaps(in []PublishDiagnosticRecipient) []map[string]any {
	if len(in) == 0 {
		return nil
	}
	out := make([]map[string]any, 0, len(in))
	for _, recipient := range in {
		item := map[string]any{
			"id": recipient.ID,
		}
		if v := strings.TrimSpace(recipient.Type); v != "" {
			item["type"] = v
		}
		if v := strings.TrimSpace(recipient.Path); v != "" {
			item["path"] = v
		}
		if v := strings.TrimSpace(recipient.MatchedPattern); v != "" {
			item["matched_pattern"] = v
		}
		if v := strings.TrimSpace(recipient.RouteSource); v != "" {
			item["route_source"] = v
		}
		if v := strings.TrimSpace(recipient.LocalizedEvent); v != "" {
			item["localized_event"] = v
		}
		out = append(out, item)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func (eb *EventBus) describeSubscribersForEvent(eventType string, in []Subscriber) []PublishDiagnosticRecipient {
	if len(in) == 0 {
		return nil
	}
	out := make([]PublishDiagnosticRecipient, 0, len(in))
	for _, subscriber := range in {
		id := strings.TrimSpace(subscriber.ID)
		if id == "" {
			continue
		}
		item := PublishDiagnosticRecipient{
			ID:             id,
			Type:           strings.TrimSpace(subscriber.Type),
			Path:           strings.TrimSpace(subscriber.Path),
			MatchedPattern: strings.TrimSpace(subscriber.MatchPattern),
			RouteSource:    strings.TrimSpace(subscriber.RouteSource),
		}
		if localized := eb.localizedSubscriberEvent(eventType, subscriber); localized != "" {
			item.LocalizedEvent = localized
		}
		out = append(out, item)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func (eb *EventBus) localizedSubscriberEvent(eventType string, subscriber Subscriber) string {
	if strings.TrimSpace(subscriber.Type) != "node" {
		return ""
	}
	candidates := []string{eventType, subscriber.MatchPattern}
	if eb != nil && eb.semanticSource != nil {
		flowID := strings.TrimSpace(routeFlowIDForPath(eb.semanticSource, subscriber.Path))
		if flowID != "" {
			scope := eventidentity.Scope{
				Path:        strings.Trim(strings.TrimSpace(subscriber.Path), "/"),
				InputEvents: append([]string{}, eb.semanticSource.FlowInputEvents(flowID)...),
			}
			for _, candidate := range candidates {
				if localized := scope.LocalizeInput(candidate); localized != "" && localized != eventidentity.Normalize(candidate) {
					return localized
				}
			}
		}
	}
	for _, candidate := range candidates {
		normalized := eventidentity.Normalize(candidate)
		if leaf := eventidentity.LeafName(normalized); leaf != "" && leaf != normalized {
			return leaf
		}
	}
	return ""
}

func (eb *EventBus) recordPublishDiagnostic(ctx context.Context, evt events.Event, plan eventDeliveryPlan) {
	rec, ok := EmittedEventsRecorderFromContext(ctx)
	if !ok || rec == nil {
		return
	}
	rec.AppendPublish(PublishDiagnostic{
		EventID:                strings.TrimSpace(evt.ID),
		EventType:              strings.TrimSpace(string(evt.Type)),
		EntityID:               strings.TrimSpace(evt.EntityID()),
		ParentEventID:          strings.TrimSpace(evt.ParentEventID),
		RoutedRecipients:       eb.describeSubscribersForEvent(string(evt.Type), plan.RoutedRecipients),
		SubscriptionRecipients: uniqueStrings(plan.SubscribedRecipients),
	})
}

// PublishDirect persists an event and delivers it to the specified recipients
// regardless of routing tables or subscription patterns. This is the "message"
// primitive: explicit, point-to-point delivery.
func (eb *EventBus) PublishDirect(ctx context.Context, evt events.Event, recipients []string) (err error) {
	ctx = WithCurrentRuntimeEpoch(ctx)
	if err := ensurePublishEpoch(ctx); err != nil {
		return err
	}
	start := time.Now()
	persisted := false
	defer func() {
		if !shouldPersistPipelineReceipt(persisted, err) {
			return
		}
		status, errText := pipelineReceiptStatus(ctx, err)
		eb.markPipelineReceipt(ctx, evt.ID, status, errText)
	}()
	recipients = uniqueStrings(recipients)
	if len(recipients) == 0 {
		return errors.New("direct publish recipients are required")
	}
	if evt.Type == "" {
		return errors.New("event type is required")
	}
	if !isValidEventTypeName(string(evt.Type)) {
		return fmt.Errorf("invalid event type: %s", strings.TrimSpace(string(evt.Type)))
	}
	if eb.payloadValidator != nil {
		if err := eb.payloadValidator(string(evt.Type), evt.Payload); err != nil {
			return fmt.Errorf("%w for %s: %v", ErrPayloadValidation, strings.TrimSpace(string(evt.Type)), err)
		}
	}
	if evt.ID == "" {
		evt.ID = uuid.NewString()
	}
	if evt.CreatedAt.IsZero() {
		evt.CreatedAt = time.Now()
	}
	ctx, evt = runtimecorrelation.CorrelateEvent(ctx, evt)
	if err := eb.persistEventRecord(ctx, evt, recipients); err != nil {
		return err
	}
	persisted = true
	if err := eb.deliverToAgents(ctx, evt, recipients); err != nil {
		return err
	}
	eb.logRuntime(ctx, "debug", "Event was delivered directly to recipients", "eventbus", "delivered", evt.ID, string(evt.Type), "", evt.EntityID(), "", nil, map[string]any{
		"direct":           true,
		"recipients_count": len(recipients),
		"parent_event_id":  strings.TrimSpace(evt.ParentEventID),
	}, "", int(time.Since(start)/time.Microsecond))
	return nil
}
