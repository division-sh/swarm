package storetest

import (
	"context"
	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

type TemplateInstanceFieldsStorage = private.TemplateInstanceFieldsStorage
type TemplateAccountIdentityStorage = private.TemplateAccountIdentityStorage

type TemplateAccountDiagnosticStorage = private.TemplateAccountDiagnosticStorage
type TemplateReceiptDiagnosticStorage = private.TemplateReceiptDiagnosticStorage

type TemplateDeliveryDiagnosticStorage = private.TemplateDeliveryDiagnosticStorage

func ReadTemplateEventDeliveryDiagnostics(ctx context.Context, selected any, event string) ([]TemplateDeliveryDiagnosticStorage, error) {
	return private.ReadTemplateEventDeliveryDiagnosticsForTest(ctx, selected, event)
}

func ReadTemplateAccountDiagnostics(ctx context.Context, selected any) ([]TemplateAccountDiagnosticStorage, error) {
	return private.ReadTemplateAccountDiagnosticsForTest(ctx, selected)
}
func ReadTemplateEventReceiptDiagnostics(ctx context.Context, selected any, event string) ([]TemplateReceiptDiagnosticStorage, error) {
	return private.ReadTemplateEventReceiptDiagnosticsForTest(ctx, selected, event)
}

func CountTemplateNodeDelivery(ctx context.Context, selected any, event, node string) (int, error) {
	return private.CountTemplateNodeDeliveryForTest(ctx, selected, event, node)
}
func CountTemplateNodeDeliveryChronology(ctx context.Context, selected any, event, node string) (int, error) {
	return private.CountTemplateNodeDeliveryChronologyForTest(ctx, selected, event, node)
}
func CountTemplateNodeDeliveredAttempt(ctx context.Context, selected any, event, node string) (int, error) {
	return private.CountTemplateNodeDeliveredAttemptForTest(ctx, selected, event, node)
}
func CountTemplateAgentDelivery(ctx context.Context, selected any, event, agent string) (int, error) {
	return private.CountTemplateAgentDeliveryForTest(ctx, selected, event, agent)
}
func CountTemplatePlaceholderDelivery(ctx context.Context, selected any, event string) (int, error) {
	return private.CountTemplatePlaceholderDeliveryForTest(ctx, selected, event)
}
func CountTemplateEventDeliveries(ctx context.Context, selected any, event string) (int, error) {
	return private.CountTemplateEventDeliveriesForTest(ctx, selected, event)
}
func CountTemplateCarrierDeliveries(ctx context.Context, selected any, event string) (int, error) {
	return private.CountTemplateCarrierDeliveriesForTest(ctx, selected, event)
}
func CountPostgresTemplateNodeTarget(ctx context.Context, selected any, event, target, node string) (int, error) {
	return private.CountPostgresTemplateNodeTargetForTest(ctx, selected, event, target, node)
}
func CountPostgresTemplateDeliveredNodeTarget(ctx context.Context, selected any, event, node, target string) (int, error) {
	return private.CountPostgresTemplateDeliveredNodeTargetForTest(ctx, selected, event, node, target)
}
func CountTemplateNodeReceipt(ctx context.Context, selected any, event, node string) (int, error) {
	return private.CountTemplateNodeReceiptForTest(ctx, selected, event, node)
}
func CountTemplateSubscribedReplayScope(ctx context.Context, selected any, event string) (int, error) {
	return private.CountTemplateSubscribedReplayScopeForTest(ctx, selected, event)
}
func CountTemplateEventReplayScopes(ctx context.Context, selected any, event string) (int, error) {
	return private.CountTemplateEventReplayScopesForTest(ctx, selected, event)
}
func CountTemplateInstances(ctx context.Context, selected any, template string) (int, error) {
	return private.CountTemplateInstancesForTest(ctx, selected, template)
}
func CountRunOperatingInstances(ctx context.Context, selected any, run string) (int, error) {
	return private.CountRunOperatingInstancesForTest(ctx, selected, run)
}
func ReadRunOperatingInstanceFields(ctx context.Context, selected any, run string) (TemplateInstanceFieldsStorage, error) {
	return private.ReadRunOperatingInstanceFieldsForTest(ctx, selected, run)
}
func CountTemplateActiveAgentRoutes(ctx context.Context, selected any, agent, path string) (int, error) {
	return private.CountTemplateActiveAgentRoutesForTest(ctx, selected, agent, path)
}
func CountTemplatePlaceholderRoutes(ctx context.Context, selected any, path string) (int, error) {
	return private.CountTemplatePlaceholderRoutesForTest(ctx, selected, path)
}
func CountTemplatePrefixRoutingRules(ctx context.Context, selected any, template string) (int, error) {
	return private.CountTemplatePrefixRoutingRulesForTest(ctx, selected, template)
}
func CountPhysicalRoutingRules(ctx context.Context, selected any) (int, error) {
	return private.CountPhysicalRoutingRulesForTest(ctx, selected)
}
func CountTemplateAccountEntities(ctx context.Context, selected any) (int, error) {
	return private.CountTemplateAccountEntitiesForTest(ctx, selected)
}
func CountPostgresReviewedTemplateAccount(ctx context.Context, selected any, fields string) (int, error) {
	return private.CountPostgresReviewedTemplateAccountForTest(ctx, selected, fields)
}
func ReadLatestTemplateAccountIdentity(ctx context.Context, selected any) (TemplateAccountIdentityStorage, error) {
	return private.ReadLatestTemplateAccountIdentityForTest(ctx, selected)
}
func ReadTemplateNamedEventIdentity(ctx context.Context, selected any, name string) (string, error) {
	return private.ReadTemplateNamedEventIdentityForTest(ctx, selected, name)
}
func ReadTemplateSourceNamedEventIdentity(ctx context.Context, selected any, name, source string) (string, error) {
	return private.ReadTemplateSourceNamedEventIdentityForTest(ctx, selected, name, source)
}
func ReadTemplateEventPayloadText(ctx context.Context, selected any, event string) (string, error) {
	return private.ReadTemplateEventPayloadTextForTest(ctx, selected, event)
}
func CountTemplateEventIdentity(ctx context.Context, selected any, event string) (int, error) {
	return private.CountTemplateEventIdentityForTest(ctx, selected, event)
}
