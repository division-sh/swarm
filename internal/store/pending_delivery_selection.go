package store

import "fmt"

func RequireCanonicalPendingAgentDeliveryCapabilities(caps StoreSchemaCapabilities) error {
	switch {
	case caps.Events.Log != SchemaFlavorCanonical:
		return unsupportedSchemaCapability("events", caps.Events.Log)
	case caps.Events.Deliveries != SchemaFlavorCanonical:
		return unsupportedSchemaCapability("event_deliveries", caps.Events.Deliveries)
	case caps.Events.Receipts != SchemaFlavorCanonical:
		return unsupportedSchemaCapability("event_receipts", caps.Events.Receipts)
	default:
		return nil
	}
}

func CanonicalPendingAgentDeliveryPredicateSQL(deliveryAlias, receiptAlias string) string {
	return fmt.Sprintf(`(
		(
			%s.delivery_id IS NOT NULL
			AND (
				%s.status IN ('pending', 'in_progress')
				OR (
					%s.status = 'failed'
					AND COALESCE(%s.retry_count, 0) < 2
					AND COALESCE(%s.delivered_at, %s.created_at) <= now() - interval '1 minute'
				)
			)
		)
		OR (
			%s.delivery_id IS NULL
			AND (
				%s.event_id IS NULL
				OR (
					COALESCE(%s.side_effects->>'manager_status', '') = 'error'
					AND COALESCE((%s.side_effects->>'retry_count')::int, 0) < 2
					AND %s.processed_at <= now() - interval '1 minute'
				)
			)
		)
	)`,
		deliveryAlias,
		deliveryAlias,
		deliveryAlias,
		deliveryAlias,
		deliveryAlias,
		deliveryAlias,
		deliveryAlias,
		receiptAlias,
		receiptAlias,
		receiptAlias,
		receiptAlias,
	)
}
