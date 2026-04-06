package store

import "fmt"

func CanonicalPendingAgentDeliveryPredicateSQL(receiptAlias string) string {
	return fmt.Sprintf(`(
		%s.event_id IS NULL
		OR (
			COALESCE(%s.side_effects->>'manager_status', '') = 'error'
			AND COALESCE((%s.side_effects->>'retry_count')::int, 0) <= 1
			AND (
				(COALESCE((%s.side_effects->>'retry_count')::int, 0) = 1 AND %s.processed_at <= now() - interval '1 minute')
			)
		)
	)`, receiptAlias, receiptAlias, receiptAlias, receiptAlias, receiptAlias)
}
