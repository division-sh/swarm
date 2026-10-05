package channeldelivery

// SendEligibilityPredicate uses the plan alias p. Destination/render authority
// is checked by its consumer; source loss cannot authorize an initial send.
const SendEligibilityPredicate = `(p.source_kind IN ('summary','response')
	OR EXISTS (SELECT 1 FROM channel_delivery_receipts predecessor
		WHERE predecessor.delivery_id=p.delivery_id
		AND predecessor.effect_operation_id=p.current_receipt_operation_id
		AND predecessor.state='sent')
	OR (p.current_receipt_operation_id IS NULL AND (
		(p.source_kind='notice' AND EXISTS (SELECT 1 FROM mailbox notice
			WHERE notice.item_id=p.source_id AND notice.status='pending'))
		OR (p.source_kind='card' AND EXISTS (SELECT 1 FROM decision_cards card
			WHERE card.card_id=p.source_id AND card.status='pending')))))`

// AcceptedEffectPredicate is responsibility to recover/settle the original
// operation, never permission to render or send again. It survives source and
// destination loss; the journal alone owns its disposition.
func AcceptedEffectPredicate(postgres bool) string {
	identity := `json_extract(accepted.authority_evidence,'$.delivery_id')=p.delivery_id`
	if postgres {
		identity = `accepted.authority_evidence->>'delivery_id'=p.delivery_id::text`
	}
	return `EXISTS (SELECT 1 FROM runtime_external_effect_operations accepted
		WHERE accepted.authority_kind='channel_delivery' AND ` + identity + `
		AND accepted.state IN ('authorized','launched','response_observed'))`
}
