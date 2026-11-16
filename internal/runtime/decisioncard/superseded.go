package decisioncard

const SupersededByUpdate = "offer_changed_after_update"

func SupersededMessage(reason string) string {
	if reason == SupersededByUpdate {
		return "Replaced by a new card after an update. No action was taken."
	}
	return "The flow moved on. No action was taken."
}
