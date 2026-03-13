package identity

const (
	ActionRecordStateChange ActionKey = "record_state_change"
	ActionUpdateState       ActionKey = "update_stage"
	ActionCancelStateTimers ActionKey = "cancel_stage_timers"
	ActionStartStateTimers  ActionKey = "start_stage_timers"
)

var ImplicitStateTransitionActions = []ActionKey{
	ActionRecordStateChange,
	ActionUpdateState,
	ActionCancelStateTimers,
	ActionStartStateTimers,
}
