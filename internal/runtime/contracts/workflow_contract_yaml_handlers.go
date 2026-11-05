package contracts

var emitFieldOptions = map[string]struct{}{
	"event":  {},
	"from":   {},
	"fields": {},
}

var onSuccessFieldOptions = map[string]struct{}{
	"emit": {},
}

var activityFieldOptions = map[string]struct{}{
	"id":       {},
	"tool":     {},
	"input":    {},
	"approval": {},
}

var activityApprovalFieldOptions = map[string]struct{}{
	"decision": {},
}

var handlerFieldOptions = map[string]struct{}{
	"activity":          {},
	"description":       {},
	"_note":             {},
	"emit":              {},
	"on_success":        {},
	"guard":             {},
	"advances_to":       {},
	"terminate":         {},
	"data_accumulation": {},
	"loop":              {},
	"on_complete":       {},
	"rules":             {},
	"accumulate":        {},
	"join":              {},
	"compute":           {},
	"query":             {},
	"fan_out":           {},
	"group_by":          {},
	"filter":            {},
	"reduce":            {},
	"count":             {},
	"clear":             {},
}

type handlerRuleDecodeContext string

const (
	handlerRuleDecodeContextRules          handlerRuleDecodeContext = "rules"
	handlerRuleDecodeContextOnComplete     handlerRuleDecodeContext = "on_complete"
	handlerRuleDecodeContextJoinOnComplete handlerRuleDecodeContext = "join.on_complete"
)
