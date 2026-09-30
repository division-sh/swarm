package contracts

var emitFieldOptions = map[string]struct{}{
	"event":     {},
	"from":      {},
	"fields":    {},
	"target":    {},
	"broadcast": {},
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

func sameStringSet(a, b []string) bool {
	a = normalizeStrings(a)
	b = normalizeStrings(b)
	if len(a) != len(b) {
		return false
	}
	seen := map[string]int{}
	for _, value := range a {
		seen[value]++
	}
	for _, value := range b {
		if seen[value] == 0 {
			return false
		}
		seen[value]--
	}
	return true
}

var handlerFieldOptions = map[string]struct{}{
	"activity":          {},
	"description":       {},
	"_note":             {},
	"create_entity":     {},
	"emit":              {},
	"on_success":        {},
	"guard":             {},
	"advances_to":       {},
	"sets_gate":         {},
	"clear_gates":       {},
	"data_accumulation": {},
	"condition":         {},
	"logic":             {},
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
	"from":              {},
	"dedup_by":          {},
}

type handlerRuleDecodeContext string

const (
	handlerRuleDecodeContextRules          handlerRuleDecodeContext = "rules"
	handlerRuleDecodeContextOnComplete     handlerRuleDecodeContext = "on_complete"
	handlerRuleDecodeContextJoinOnComplete handlerRuleDecodeContext = "join.on_complete"
	handlerRuleDecodeContextJoinTimeout    handlerRuleDecodeContext = "join.timeout"
)
