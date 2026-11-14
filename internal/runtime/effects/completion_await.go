package effects

import "encoding/json"

// CompletionAwaitPublication is private native composition, not authored syntax
// or permission to end, recover, or resume an invocation.
type CompletionAwaitPublication struct {
	Attempt          Attempt
	CallIndex        int
	CompletedResults json.RawMessage
}
