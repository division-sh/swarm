package llm

import (
	"encoding/json"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
)

type contextCompactionBudget struct {
	Trigger int
	Keep    int
}

const contextCompactionInstruction = `Context compaction: summarize the earlier conversation as a durable checkpoint.
Use Markdown sections: Intent, Essential facts, Completed work, Errors, Pending work, Next action.
Preserve exact identifiers, decisions and unfinished obligations. Merge still-valid facts from earlier checkpoints.
Return only the checkpoint text. Do not call tools, emit an event, claim success or invent a result.`

const contextCompactionPreamble = "The following checkpoint summarizes earlier conversation. Use it as background and continue from the retained messages.\n\n"

type contextCompactionReplay struct {
	Messages []Message
	Native   json.RawMessage
	Method   string
}

func contextCompactionRoute(backend, destination string) string {
	fingerprint, _ := canonicaljson.Hash([]string{backend, destination})
	return fingerprint
}

func replayContextCompaction(session *Session, route, model string) (contextCompactionReplay, error) {
	if err := validateContextCompactionCheckpoints(session.ID, session.Messages, session.ContextCheckpoints); err != nil {
		return contextCompactionReplay{}, err
	}
	if len(session.ContextCheckpoints) == 0 {
		return contextCompactionReplay{Messages: session.Messages}, nil
	}
	checkpoint := session.ContextCheckpoints[len(session.ContextCheckpoints)-1]
	replay := contextCompactionReplay{Messages: session.Messages[checkpoint.FirstKeptMessage:], Method: checkpoint.Method}
	if checkpoint.Method == "platform_summary" {
		replay.Messages = append([]Message{{Role: "user", Content: contextCompactionPreamble + checkpoint.Summary}}, replay.Messages...)
		return replay, nil
	}
	if checkpoint.Route != route || checkpoint.Model != model {
		return contextCompactionReplay{}, contextCompactionLimitation("native_checkpoint_requires_new_route_compaction")
	}
	var err error
	replay.Native, err = canonicaljson.Canonicalize(checkpoint.Native)
	if err != nil {
		return contextCompactionReplay{}, contextCompactionLimitation("invalid_native_replay")
	}
	return replay, nil
}

func resolveContextCompactionBudget(window, output int) (contextCompactionBudget, error) {
	const headroom = 65536
	if window <= 0 || output <= 0 || output >= window || window-output <= headroom {
		return contextCompactionBudget{}, contextCompactionLimitation("unknown_or_invalid_budget")
	}
	trigger := min(window/5*4+window%5*4/5, window-output-headroom)
	available := window - output
	keep := available/25*4 + available%25*4/25
	if trigger <= 0 || keep <= 0 || keep >= trigger {
		return contextCompactionBudget{}, contextCompactionLimitation("invalid_trigger_or_retention")
	}
	return contextCompactionBudget{Trigger: trigger, Keep: keep}, nil
}

func contextCompactionLimitation(reason string) error {
	return runtimefailures.New(runtimefailures.ClassDependencyUnavailable, "context_compaction_unavailable", "llm-context", "compact", map[string]any{"reason": reason})
}

type ContextCompactionCheckpoint struct {
	Version              int             `json:"version"`
	ID                   string          `json:"id"`
	ParentID             string          `json:"parent_id,omitempty"`
	SessionID            string          `json:"session_id"`
	Method               string          `json:"method"`
	Route                string          `json:"route"`
	Model                string          `json:"model"`
	CoveredMessages      int             `json:"covered_messages"`
	FirstKeptMessage     int             `json:"first_kept_message"`
	CanonicalFingerprint string          `json:"canonical_fingerprint"`
	SystemFingerprint    string          `json:"system_fingerprint"`
	ToolsFingerprint     string          `json:"tools_fingerprint"`
	OperationID          string          `json:"operation_id"`
	AttemptID            string          `json:"attempt_id"`
	ResultFingerprint    string          `json:"result_fingerprint"`
	Summary              string          `json:"summary,omitempty"`
	Native               json.RawMessage `json:"native,omitempty"`
}

func (c ContextCompactionCheckpoint) Fingerprint() (string, error) {
	c.ID = ""
	return canonicaljson.Hash(c)
}

func cloneContextCompactionCheckpoints(checkpoints []ContextCompactionCheckpoint) []ContextCompactionCheckpoint {
	copy := append([]ContextCompactionCheckpoint(nil), checkpoints...)
	for i := range copy {
		copy[i].Native = append(json.RawMessage(nil), copy[i].Native...)
	}
	return copy
}

func validateContextCompactionCheckpoints(sessionID string, messages []Message, checkpoints []ContextCompactionCheckpoint) error {
	parent := ""
	covered, firstKept := 0, 0
	for _, checkpoint := range checkpoints {
		if checkpoint.Version != 1 || checkpoint.SessionID != sessionID || checkpoint.ParentID != parent ||
			checkpoint.CoveredMessages < covered || checkpoint.CoveredMessages > len(messages) ||
			checkpoint.FirstKeptMessage <= firstKept || checkpoint.FirstKeptMessage > checkpoint.CoveredMessages ||
			checkpoint.Route == "" || checkpoint.Model == "" || checkpoint.OperationID == "" || checkpoint.AttemptID == "" ||
			checkpoint.CanonicalFingerprint == "" || checkpoint.SystemFingerprint == "" || checkpoint.ToolsFingerprint == "" || checkpoint.ResultFingerprint == "" {
			return contextCompactionLimitation("checkpoint_identity_or_parent_mismatch")
		}
		switch checkpoint.Method {
		case "platform_summary":
			if strings.TrimSpace(checkpoint.Summary) == "" || len(checkpoint.Native) != 0 {
				return contextCompactionLimitation("invalid_summary_checkpoint")
			}
		case "anthropic_native", "openai_native":
			if !json.Valid(checkpoint.Native) || len(checkpoint.Native) == 0 || checkpoint.Summary != "" {
				return contextCompactionLimitation("invalid_native_checkpoint")
			}
		default:
			return contextCompactionLimitation("unknown_checkpoint_method")
		}
		fingerprint, err := checkpoint.Fingerprint()
		if err != nil || fingerprint != checkpoint.ID {
			return contextCompactionLimitation("checkpoint_fingerprint_mismatch")
		}
		parent, covered, firstKept = checkpoint.ID, checkpoint.CoveredMessages, checkpoint.FirstKeptMessage
	}
	if len(checkpoints) != 0 {
		last := checkpoints[len(checkpoints)-1]
		fingerprint, err := canonicaljson.Hash(messages[:last.CoveredMessages])
		if err != nil || fingerprint != last.CanonicalFingerprint {
			return contextCompactionLimitation("checkpoint_canonical_prefix_mismatch")
		}
	}
	return nil
}

func contextCompactionMessageTokens(message Message) int {
	content := message.Content + message.RuntimeContext
	for _, call := range message.ToolCalls {
		arguments, _ := json.Marshal(call.Arguments)
		content += call.Name + string(arguments)
	}
	// Match the reference's UTF-16 character-density estimate, not billing usage.
	characters := 0
	for _, r := range content {
		characters++
		if r > 0xffff {
			characters++
		}
	}
	return (characters+3)/4 + 8
}

func contextCompactionCut(messages []Message, firstEligible, keep int) (int, error) {
	if firstEligible < 0 || firstEligible > len(messages) || keep < 0 {
		return 0, contextCompactionLimitation("invalid_cut_budget")
	}
	balanced := make([]bool, len(messages)+1)
	balanced[0] = true
	pending := map[string]bool{}
	for i, message := range messages {
		for _, call := range message.ToolCalls {
			if call.ID == "" || pending[call.ID] {
				return 0, contextCompactionLimitation("invalid_tool_call_identity")
			}
			pending[call.ID] = true
		}
		if message.Role == "tool" {
			results := providerToolResults(message.Content)
			if len(results) == 0 {
				return 0, contextCompactionLimitation("tool_result_identity_missing")
			}
			for _, result := range results {
				if !pending[result.CallID] {
					return 0, contextCompactionLimitation("tool_result_has_no_pending_call")
				}
				delete(pending, result.CallID)
			}
		}
		balanced[i+1] = len(pending) == 0
	}
	if len(pending) != 0 {
		return 0, contextCompactionLimitation("unanswered_tool_call")
	}
	cut, tokens := len(messages), 0
	for cut > firstEligible {
		next := contextCompactionMessageTokens(messages[cut-1])
		if next > keep-tokens {
			break
		}
		cut--
		tokens += next
	}
	for cut > firstEligible && !balanced[cut] {
		cut--
	}
	if cut <= firstEligible || !balanced[cut] {
		return 0, contextCompactionLimitation("no_safe_older_span")
	}
	return cut, nil
}
