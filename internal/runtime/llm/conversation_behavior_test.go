package llm

import (
	"context"

	"github.com/division-sh/swarm/internal/runtime/agentmemory"
)

// testConversationRuntime keeps fork-chat behavior tests independent
// from the production managed-call proof fixtures.
type testConversationRuntime interface {
	Runtime
	continueForTest(context.Context, *Session, Message) (*Response, error)
}

type testConversationRuntimeAdapter struct {
	Runtime
	continueTest func(context.Context, *Session, Message) (*Response, error)
}

func (a testConversationRuntimeAdapter) ContinueForkChatSession(ctx context.Context, session *Session, call ForkChatCall) (*Response, error) {
	return a.continueTest(ctx, session, call.providerMessage())
}

type testConversationRelayRuntimeAdapter struct {
	testConversationRuntimeAdapter
	relay oversizedToolResultRelayWriter
}

func (a testConversationRelayRuntimeAdapter) PersistOversizedToolResultRelay(ctx context.Context, session *Session, name string, rawJSON []byte) (toolResultRelayRef, error) {
	return a.relay.PersistOversizedToolResultRelay(ctx, session, name, rawJSON)
}

func newTestConversation(agentID, taskID, systemPrompt string, tools []ToolDefinition, memory agentmemory.Plan, maxTurns int, runtime Runtime) *Conversation {
	var continueTest func(context.Context, *Session, Message) (*Response, error)
	if typed, ok := runtime.(testConversationRuntime); ok {
		continueTest = typed.continueForTest
	} else {
		switch typed := runtime.(type) {
		case *AnthropicAPIRuntime:
			continueTest = func(ctx context.Context, session *Session, message Message) (*Response, error) {
				return typed.continueSession(ctx, session, message, nil)
			}
		case *OpenAICompatibleRuntime:
			continueTest = func(ctx context.Context, session *Session, message Message) (*Response, error) {
				return typed.continueSession(ctx, session, message, nil)
			}
		case *OpenAIResponsesRuntime:
			continueTest = func(ctx context.Context, session *Session, message Message) (*Response, error) {
				return typed.continueSession(ctx, session, message, nil)
			}
		case *ClaudeCLIRuntime:
			continueTest = func(ctx context.Context, session *Session, message Message) (*Response, error) {
				return typed.continueSession(ctx, session, message, nil)
			}
		case *MockRuntime:
			continueTest = func(ctx context.Context, session *Session, message Message) (*Response, error) {
				return typed.continueSession(ctx, session, message, nil)
			}
		default:
			panic("test runtime has no behavior continuation implementation")
		}
	}
	base := testConversationRuntimeAdapter{Runtime: runtime, continueTest: continueTest}
	var adapted Runtime = base
	if relay, ok := runtime.(oversizedToolResultRelayWriter); ok {
		adapted = testConversationRelayRuntimeAdapter{testConversationRuntimeAdapter: base, relay: relay}
	}
	conversation := newConversation(agentID, taskID, systemPrompt, tools, memory, maxTurns, adapted)
	conversation.kind = conversationForkChat
	return conversation
}

func (c *Conversation) runTestTurn(ctx context.Context, input string) (*Response, error) {
	return c.stepForkChat(ctx, Message{Role: "user", Content: input})
}
