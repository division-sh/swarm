package llm

import (
	"github.com/division-sh/swarm/internal/runtime/agentintent"
	"github.com/division-sh/swarm/internal/runtime/core/managedcapabilities"
)

func prepareEnvironmentFrame(session *Session, surface managedcapabilities.Surface) error {
	// Restored conversations already own their first frame. Never reproject or
	// prepend environment facts to retained history when the grant changes.
	if len(session.Messages) != 0 || session.TurnCount != 0 {
		return nil
	}
	for _, tool := range surface.Tools {
		if !tool.EffectiveCallable || !tool.EffectiveVisible || tool.Capability.AuthorizationClass != "native_tool" {
			continue
		}
		switch tool.Capability.Name {
		case "bash", "read_file", "write_file":
			text, err := agentintent.RuntimeEnvironmentContext().Text()
			if err != nil {
				return err
			}
			message := Message{Role: "user", Content: text, RuntimeContext: "environment"}
			session.Messages = []Message{message}
			return nil
		}
	}
	return nil
}
