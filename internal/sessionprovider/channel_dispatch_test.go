//go:build linux || darwin

package sessionprovider

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/sessionprovider/execution"
)

func TestWhatsAppOwnedChannelDispatchRefusesReconstructedAndChangedSelectionBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newNativeConfirmationFixture(t, backend)
			ctx := f.confirmationContext(t)
			executor := sessionChannelExecutor{owner: f.authority, plan: f.channel}.channelExecution()
			encoded, err := json.Marshal(executor)
			if err != nil {
				t.Fatal(err)
			}
			var reconstructed execution.Channel
			if err := json.Unmarshal(encoded, &reconstructed); err != nil {
				t.Fatal(err)
			}
			toolID, tool, err := f.channel.ConnectorOperation("deliver")
			if err != nil {
				t.Fatal(err)
			}
			changedInput, err := tool.InputSchema().WithRequiredProperty("extra", contracts.MustToolInputSchema(contracts.ToolSchemaString))
			if err != nil {
				t.Fatal(err)
			}
			changed, err := tool.WithSchemas(changedInput, tool.OutputSchema())
			if err != nil {
				t.Fatal(err)
			}
			canceled, cancel := context.WithCancel(ctx)
			cancel()
			input := map[string]any{"destination": f.binding.ConversationRef, "text": "never sent"}
			for _, row := range []struct {
				name        string
				ctx         context.Context
				executor    execution.Channel
				operationID string
				operation   string
				toolID      string
				tool        contracts.ToolSchemaEntry
			}{
				{"reconstructed", ctx, reconstructed, f.operation.OperationID, "deliver", toolID, tool},
				{"foreign_connection", ctx, executor, "another-connection", "deliver", toolID, tool},
				{"foreign_operation", ctx, executor, f.operation.OperationID, "not-deliver", toolID, tool},
				{"foreign_tool", ctx, executor, f.operation.OperationID, "deliver", "other.send", tool},
				{"changed_contract", ctx, executor, f.operation.OperationID, "deliver", toolID, changed},
				{"canceled", canceled, executor, f.operation.OperationID, "deliver", toolID, tool},
			} {
				t.Run(row.name, func(t *testing.T) {
					if _, err := row.executor.DeliverChannelConfirmation(row.ctx, row.operationID, row.operation, row.toolID, row.tool, input, nil); err == nil {
						t.Fatal("unowned dispatch succeeded")
					}
				})
			}
			if _, found, err := f.selected.(runtimeeffects.OutcomeStore).GetExternalEffectOutcome(ctx, f.operation.ConfirmationOperationID); err != nil || found {
				t.Fatalf("refused dispatch mutated journal: found=%t %v", found, err)
			}
			select {
			case frame := <-f.peer.frames:
				t.Fatalf("refused dispatch reached SDK: %+v", frame.node)
			default:
			}
		})
	}
}
