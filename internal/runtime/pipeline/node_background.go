package pipeline

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimelifecycleprobe "github.com/division-sh/swarm/internal/runtime/lifecycleprobe"
)

type backgroundWorkflowNode struct {
	executor WorkflowNodeExecutor
	runner   *systemNodeRunner
}

func newBackgroundWorkflowNode(executor WorkflowNodeExecutor, bus systemNodeBus, db *sql.DB) *backgroundWorkflowNode {
	return newBackgroundWorkflowNodeWithRetryBase(executor, bus, db, nil, 0)
}

func newBackgroundWorkflowNodeWithRetryBase(executor WorkflowNodeExecutor, bus systemNodeBus, db *sql.DB, eventReceiptsCapability func(context.Context) (bool, error), retryBase time.Duration) *backgroundWorkflowNode {
	return newBackgroundWorkflowNodeWithReceiptStoreAndRetryBase(executor, bus, db, NewWorkflowInstanceStore(db), eventReceiptsCapability, retryBase)
}

func newBackgroundWorkflowNodeWithReceiptStoreAndRetryBase(executor WorkflowNodeExecutor, bus systemNodeBus, db *sql.DB, receiptStore SystemNodeReceiptPersistence, eventReceiptsCapability func(context.Context) (bool, error), retryBase time.Duration) *backgroundWorkflowNode {
	if executor == nil || bus == nil {
		return nil
	}
	node := &backgroundWorkflowNode{executor: executor}
	node.runner = newSystemNodeRunnerWithReceiptStoreAndRetryBase(executor.NodeID(), bus, db, receiptStore, executor.Subscriptions, func(ctx context.Context, evt events.Event) error {
		if handled := executor.Handle(ctx, evt); handled {
			return nil
		}
		return fmt.Errorf("workflow executor %s did not handle subscribed event %s", executor.NodeID(), evt.Type())
	}, retryBase, eventReceiptsCapability)
	return node
}

func (n *backgroundWorkflowNode) Run(ctx context.Context) {
	if n == nil || n.runner == nil {
		return
	}
	n.runner.Run(ctx)
}

func (n *backgroundWorkflowNode) ProcessEventForTest(ctx context.Context, evt events.Event) {
	if n == nil || n.runner == nil {
		return
	}
	n.runner.ProcessEventForTest(ctx, evt)
}

func (n *backgroundWorkflowNode) SetRetryPolicyForTest(limit int, backoff func(int) time.Duration) {
	if n == nil || n.runner == nil {
		return
	}
	n.runner.SetRetryPolicyForTest(limit, backoff)
}

func (n *backgroundWorkflowNode) SetOverrideHandleForTest(fn func(context.Context, events.Event) error) {
	if n == nil || n.runner == nil {
		return
	}
	n.runner.SetOverrideHandleForTest(fn)
}

func (n *backgroundWorkflowNode) SetOnSubscribeForTest(fn func()) {
	if n == nil || n.runner == nil {
		return
	}
	n.runner.SetOnSubscribeForTest(fn)
}

func (n *backgroundWorkflowNode) SetTestLifecycleProbe(probe runtimelifecycleprobe.Observer) {
	if n == nil || n.runner == nil {
		return
	}
	n.runner.SetTestLifecycleProbe(probe)
}

func (n *backgroundWorkflowNode) String() string {
	if n == nil || n.runner == nil {
		return ""
	}
	return n.runner.String()
}
