package runforkexecution

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiidempotency"
	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/mailbox"
	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/runtime/core/activityidentity"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/llm"
	"github.com/division-sh/swarm/internal/runtime/llm/selection"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkadmission"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/runtime/tools"
	"github.com/division-sh/swarm/internal/runtime/workspace"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

type freshSelectedNoticeStore interface {
	startupownership.Store
	tools.MailboxPersistence
	ListV1MailboxItems(context.Context, mailbox.V1ListOptions) ([]mailbox.V1Item, string, error)
	GetV1MailboxItem(context.Context, string) (mailbox.V1ItemDetail, error)
	EnsureOperatorPrincipal(context.Context, time.Time) (operatorchannel.Principal, error)
	AcknowledgeMailboxNotice(context.Context, apiidempotency.Request) (apiidempotency.Completion, bool, error)
}

func TestIssue642FreshSelectedNoticeReadbackAcknowledgmentBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			var selected freshSelectedNoticeStore
			var owner SelectedContractExecutionOwner
			if backend == "sqlite" {
				s := storetest.StartSQLiteRuntimeStore(t)
				selected, owner = s, selectedContractSQLiteExecutionOwnerForTest(t, s)
			} else {
				s := storetest.StartPostgresRuntimeStore(t)
				selected, owner = s, selectedContractExecutionOwnerForTest(t, s)
			}
			ctx, repo := runForkTestContext(t), runForkExecutionRepoRoot(t)
			root := nativeSelectedForkCrashFixture(t, repo)
			mock := `def handle(input):
    results = input["tool_results"] or []
    if not results:
        return {"calls": [{"name": "notify_human", "arguments": {"summary": "own selected notice", "context": {"proof": "fresh-child-only"}}}], "usage": {"input_tokens": 1, "output_tokens": 1}}
    return {"calls": [{"name": "emit_task_completed", "arguments": {"fork_result": "selected-native-once"}}], "usage": {"input_tokens": 1, "output_tokens": 1}}
`
			if err := os.WriteFile(filepath.Join(root, "worker/mocks/worker.py"), []byte(mock), 0o600); err != nil {
				t.Fatal(err)
			}
			loader := nativeSelectedCrashLoader{admittedFixtureSelectedContractSourceLoader{
				RepoRoot: repo, SourceRoot: root, PlatformSpecPath: filepath.Join(repo, "platform-spec.yaml"),
			}}
			loaded, err := loader.LoadRunForkSelectedContractSourceForRequest(ctx, SelectedContractSourceLoadRequest{
				Selection: runfork.RunForkContractSelection{Mode: "selected_contracts"},
			})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if loaded.Cleanup != nil {
					if err := loaded.Cleanup(); err != nil {
						t.Error(err)
					}
				}
			})
			source, initial := uuid.NewString(), uuid.NewString()
			seedSelectedAgentExecutionSource(t, ctx, selected, loaded, source, initial, time.Unix(1700002303, 0).UTC(), executionmode.Mock)
			before, err := storetest.ReadSelectedForkSourceDomain(ctx, selected, source)
			if err != nil {
				t.Fatal(err)
			}
			capability := selectedContractTestProcessCapability(t, ctx, selected)
			manager := workspace.NewHostManager()
			workspaceConfig := workspace.DefaultHostConfig()
			workspaceConfig.WorkspaceRoot = t.TempDir()
			manager.SetConfig(workspaceConfig)
			process, found := worklifetime.ProcessFromContext(ctx)
			if !found {
				t.Fatal("fresh notice requires its exact process owner")
			}
			baseline := process.ActiveCount()
			result, err := ExecuteSelectedContractRunFork(ctx, SelectedContractExecutionRequest{
				SourceRunID: source, At: initial, AllowSourceFreeze: true, Owner: owner, SourceLoader: loader,
				ContractSelection: runforkadmission.SelectedContractSelection(loaded.Source),
				AgentRuntime: SelectedContractAgentRuntimeOptions{
					ExecutionPosture: executionposture.MockOnly, ProcessCapability: capability, Workspace: manager, MailboxStore: selected,
					Config: &config.Config{LLM: config.LLMConfig{Backend: selection.BackendClaudeCLI,
						Session: config.LLMSessionConfig{LockTTL: time.Second, RotateAfterTurns: 40, RotateOnParseFailures: 3}}},
				},
			})
			if err != nil || !result.Activation.Activated {
				t.Fatalf("fresh notice activation: %+v error=%v", result, err)
			}
			wait, cancel := context.WithTimeout(ctx, 20*time.Second)
			defer cancel()
			child := result.Materialization.ForkRunID
			var items []mailbox.V1Item
			for {
				items, _, err = selected.ListV1MailboxItems(wait, mailbox.V1ListOptions{RunID: child, Limit: 10})
				delivery, readErr := storetest.ReadManagedDeliveryStorage(wait, selected, child, "test-agent")
				if err != nil || readErr != nil || len(delivery.Failures) != 0 {
					t.Fatalf("fresh notice execution: items=%+v delivery=%+v errors=%v/%v", items, delivery, err, readErr)
				}
				if len(items) == 1 && delivery.Delivered == 1 {
					break
				}
				select {
				case <-wait.Done():
					t.Fatalf("fresh notice did not settle: %+v error=%v", items, wait.Err())
				case <-time.After(10 * time.Millisecond):
				}
			}
			item := items[0]
			raw, rawErr := selected.GetMailboxItem(wait, item.MailboxID)
			if item.Type != tools.NotifyHumanMailboxItemType || item.SourceEventID != activityidentity.ForkLineageEventID(child, initial) ||
				item.SourceFlow != "worker" || rawErr != nil || raw.FromAgent == "" || raw.Summary != "own selected notice" || item.Status != "pending" ||
				!reflect.DeepEqual(item.Payload, map[string]any{"proof": "fresh-child-only"}) {
				t.Fatalf("fresh notice borrowed source ownership/payload: item=%+v raw=%+v error=%v", item, raw, rawErr)
			}
			detail, err := selected.GetV1MailboxItem(wait, item.MailboxID)
			if err != nil || !reflect.DeepEqual(detail.Item, item) || !reflect.DeepEqual(detail.Payload, item.Payload) {
				t.Fatalf("fresh notice get/list mismatch: %+v error=%v", detail, err)
			}
			turns, err := storetest.ReadManagedAgentTurnStorage(wait, selected, child, "test-agent")
			if err != nil || len(turns) != 2 {
				t.Fatalf("fresh notice lacks real provider turns: %+v error=%v", turns, err)
			}
			var calls []llm.ToolCall
			if err := json.Unmarshal(turns[0].ToolCalls, &calls); err != nil || len(calls) != 1 || calls[0].Name != "notify_human" {
				t.Fatalf("fresh notice did not execute through gateway: %+v error=%v", calls, err)
			}
			cleanup, finish := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
			defer finish()
			if err := owner.RetireSelectedContexts(cleanup); err != nil || process.ActiveCount() != baseline {
				t.Fatalf("notice retirement failed: work=%d baseline=%d error=%v", process.ActiveCount(), baseline, err)
			}
			childBefore, err := storetest.ReadSelectedForkSourceDomain(cleanup, selected, child)
			if err != nil {
				t.Fatal(err)
			}
			executionBefore, err := storetest.ReadSelectedExecutionStorage(cleanup, selected, child)
			if err != nil {
				t.Fatal(err)
			}
			repliesBefore, err := storetest.ReadReplyContextRecords(cleanup, selected, child)
			if err != nil {
				t.Fatal(err)
			}
			principal, err := selected.EnsureOperatorPrincipal(cleanup, time.Now().UTC())
			if err != nil {
				t.Fatal(err)
			}
			request := apiidempotency.Request{Method: "mailbox.acknowledge", Actor: apiidempotency.PrincipalActor(principal.ID),
				IdempotencyKey: "fresh-selected-notice-ack", ResourceID: item.MailboxID, RequestHash: "fresh-selected-notice-ack", Now: time.Now().UTC()}
			ackCtx := correlation.WithRunID(cleanup, child)
			completion, replayed, err := selected.AcknowledgeMailboxNotice(ackCtx, request)
			if err != nil || replayed {
				t.Fatalf("fresh notice acknowledgment failed: %+v replay=%t error=%v", completion, replayed, err)
			}
			again, replayed, err := selected.AcknowledgeMailboxNotice(ackCtx, request)
			if err != nil || !replayed || !reflect.DeepEqual(completion, again) {
				t.Fatalf("fresh notice acknowledgment did not replay: %+v replay=%t error=%v", again, replayed, err)
			}
			acknowledged, err := selected.GetMailboxItem(ackCtx, item.MailboxID)
			if err != nil || !acknowledged.Notified {
				t.Fatalf("fresh notice acknowledgment not durable: %+v error=%v", acknowledged, err)
			}
			still, err := selected.GetV1MailboxItem(ackCtx, item.MailboxID)
			if err != nil || !reflect.DeepEqual(detail, still) {
				t.Fatalf("ack changed notice identity/payload: %+v error=%v", still, err)
			}
			childAfter, domainErr := storetest.ReadSelectedForkSourceDomain(cleanup, selected, child)
			executionAfter, executionErr := storetest.ReadSelectedExecutionStorage(cleanup, selected, child)
			repliesAfter, replyErr := storetest.ReadReplyContextRecords(cleanup, selected, child)
			sourceAfter, sourceErr := storetest.ReadSelectedForkSourceDomain(cleanup, selected, source)
			sourceNotices, _, noticeErr := selected.ListV1MailboxItems(cleanup, mailbox.V1ListOptions{RunID: source, Limit: 10})
			if domainErr != nil || executionErr != nil || replyErr != nil || sourceErr != nil || noticeErr != nil ||
				!reflect.DeepEqual(childBefore, childAfter) || !reflect.DeepEqual(executionBefore, executionAfter) || !reflect.DeepEqual(repliesBefore, repliesAfter) ||
				!reflect.DeepEqual(before, sourceAfter) || len(sourceNotices) != 0 || process.ActiveCount() != baseline {
				t.Fatalf("notice ACK changed execution/replies/source: errors=%v/%v/%v/%v/%v", domainErr, executionErr, replyErr, sourceErr, noticeErr)
			}
		})
	}
}
