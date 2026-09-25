package conformance

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/durabledata"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/notifyallchildren"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

const (
	semanticProofPayloadEvent      = "portfolio.proof.payload.requested"
	semanticProofEntityEvent       = "portfolio.proof.entity.requested"
	semanticProofResourceEvent     = "portfolio.proof.resource.requested"
	semanticProofJobflowEvent      = "portfolio.proof.jobflow.requested"
	semanticProofKeylessEvent      = "portfolio.proof.keyless.requested"
	semanticProofRuleDataEvent     = "portfolio.proof.rule_data.requested"
	semanticProofCompleteDataEvent = "portfolio.proof.complete_data.requested"
	semanticProofOverwriteEvent    = "portfolio.proof.overwrite.requested"
)

func TestFanOutSemanticProofFixtureAdmitsExactProducerSites(t *testing.T) {
	source := semanticProofSource(t)
	node := identitytest.FlowNode(t, notifyallchildren.OwnerFlowID, "portfolio-coordinator")
	for _, test := range []struct {
		event, source string
		site          contracts.FanOutSiteKind
	}{
		{semanticProofPayloadEvent, "payload.account_ids", contracts.FanOutSiteRule},
		{semanticProofEntityEvent, "entity.account_ids", contracts.FanOutSiteOnComplete},
	} {
		plans := source.FanOutPlansForHandler(node, test.event)
		if len(plans) != 1 || plans[0].ItemsFrom != test.source || plans[0].Site.Kind != test.site {
			t.Fatalf("compiled producer %s: %+v", test.event, plans)
		}
		if test.site == contracts.FanOutSiteOnComplete && !plans[0].SourceAfterWrites {
			t.Fatal("entity proof must bind its same-handler writes")
		}
	}
}

func TestFanOutResourceCrossFlowFixtureCompiles(t *testing.T) {
	source := semanticProofSourceWithCrossFlow(t, true)
	node := identitytest.FlowNode(t, notifyallchildren.OwnerFlowID, "portfolio-coordinator")
	plans := source.FanOutPlansForHandler(node, semanticProofResourceEvent)
	if len(plans) != 1 || plans[0].ResourceSource == nil || plans[0].EmittedEventType() != "portfolio/account.registered" {
		t.Fatalf("cross-flow resource source plan = %+v", plans)
	}
}

// Extend a copy of the existing admitted numeric fixture, not its shared files.
func semanticProofSource(t *testing.T) semanticview.Source {
	return semanticProofSourceWithCrossFlow(t, false)
}

func semanticProofSourceWithCrossFlow(t *testing.T, crossFlow bool) semanticview.Source {
	t.Helper()
	root := notifyallchildren.WriteVariant(t, notifyallchildren.Options{NumericRegistrationRows: true, NumericInternalSettlement: !crossFlow, RegistrationUUIDField: true})
	modify := func(relative string, change func(string) string) {
		t.Helper()
		path := filepath.Join(root, notifyallchildren.OwnerFlowID, relative)
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(change(string(raw))), 0600); err != nil {
			t.Fatal(err)
		}
	}
	replace := func(raw, old, next string) string {
		t.Helper()
		if strings.Count(raw, old) != 1 {
			t.Fatalf("fixture replacement is not exact: %q", old)
		}
		return strings.Replace(raw, old, next, 1)
	}
	eventsToAdd := []string{semanticProofPayloadEvent, semanticProofEntityEvent, semanticProofResourceEvent, semanticProofJobflowEvent, semanticProofKeylessEvent, semanticProofRuleDataEvent, semanticProofCompleteDataEvent, semanticProofOverwriteEvent}
	modify("schema.yaml", func(raw string) string {
		var added strings.Builder
		for _, name := range eventsToAdd {
			fmt.Fprintf(&added, "      - event: %s\n        source: external\n", name)
		}
		raw = replace(raw, "  outputs:\n", added.String()+"  outputs:\n")
		return replace(raw, "      - account.registered\n", "      - account.registered\n      - company.lead\n      - company.keyless\n")
	})
	modify("events.yaml", func(raw string) string {
		raw = replace(raw, "  eligible: boolean\n", "  eligible: boolean\n  ordinal: integer\n  source_count: integer\n  snapshot_threshold: integer\n")
		for _, name := range eventsToAdd {
			raw += fmt.Sprintf("%s:\n  portfolio_id: text\n  account_ids: \"[NumericAccount]\"\n  threshold: integer\n", name)
		}
		raw += `company.lead:
  key: slug
  slug: text
  name: text
  domain: text
  funds: "[text]"
  sources: "[text]"
  tvl: number?
  category: text
  has_token: boolean
  note: text
  on_w3c: boolean
  ats: JobflowATS?
  eng_roles: integer
  fit_titles: "[text]"
  gem_score: number
company.keyless:
  value: text
`
		return raw
	})
	modify("types.yaml", func(raw string) string {
		return raw + `  JobflowATS:
    provider: text
    ats_slug: text
    titles: "[text]"
`
	})
	modify("nodes.yaml", func(raw string) string {
		var subscriptions strings.Builder
		for _, name := range eventsToAdd {
			fmt.Fprintf(&subscriptions, "    - %s\n", name)
		}
		raw = replace(raw, "    - portfolio.opened\n", "    - portfolio.opened\n"+subscriptions.String())
		// Existing producers of account.registered must satisfy the extended
		// output contract too, even though these proofs use the new handlers.
		raw = replace(raw, "            eligible: entity.threshold >= 70\n", "            eligible: entity.threshold >= 70\n            ordinal: fan_out.index\n            source_count: fan_out.count\n            snapshot_threshold: entity.threshold\n")
		for _, producer := range []struct{ event, site, source string }{
			{semanticProofPayloadEvent, "rules", "payload.account_ids"},
			{semanticProofEntityEvent, "on_complete", "entity.account_ids"},
		} {
			raw += fmt.Sprintf(`    %s:
      %s:
        - id: issue
          condition: else
          data_accumulation:
            writes:
              - source_field: account_ids
                target_field: account_ids
              - source_field: threshold
                target_field: threshold
          fan_out:
            items_from: %s
            as: account
            identity: account.account_id
            max_items: 100
            emit:
              event: account.registered
              fields:
                portfolio_id: payload.portfolio_id
                account_id: account.account_id
                eng_roles: account.eng_roles
                gem_score: account.gem_score
                external_id: account.external_id
                eligible: entity.threshold >= 70
                ordinal: fan_out.index
                source_count: fan_out.count
                snapshot_threshold: entity.threshold
`, producer.event, producer.site, producer.source)
		}
		raw += fmt.Sprintf(`    %s:
      fan_out:
        items_from: data.portfolio/account.registered
`, semanticProofResourceEvent)
		raw += fmt.Sprintf(`    %s:
      fan_out:
        items_from: data.portfolio/company.lead
`, semanticProofJobflowEvent)
		raw += fmt.Sprintf(`    %s:
      fan_out:
        items_from: data.portfolio/company.keyless
`, semanticProofKeylessEvent)
		raw += fmt.Sprintf(`    %s:
      rules:
        - id: unselected
          condition: payload.threshold < 0
          fan_out:
            items_from: data.portfolio/company.lead
        - id: selected
          condition: else
          fan_out:
            items_from: data.portfolio/company.keyless
`, semanticProofRuleDataEvent)
		raw += fmt.Sprintf(`    %s:
      on_complete:
        - id: complete
          fan_out:
            items_from: data.portfolio/company.keyless
`, semanticProofCompleteDataEvent)
		raw += fmt.Sprintf(`    %s:
      data_accumulation:
        writes:
          - source_field: account_ids
            target_field: account_ids
          - source_field: threshold
            target_field: threshold
`, semanticProofOverwriteEvent)
		return `jobflow-lead-observer:
  execution_type: system_node
  subscribes_to:
    - company.lead
  event_handlers:
    company.lead: {}
jobflow-keyless-observer:
  execution_type: system_node
  subscribes_to:
    - company.keyless
  event_handlers:
    company.keyless: {}
` + raw
	})
	bundle, err := contracts.LoadWorkflowContractBundleWithOverrides(canonicalrouting.RepoRoot(t), root, contracts.DefaultPlatformSpecFile(canonicalrouting.RepoRoot(t)))
	if err != nil {
		t.Fatal(err)
	}
	return semanticview.Wrap(bundle)
}

type semanticProofPolicy struct {
	failure   string
	maxCommit int
}

type semanticProofAttempt struct {
	start, count int
	injected     bool
}
type semanticProofReceipt struct {
	key     fanoutobligation.IntentKey
	trigger string
	result  pipeline.FanOutTurnResult
	err     error
}

type semanticProofProbe struct {
	*pipeline.PipelineCoordinator
	db               *sql.DB
	mu               sync.Mutex
	policies         map[string]semanticProofPolicy
	intents          map[string]fanoutobligation.Intent
	attempts         map[string][]semanticProofAttempt
	sqlFaults        map[string][]semanticProofAttempt
	outcomeSQLFaults map[string]int
	blocks           map[string]pipeline.FanOutBlockRequest
	retries          map[string]pipeline.FanOutRetryableRelease
	completed        chan semanticProofReceipt
}

type semanticProofTurn struct {
	pipeline.FanOutObligationOwner
	probe  *semanticProofProbe
	intent fanoutobligation.Intent
}

func (p *semanticProofProbe) ServeFanOutCandidate(ctx context.Context, owner pipeline.FanOutObligationOwner, key fanoutobligation.IntentKey) (pipeline.FanOutTurnResult, error) {
	turn := &semanticProofTurn{FanOutObligationOwner: owner, probe: p}
	result, err := p.PipelineCoordinator.ServeFanOutCandidate(ctx, turn, key)
	p.completed <- semanticProofReceipt{key: key, trigger: turn.intent.Request.Capsule.Lineage.ParentEventID, result: result, err: err}
	return result, err
}

func (turn *semanticProofTurn) ClaimFanOutIntent(ctx context.Context, req pipeline.FanOutClaimRequest) (fanoutobligation.Intent, fanoutobligation.Claim, bool, error) {
	intent, claim, found, err := turn.FanOutObligationOwner.ClaimFanOutIntent(ctx, req)
	if found && err == nil {
		turn.intent = intent
		p := turn.probe
		p.mu.Lock()
		p.intents[intent.Request.Capsule.Lineage.ParentEventID] = intent
		p.mu.Unlock()
	}
	return intent, claim, found, err
}

func (turn *semanticProofTurn) CommitFanOutChunk(ctx context.Context, command pipeline.FanOutChunkCommand) (pipeline.CommittedFanOutChunk, error) {
	trigger := turn.intent.Request.Capsule.Lineage.ParentEventID
	p := turn.probe
	p.mu.Lock()
	policy := p.policies[trigger]
	first := len(p.attempts[trigger]) == 0
	var failure error
	var sqlFault *semanticProofSQLFault
	switch {
	case first && policy.failure == "unknown":
		failure = errors.New("injected unknown publication commit failure")
	case first && policy.failure == "authorization":
		failure = runtimefailures.New(runtimefailures.ClassAuthorizationDenied, "proof_publish_denied", "test", "commit", map[string]any{"action": "publish"})
	case first && policy.failure == "retry":
		failure = runtimefailures.New(runtimefailures.ClassDependencyUnavailable, "proof_safe_retry", "test", "commit", nil)
	case policy.maxCommit > 0 && len(command.Outcomes) > policy.maxCommit:
		envelope := runtimefailures.Normalize(runtimefailures.New(runtimefailures.ClassSchemaInvalid, "proof_safe_aggregate", "test", "commit", nil), "test", "commit")
		failure = pipeline.NewFanOutSafeAggregateError(envelope, errors.New("injected safe aggregate rejection after native insert"))
		sqlFault = &semanticProofSQLFault{cause: failure, outcomeInsert: true}
		// All-rejected commands have no event INSERT. Choose from typed plans,
		// retaining the event-bearing fault path for every other command.
		for _, outcome := range command.Outcomes {
			if outcome.Publication != nil {
				sqlFault.outcomeInsert = false
				break
			}
		}
	}
	p.attempts[trigger] = append(p.attempts[trigger], semanticProofAttempt{start: command.Outcomes[0].Ordinal, count: len(command.Outcomes), injected: failure != nil})
	p.mu.Unlock()
	if sqlFault != nil {
		result, err := turn.FanOutObligationOwner.CommitFanOutChunk(context.WithValue(ctx, semanticProofSQLFaultKey{}, sqlFault), command)
		if !sqlFault.fired.Load() {
			return result, errors.Join(err, errors.New("safe aggregate fixture did not reach its actual native insert"))
		}
		if _, safe := pipeline.FanOutSafeAggregateFailure(err); safe {
			if rollbackErr := p.verifyNoAttemptRows(ctx, command); rollbackErr != nil {
				return result, rollbackErr
			}
			p.mu.Lock()
			p.sqlFaults[trigger] = append(p.sqlFaults[trigger], semanticProofAttempt{start: command.Outcomes[0].Ordinal, count: len(command.Outcomes), injected: true})
			if sqlFault.outcomeInsert {
				p.outcomeSQLFaults[trigger]++
			}
			p.mu.Unlock()
		}
		return result, err
	}
	if failure != nil {
		return pipeline.CommittedFanOutChunk{}, failure
	}
	return turn.FanOutObligationOwner.CommitFanOutChunk(ctx, command)
}

func (p *semanticProofProbe) verifyNoAttemptRows(ctx context.Context, command pipeline.FanOutChunkCommand) error {
	key := command.Claim.Key
	query := `SELECT (SELECT COUNT(*) FROM fan_out_outcomes WHERE run_id=$1 AND triggering_delivery_id=$2 AND flow_path=$3 AND declaration_family=$4 AND semantic_path=$5 AND ordinal >= $6 AND ordinal < $7)`
	args := []any{key.RunID, key.TriggeringDeliveryID, key.ElementRef.FlowPath, key.ElementRef.Family, key.ElementRef.SemanticPath, command.Outcomes[0].Ordinal, command.Outcomes[len(command.Outcomes)-1].Ordinal + 1}
	var eventParams []string
	for _, outcome := range command.Outcomes {
		if outcome.Publication != nil {
			args = append(args, outcome.Publication.DurablePublicationEventID())
			eventParams = append(eventParams, fmt.Sprintf("$%d", len(args)))
		}
	}
	if len(eventParams) != 0 {
		query += ` + (SELECT COUNT(*) FROM events WHERE run_id=$1 AND event_id IN (` + strings.Join(eventParams, ",") + `))`
	}
	var leaked int
	if err := p.db.QueryRowContext(ctx, query, args...).Scan(&leaked); err != nil {
		return fmt.Errorf("verify native safe-aggregate rollback: %w", err)
	}
	if leaked != 0 {
		return fmt.Errorf("native safe-aggregate rollback leaked %d attempted event/outcome rows", leaked)
	}
	return nil
}

func (turn *semanticProofTurn) BlockFanOutClaim(ctx context.Context, request pipeline.FanOutBlockRequest) (pipeline.FanOutClaimSettlement, error) {
	settlement, err := turn.FanOutObligationOwner.BlockFanOutClaim(ctx, request)
	if !settlement.Acknowledged {
		return settlement, err
	}
	turn.probe.mu.Lock()
	turn.probe.blocks[turn.intent.Request.Capsule.Lineage.ParentEventID] = request
	turn.probe.mu.Unlock()
	return settlement, err
}

func (turn *semanticProofTurn) ReleaseFanOutRetryable(ctx context.Context, request pipeline.FanOutRetryableRelease) (pipeline.FanOutClaimSettlement, error) {
	settlement, err := turn.FanOutObligationOwner.ReleaseFanOutRetryable(ctx, request)
	if !settlement.Acknowledged {
		return settlement, err
	}
	turn.probe.mu.Lock()
	turn.probe.retries[turn.intent.Request.Capsule.Lineage.ParentEventID] = request
	turn.probe.mu.Unlock()
	return settlement, err
}

type semanticProofFixture struct {
	selected notifyAllChildrenStore
	db       *sql.DB
	source   semanticview.Source
	runtime  notifyAllChildrenRuntime
	topology *notifyAllChildrenProcessTopology
	probe    *semanticProofProbe
	ctx      context.Context
	runID    string
}

func newSemanticProofFixture(t *testing.T, backend string, sqlFaults ...bool) *semanticProofFixture {
	return newSemanticProofFixtureWithPreparation(t, backend, nil, sqlFaults...)
}

func newSemanticProofFixtureWithPreparation(t *testing.T, backend string, prepare func(*semanticProofFixture) []durabledata.ExplicitPin, sqlFaults ...bool) *semanticProofFixture {
	return newSemanticProofFixtureWithSourcePreparation(t, backend, semanticProofSource(t), prepare, sqlFaults...)
}

func newSemanticProofFixtureWithSourcePreparation(t *testing.T, backend string, source semanticview.Source, prepare func(*semanticProofFixture) []durabledata.ExplicitPin, sqlFaults ...bool) *semanticProofFixture {
	t.Helper()
	f := &semanticProofFixture{source: source, runID: uuid.NewString()}
	if len(sqlFaults) != 0 && sqlFaults[0] {
		f.selected, f.db = newSemanticProofSQLFaultStore(t, backend)
	} else if backend == "postgres" {
		_, db, cleanup := testutil.StartPostgres(t)
		t.Cleanup(cleanup)
		f.selected, f.db = storetest.AdmitPostgresRuntimeStore(t, db), db
	} else {
		selected := storetest.StartSQLiteRuntimeStore(t)
		f.selected, f.db = selected, storetest.DatabaseForTest(selected)
	}
	f.probe = &semanticProofProbe{db: f.db, policies: map[string]semanticProofPolicy{}, intents: map[string]fanoutobligation.Intent{}, attempts: map[string][]semanticProofAttempt{}, sqlFaults: map[string][]semanticProofAttempt{}, outcomeSQLFaults: map[string]int{}, blocks: map[string]pipeline.FanOutBlockRequest{}, retries: map[string]pipeline.FanOutRetryableRelease{}, completed: make(chan semanticProofReceipt, 1024)}
	f.topology = newNotifyAllChildrenProcessTopology(t, testAuthorActivityContextForBundle(context.Background(), conformanceSourceArtifactFact(t, f.source)), f.selected, f.source)
	f.runtime = newNotifyAllChildrenRuntime(t, f.selected, f.db, f.source, time.Now, notifyAllChildrenRuntimeOptions{
		processTopology: f.topology,
		fanOutExecutor: func(pc *pipeline.PipelineCoordinator) startupownership.FanOutExecutor {
			f.probe.PipelineCoordinator = pc
			return f.probe
		},
	})
	f.ctx = correlation.WithRunID(testAuthorActivityContextForBundle(context.Background(), f.runtime.sourceArtifactFact), f.runID)
	if err := f.runtime.manager.Run(managedConformanceExecutionContextForBundle(t, f.ctx, "fan-out-semantic-proof", f.runtime.sourceArtifactFact)); err != nil {
		t.Fatal(err)
	}
	if prepare == nil {
		publishNotifyAllChildrenRunCreatingEvent(t, f.ctx, f.runtime, f.source, f.runID, "portfolio.opened", map[string]any{"portfolio_id": f.runID, "threshold": 75})
	} else {
		publishSemanticProofRunWithPins(t, f, prepare(f))
	}
	return f
}

func (f *semanticProofFixture) pauseAtEmptyScan(t *testing.T) func() {
	t.Helper()
	gate := make(chan struct{})
	release := sync.OnceFunc(func() { close(gate) })
	t.Cleanup(release)
	observeServingMatrixScans(t, f.runtime, gate)
	f.runtime.pipeline.InstallFanOutWorkNotifier(servingMatrixDroppedWake{calls: make(chan time.Time, 64)})
	return func() { release(); f.runtime.fanOutServing.Wake() }
}

func (f *semanticProofFixture) submit(t *testing.T, event string, rows []map[string]any, threshold int) string {
	t.Helper()
	return publishNotifyAllChildrenEventAsync(t, f.ctx, f.runtime, f.source, f.runID, event, map[string]any{"portfolio_id": f.runID, "account_ids": rows, "threshold": threshold})
}

func semanticProofWait(t *testing.T, check func() (bool, error)) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		done, err := check()
		if err != nil {
			t.Fatal(err)
		}
		if done {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("semantic serving proof did not reach its exact durable predicate within 10s")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func (f *semanticProofFixture) waitIntent(t *testing.T, trigger string, cursor int, status string) {
	t.Helper()
	var got int
	var state string
	reached := false
	defer func() {
		if reached {
			return
		}
		var deliveryStatus string
		var deliveryFailure []byte
		if err := f.db.QueryRowContext(f.ctx, `SELECT status,failure FROM event_deliveries WHERE run_id=$1 AND event_id=$2`, f.runID, trigger).Scan(&deliveryStatus, &deliveryFailure); err == nil {
			t.Logf("trigger delivery status=%s failure=%s", deliveryStatus, deliveryFailure)
		} else {
			t.Logf("trigger delivery readback: %v", err)
		}
		logs, err := f.db.QueryContext(f.ctx, `SELECT payload FROM events WHERE run_id=$1 AND event_name='platform.runtime_log' ORDER BY insertion_sequence DESC LIMIT 3`, f.runID)
		if err == nil {
			defer logs.Close()
			for logs.Next() {
				var payload []byte
				if logs.Scan(&payload) == nil {
					t.Logf("recent runtime log: %s", payload)
				}
			}
		}
		f.probe.mu.Lock()
		policy := f.probe.policies[trigger]
		attempts := append([]semanticProofAttempt(nil), f.probe.attempts[trigger]...)
		blocked, hasBlock := f.probe.blocks[trigger]
		f.probe.mu.Unlock()
		t.Logf("intent closure evidence: trigger=%s want=%d/%s got=%d/%s policy=%+v attempts=%+v blocked=%t failure=%+v", trigger, cursor, status, got, state, policy, attempts, hasBlock, blocked.Failure)
		for {
			select {
			case receipt := <-f.probe.completed:
				t.Logf("serving receipt: trigger=%s key=%+v result=%+v err=%v", receipt.trigger, receipt.key, receipt.result, receipt.err)
			default:
				return
			}
		}
	}()
	semanticProofWait(t, func() (bool, error) {
		err := f.db.QueryRowContext(f.ctx, `SELECT i.cursor,i.status FROM fan_out_intents i JOIN event_deliveries d ON d.delivery_id=i.triggering_delivery_id WHERE i.run_id=$1 AND d.event_id=$2`, f.runID, trigger).Scan(&got, &state)
		if err == sql.ErrNoRows {
			return false, nil
		}
		return got == cursor && state == status, err
	})
	reached = true
}

type semanticProofOutput struct {
	PortfolioID string  `json:"portfolio_id"`
	AccountID   string  `json:"account_id"`
	EngRoles    int64   `json:"eng_roles"`
	GemScore    float64 `json:"gem_score"`
	ExternalID  string  `json:"external_id"`
	Eligible    bool    `json:"eligible"`
	Ordinal     int     `json:"ordinal"`
	Count       int     `json:"source_count"`
	Threshold   int     `json:"snapshot_threshold"`
}

func (f *semanticProofFixture) assertOutcomes(t *testing.T, trigger string, rows []map[string]any, threshold int, rejected map[int]bool) []semanticProofOutput {
	t.Helper()
	result, err := f.db.QueryContext(f.ctx, `SELECT o.ordinal,o.outcome_kind,e.event_id,e.payload_bytes,o.failure FROM fan_out_outcomes o JOIN event_deliveries d ON d.delivery_id=o.triggering_delivery_id LEFT JOIN events e ON e.event_id=o.event_id WHERE o.run_id=$1 AND d.event_id=$2 ORDER BY o.ordinal`, f.runID, trigger)
	if err != nil {
		t.Fatal(err)
	}
	defer result.Close()
	var outputs []semanticProofOutput
	seenIDs := map[string]bool{}
	ordinal := 0
	for result.Next() {
		var got int
		var kind string
		var id sql.NullString
		var payload, failure []byte
		if err := result.Scan(&got, &kind, &id, &payload, &failure); err != nil {
			t.Fatal(err)
		}
		if got != ordinal || ordinal >= len(rows) {
			t.Fatalf("non-contiguous exact intent ordinal=%d expected=%d N=%d", got, ordinal, len(rows))
		}
		if rejected[ordinal] {
			if kind != "semantic_rejected" || id.Valid {
				t.Fatalf("rejection issued an event at %d: kind=%s id=%v", ordinal, kind, id)
			}
			envelope, err := runtimefailures.UnmarshalEnvelope(failure)
			if err != nil || envelope.Detail.Code != "emit_payload_contract_violation" {
				t.Fatalf("rejection lost exact emit evidence: %+v %v", envelope, err)
			}
		} else {
			if kind != "committed" || !id.Valid || seenIDs[id.String] || len(failure) != 0 {
				t.Fatalf("publication identity/evidence at %d: kind=%s id=%v failure=%s", ordinal, kind, id, failure)
			}
			seenIDs[id.String] = true
			var output semanticProofOutput
			if err := json.Unmarshal(payload, &output); err != nil {
				t.Fatal(err)
			}
			row := rows[ordinal]
			if output.PortfolioID != f.runID || output.AccountID != row["account_id"] || output.EngRoles != row["eng_roles"] || output.GemScore != row["gem_score"] || output.ExternalID != row["external_id"] || output.Ordinal != ordinal || output.Count != len(rows) || output.Threshold != threshold || output.Eligible != (threshold >= 70) {
				t.Fatalf("immutable source/schema/order mismatch at %d: output=%+v row=%+v threshold=%d", ordinal, output, row, threshold)
			}
			outputs = append(outputs, output)
		}
		ordinal++
	}
	if err := result.Err(); err != nil {
		t.Fatal(err)
	}
	if ordinal != len(rows) {
		t.Fatalf("outcomes=%d want=%d for %s", ordinal, len(rows), trigger)
	}
	return outputs
}

func semanticProofRows(count int) []map[string]any {
	rows := make([]map[string]any, count)
	for i := range rows {
		// Deliberately repeat complete business content, including identity,
		// across ordinals and the default 32-item boundary.
		rows[i] = map[string]any{"account_id": fmt.Sprintf("repeat-%d", i%3), "eng_roles": int64(7 + i%3), "gem_score": 7.25 + float64(i%3), "external_id": "11111111-1111-4111-8111-111111111111"}
	}
	return rows
}
