package main

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/lib/pq"
	builderpkg "swarm/internal/builder"
	"swarm/internal/config"
	dashboardserver "swarm/internal/dashboard/server"
	"swarm/internal/runtime"
	runtimebootverify "swarm/internal/runtime/bootverify"
	runtimebus "swarm/internal/runtime/bus"
	runtimecontracts "swarm/internal/runtime/contracts"
	runtimecredentials "swarm/internal/runtime/credentials"
	runtimellm "swarm/internal/runtime/llm"
	runtimemanager "swarm/internal/runtime/manager"
	runtimemcp "swarm/internal/runtime/mcp"
	runtimepipeline "swarm/internal/runtime/pipeline"
	"swarm/internal/runtime/semanticview"
	"swarm/internal/runtime/sessions"
	workspace "swarm/internal/runtime/workspace"
	"swarm/internal/store"
)

const (
	defaultPlatformSpecPath = "docs/specs/swarm-platform/platform/contracts/platform-spec.yaml"
	defaultHealthAddr       = ":8081"
)

type storeBundle struct {
	Postgres          *store.PostgresStore
	SQLDB             *sql.DB
	EventStore        runtimebus.EventStore
	SessionRegistry   sessions.Registry
	ConversationStore runtimellm.ConversationPersistence
	ManagerStore      runtimemanager.ManagerPersistence
	ScheduleStore     runtimepipeline.SchedulePersistence
	TurnStore         runtimellm.TurnPersistence
}

func (s storeBundle) runtimeStores() runtime.Stores {
	return runtime.Stores{
		SQLDB:             s.SQLDB,
		EventStore:        s.EventStore,
		SessionRegistry:   s.SessionRegistry,
		ConversationStore: s.ConversationStore,
		ManagerStore:      s.ManagerStore,
		ScheduleStore:     s.ScheduleStore,
		TurnStore:         s.TurnStore,
	}
}

func main() {
	repo := repoRoot()
	if len(os.Args) > 1 && strings.TrimSpace(os.Args[1]) == "verify" {
		os.Exit(runVerifyCommand(context.Background(), repo, os.Args[2:], os.Stdout))
	}
	if len(os.Args) > 1 && strings.TrimSpace(os.Args[1]) == "status" {
		os.Exit(runStatusCommand(context.Background(), repo, os.Args[2:], os.Stdout))
	}
	configPath := flag.String("config", "", "Optional path to Swarm runtime config")
	contractsPath := flag.String("contracts", "", "Path to Swarm contract bundle root")
	platformSpecPath := flag.String("platform-spec", defaultPlatformSpecPath, "Path to platform spec yaml")
	storeMode := flag.String("store", "postgres", "Store mode: postgres")
	healthAddr := flag.String("health-addr", defaultHealthAddr, "HTTP bind address for health checks")
	selfCheck := flag.Bool("self-check", true, "Run runtime self-check during boot")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	resolvedConfigPath := resolvePath(repo, *configPath)
	resolvedContractsPath := resolveContractsPath(repo, *contractsPath)
	resolvedPlatformSpecPath := resolvePath(repo, *platformSpecPath)
	if err := loadRepoDotEnv(repo); err != nil {
		log.Fatalf("load .env: %v", err)
	}

	cfg, err := loadRuntimeConfig(resolvedConfigPath)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}
	stores, err := buildStores(ctx, *storeMode, cfg)
	if err != nil {
		log.Fatalf("init stores: %v", err)
	}
	defer closeDB(stores.SQLDB)
	contractsRoot, err := normalizeContractsRoot(resolvedContractsPath)
	if err != nil {
		log.Fatalf("resolve contracts: %v", err)
	}
	module, bundle, err := newSwarmWorkflowModule(repo, contractsRoot, resolvedPlatformSpecPath)
	if err != nil {
		log.Fatalf("load Swarm contracts: %v", err)
	}
	if err := runtimecontracts.ValidatePromptSchemaGuardsForBundle(bundle); err != nil {
		slog.Error("validate prompt schema guards", "error", err)
		os.Exit(1)
	}
	source := semanticview.Wrap(bundle)
	stateStoreSummary, err := initializeStateStores(ctx, stores, bundle)
	if err != nil {
		slog.Error("initialize state stores", "error", err)
		os.Exit(1)
	}
	workspaces := configuredWorkspaceLifecycle(stores.SQLDB, repo, contractsRoot, source)
	if err := workspaces.ValidateSource(ctx, source); err != nil {
		slog.Error("validate workspaces", "error", err)
		os.Exit(1)
	}
	if err := workspaces.EnsurePrereqs(ctx); err != nil {
		slog.Error("prepare workspaces", "error", err)
		os.Exit(1)
	}
	if err := workspaces.EnsureSystemWorkspaces(ctx); err != nil {
		slog.Error("ensure system workspaces", "error", err)
		os.Exit(1)
	}
	credentialStore, err := buildCredentialStore()
	if err != nil {
		slog.Error("configure credentials", "error", err)
		os.Exit(1)
	}
	validation, err := runtime.ValidateWorkflowContractSurface(ctx, source, runtime.DefaultWorkflowContractValidationOptions(credentialStore))
	bootReport := validation.BootReport
	if err != nil {
		if bootReport.HasErrors() {
			for _, finding := range bootReport.Errors() {
				slog.Error("swarm boot verification failed", "check_id", finding.CheckID, "location", finding.Location, "detail", finding.Message)
			}
		} else {
			slog.Error("workflow contract validation failed", "error", err)
		}
		os.Exit(1)
	}

	rt, err := runtime.NewRuntime(ctx, cfg, stores.runtimeStores(), runtime.RuntimeOptions{
		SelfCheck:          *selfCheck,
		WorkflowModule:     module,
		WorkspaceLifecycle: workspaces,
		EnableToolGateway:  true,
		ToolGatewayToken:   strings.TrimSpace(os.Getenv("SWARM_TOOL_GATEWAY_TOKEN")),
		Credentials:        credentialStore,
	})
	if err != nil {
		log.Fatalf("init runtime: %v", err)
	}

	var ready atomic.Bool
	supervisor := newRuntimeProjectSupervisor(repo, resolvedPlatformSpecPath, cfg, stores, &ready, contractsRoot, bundle, source, rt)
	defer func() {
		if _, err := supervisor.CloseProject(context.Background()); err != nil {
			log.Printf("runtime shutdown failed: %v", err)
		}
	}()
	healthServer := newHealthServer(*healthAddr, &ready, rt.ToolGateway, dashboardServerOptions(supervisor, stores, &ready, cfg.LLM.Session.RotateAfterTurns, credentialStore))
	go serveHealth(healthServer)
	defer shutdownHealthServer(healthServer)

	logBootSkeleton(source, contractsRoot, resolvedPlatformSpecPath, bootReport, stateStoreSummary)
	if err := rt.Start(ctx); err != nil {
		log.Fatalf("start runtime: %v", err)
	}
	ready.Store(true)
	logReadySummary(source, contractsRoot, *healthAddr)

	<-ctx.Done()
	ready.Store(false)
}

func runVerifyCommand(ctx context.Context, repo string, args []string, out io.Writer) int {
	fs := flag.NewFlagSet("verify", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	contractsPath := fs.String("contracts", "", "Path to Swarm contract bundle root")
	platformSpecPath := fs.String("platform-spec", defaultPlatformSpecPath, "Path to platform spec yaml")
	if err := fs.Parse(args); err != nil {
		if out != nil {
			fmt.Fprintf(out, "verify failed: %v\n", err)
		}
		return 2
	}
	if err := loadRepoDotEnv(repo); err != nil {
		if out != nil {
			fmt.Fprintf(out, "verify failed: load .env: %v\n", err)
		}
		return 1
	}
	resolvedContractsPath := resolveContractsPath(repo, *contractsPath)
	resolvedPlatformSpecPath := resolvePath(repo, *platformSpecPath)
	contractsRoot, err := normalizeContractsRoot(resolvedContractsPath)
	if err != nil {
		if out != nil {
			fmt.Fprintf(out, "verify failed: resolve contracts: %v\n", err)
		}
		return 1
	}
	if _, bundle, err := newSwarmWorkflowModule(repo, contractsRoot, resolvedPlatformSpecPath); err != nil {
		if out != nil {
			fmt.Fprintf(out, "verify failed: load Swarm contracts: %v\n", err)
		}
		return 1
	} else {
		if err := verifyBundle(ctx, semanticview.Wrap(bundle)); err != nil {
			if out != nil {
				fmt.Fprintf(out, "verify failed: %v\n", err)
			}
			return 1
		}
		if out != nil {
			fmt.Fprintf(out, "verify ok: contracts=%s\n", contractsRoot)
		}
	}
	return 0
}

type runStatusReport struct {
	RunID             string                    `json:"run_id"`
	RunTableStatus    string                    `json:"run_table_status,omitempty"`
	RootEventID       string                    `json:"root_event_id,omitempty"`
	RootEventType     string                    `json:"root_event_type,omitempty"`
	StartedAt         time.Time                 `json:"started_at,omitempty"`
	LastEventAt       time.Time                 `json:"last_event_at,omitempty"`
	EndedAt           *time.Time                `json:"ended_at,omitempty"`
	EventCount        int                       `json:"event_count"`
	WarnErrorLogCount int                       `json:"warn_error_log_count"`
	EventCounts       []runStatusEventCount     `json:"event_counts"`
	Deliveries        []runStatusDeliveryCount  `json:"deliveries"`
	RecentEvents      []runStatusEvent          `json:"recent_events"`
	DeadLetters       []runStatusDeadLetter     `json:"dead_letters,omitempty"`
	AgentTurns        []runStatusAgentTurn      `json:"agent_turns,omitempty"`
	Heuristics        []string                  `json:"heuristics,omitempty"`
	RuntimeLogSummary []runStatusRuntimeSummary `json:"runtime_log_summary,omitempty"`
	RuntimeLogs       []runStatusRuntimeLog     `json:"runtime_logs,omitempty"`
}

type runStatusEventCount struct {
	EventName string `json:"event_name"`
	Count     int    `json:"count"`
}

type runStatusDeliveryCount struct {
	SubscriberID string `json:"subscriber_id"`
	Status       string `json:"status"`
	Count        int    `json:"count"`
}

type runStatusEvent struct {
	EventName string    `json:"event_name"`
	EntityID  string    `json:"entity_id,omitempty"`
	CreatedAt time.Time `json:"created_at"`
}

type runStatusDeadLetter struct {
	OriginalEvent string    `json:"original_event"`
	EntityID      string    `json:"entity_id,omitempty"`
	FailureType   string    `json:"failure_type"`
	ErrorMessage  string    `json:"error_message,omitempty"`
	HandlerNode   string    `json:"handler_node,omitempty"`
	CreatedAt     time.Time `json:"created_at"`
}

type runStatusAgentTurn struct {
	AgentID    string    `json:"agent_id"`
	Turns      int       `json:"turns"`
	ErrorCount int       `json:"error_count"`
	LastAt     time.Time `json:"last_at"`
}

type runStatusRuntimeLog struct {
	Level     string    `json:"level"`
	Component string    `json:"component"`
	Action    string    `json:"action"`
	Error     string    `json:"error,omitempty"`
	CreatedAt time.Time `json:"created_at"`
}

type runStatusRuntimeSummary struct {
	Level     string `json:"level"`
	Component string `json:"component"`
	Action    string `json:"action"`
	Count     int    `json:"count"`
}

type runStatusOptions struct {
	LogsOnly      bool
	LogsAllLevels bool
	Component     string
}

func runStatusCommand(ctx context.Context, repo string, args []string, out io.Writer) int {
	fs := flag.NewFlagSet("status", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	configPath := fs.String("config", "", "Optional path to Swarm runtime config")
	storeMode := fs.String("store", "postgres", "Store mode: postgres")
	runID := fs.String("run-id", "", "Run ID to inspect; defaults to latest run with scan.requested")
	asJSON := fs.Bool("json", false, "Emit JSON")
	logsOnly := fs.Bool("logs", false, "Show runtime log-focused status output")
	logsAll := fs.Bool("logs-all", false, "Include info-level runtime logs in status output")
	component := fs.String("component", "", "Filter runtime logs to a specific component")
	if err := fs.Parse(args); err != nil {
		if out != nil {
			fmt.Fprintf(out, "status failed: %v\n", err)
		}
		return 2
	}
	if err := loadRepoDotEnv(repo); err != nil {
		if out != nil {
			fmt.Fprintf(out, "status failed: load .env: %v\n", err)
		}
		return 1
	}
	cfg, err := loadRuntimeConfig(resolvePath(repo, *configPath))
	if err != nil {
		if out != nil {
			fmt.Fprintf(out, "status failed: load config: %v\n", err)
		}
		return 1
	}
	stores, err := buildStores(ctx, *storeMode, cfg)
	if err != nil {
		if out != nil {
			fmt.Fprintf(out, "status failed: init stores: %v\n", err)
		}
		return 1
	}
	defer closeDB(stores.SQLDB)
	if stores.SQLDB == nil {
		if out != nil {
			fmt.Fprintln(out, "status failed: postgres store required")
		}
		return 1
	}
	report, err := loadRunStatusReport(ctx, stores.SQLDB, strings.TrimSpace(*runID), runStatusOptions{
		LogsOnly:      *logsOnly,
		LogsAllLevels: *logsAll,
		Component:     strings.TrimSpace(*component),
	})
	if err != nil {
		if out != nil {
			fmt.Fprintf(out, "status failed: %v\n", err)
		}
		return 1
	}
	if *asJSON {
		enc := json.NewEncoder(out)
		enc.SetIndent("", "  ")
		if err := enc.Encode(report); err != nil {
			fmt.Fprintf(out, "status failed: encode json: %v\n", err)
			return 1
		}
		return 0
	}
	printRunStatusReport(out, report)
	return 0
}

func loadRunStatusReport(ctx context.Context, db *sql.DB, runID string, opts runStatusOptions) (runStatusReport, error) {
	if db == nil {
		return runStatusReport{}, errors.New("db is required")
	}
	report := runStatusReport{}
	if strings.TrimSpace(runID) == "" {
		if err := db.QueryRowContext(ctx, `
			SELECT COALESCE(run_id::text, '')
			FROM events
			WHERE event_name = 'scan.requested'
			  AND run_id IS NOT NULL
			ORDER BY created_at DESC
			LIMIT 1
		`).Scan(&runID); err != nil {
			if err == sql.ErrNoRows {
				return runStatusReport{}, errors.New("no current run found")
			}
			return runStatusReport{}, fmt.Errorf("resolve latest run: %w", err)
		}
	}
	report.RunID = strings.TrimSpace(runID)
	if report.RunID == "" {
		return runStatusReport{}, errors.New("run_id is required")
	}

	var (
		runStatus string
		trigger   string
		started   sql.NullTime
		ended     sql.NullTime
	)
	if err := db.QueryRowContext(ctx, `
		SELECT COALESCE(status, ''), COALESCE(trigger_event_type, ''), started_at, ended_at
		FROM runs
		WHERE run_id = $1::uuid
	`, report.RunID).Scan(&runStatus, &trigger, &started, &ended); err == nil {
		report.RunTableStatus = strings.TrimSpace(runStatus)
		if started.Valid {
			report.StartedAt = started.Time
		}
		if ended.Valid {
			tm := ended.Time
			report.EndedAt = &tm
		}
		_ = trigger
	}

	if err := db.QueryRowContext(ctx, `
		SELECT event_id::text, event_name, created_at
		FROM events
		WHERE run_id = $1::uuid
		ORDER BY created_at ASC
		LIMIT 1
	`, report.RunID).Scan(&report.RootEventID, &report.RootEventType, &report.StartedAt); err != nil {
		return runStatusReport{}, fmt.Errorf("load root event: %w", err)
	}
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*), MAX(created_at)
		FROM events
		WHERE run_id = $1::uuid
	`, report.RunID).Scan(&report.EventCount, &report.LastEventAt); err != nil {
		return runStatusReport{}, fmt.Errorf("load event summary: %w", err)
	}

	eventRows, err := db.QueryContext(ctx, `
		SELECT event_name, COUNT(*)
		FROM events
		WHERE run_id = $1::uuid
		GROUP BY event_name
		ORDER BY event_name
	`, report.RunID)
	if err != nil {
		return runStatusReport{}, fmt.Errorf("load event counts: %w", err)
	}
	defer eventRows.Close()
	for eventRows.Next() {
		var item runStatusEventCount
		if err := eventRows.Scan(&item.EventName, &item.Count); err != nil {
			return runStatusReport{}, fmt.Errorf("scan event counts: %w", err)
		}
		report.EventCounts = append(report.EventCounts, item)
	}
	if err := eventRows.Err(); err != nil {
		return runStatusReport{}, fmt.Errorf("read event counts: %w", err)
	}

	deliveryRows, err := db.QueryContext(ctx, `
		SELECT COALESCE(subscriber_id, ''), COALESCE(status, ''), COUNT(*)
		FROM event_deliveries
		WHERE run_id = $1::uuid
		GROUP BY subscriber_id, status
		ORDER BY subscriber_id, status
	`, report.RunID)
	if err != nil {
		return runStatusReport{}, fmt.Errorf("load deliveries: %w", err)
	}
	defer deliveryRows.Close()
	for deliveryRows.Next() {
		var item runStatusDeliveryCount
		if err := deliveryRows.Scan(&item.SubscriberID, &item.Status, &item.Count); err != nil {
			return runStatusReport{}, fmt.Errorf("scan deliveries: %w", err)
		}
		report.Deliveries = append(report.Deliveries, item)
	}
	if err := deliveryRows.Err(); err != nil {
		return runStatusReport{}, fmt.Errorf("read deliveries: %w", err)
	}

	recentRows, err := db.QueryContext(ctx, `
		SELECT event_name, COALESCE(entity_id::text, ''), created_at
		FROM events
		WHERE run_id = $1::uuid
		ORDER BY created_at DESC
		LIMIT 15
	`, report.RunID)
	if err != nil {
		return runStatusReport{}, fmt.Errorf("load recent events: %w", err)
	}
	defer recentRows.Close()
	for recentRows.Next() {
		var item runStatusEvent
		if err := recentRows.Scan(&item.EventName, &item.EntityID, &item.CreatedAt); err != nil {
			return runStatusReport{}, fmt.Errorf("scan recent events: %w", err)
		}
		report.RecentEvents = append(report.RecentEvents, item)
	}
	if err := recentRows.Err(); err != nil {
		return runStatusReport{}, fmt.Errorf("read recent events: %w", err)
	}

	deadRows, err := db.QueryContext(ctx, `
		SELECT
			COALESCE(dl.original_event, ''),
			COALESCE(dl.entity_id::text, ''),
			COALESCE(dl.failure_type, ''),
			COALESCE(dl.error_message, ''),
			COALESCE(dl.handler_node, ''),
			dl.created_at
		FROM dead_letters dl
		INNER JOIN events e ON e.event_id = dl.original_event_id
		WHERE e.run_id = $1::uuid
		ORDER BY dl.created_at DESC
		LIMIT 10
	`, report.RunID)
	if err != nil {
		return runStatusReport{}, fmt.Errorf("load dead letters: %w", err)
	}
	defer deadRows.Close()
	for deadRows.Next() {
		var item runStatusDeadLetter
		if err := deadRows.Scan(&item.OriginalEvent, &item.EntityID, &item.FailureType, &item.ErrorMessage, &item.HandlerNode, &item.CreatedAt); err != nil {
			return runStatusReport{}, fmt.Errorf("scan dead letters: %w", err)
		}
		report.DeadLetters = append(report.DeadLetters, item)
	}
	if err := deadRows.Err(); err != nil {
		return runStatusReport{}, fmt.Errorf("read dead letters: %w", err)
	}

	turnRows, err := db.QueryContext(ctx, `
		SELECT agent_id, COUNT(*), COUNT(*) FILTER (WHERE COALESCE(error, '') <> ''), MAX(created_at)
		FROM agent_turns
		WHERE run_id = $1::uuid
		GROUP BY agent_id
		ORDER BY agent_id
	`, report.RunID)
	if err != nil {
		return runStatusReport{}, fmt.Errorf("load agent turns: %w", err)
	}
	defer turnRows.Close()
	for turnRows.Next() {
		var item runStatusAgentTurn
		if err := turnRows.Scan(&item.AgentID, &item.Turns, &item.ErrorCount, &item.LastAt); err != nil {
			return runStatusReport{}, fmt.Errorf("scan agent turns: %w", err)
		}
		report.AgentTurns = append(report.AgentTurns, item)
	}
	if err := turnRows.Err(); err != nil {
		return runStatusReport{}, fmt.Errorf("read agent turns: %w", err)
	}

	if report.RunID != "" {
		if err := loadRunStatusRuntimeLogs(ctx, db, report.RunID, opts, &report); err != nil {
			return runStatusReport{}, err
		}
	}
	report.Heuristics = deriveRunStatusHeuristics(report)

	return report, nil
}

func loadRunStatusRuntimeLogs(ctx context.Context, db *sql.DB, runID string, opts runStatusOptions, report *runStatusReport) error {
	if db == nil {
		return errors.New("db is required")
	}
	if report == nil {
		return errors.New("report is required")
	}
	logLevels := []string{"warn", "error"}
	if opts.LogsAllLevels {
		logLevels = []string{"info", "warn", "error"}
	}
	componentFilter := strings.TrimSpace(opts.Component)
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM events
		WHERE event_name = 'platform.runtime_log'
		  AND run_id = $1::uuid
		  AND payload->>'log_level' = ANY($2::text[])
		  AND ($3 = '' OR payload->'details'->>'component' = $3)
	`, runID, pq.Array(logLevels), componentFilter).Scan(&report.WarnErrorLogCount); err != nil {
		return fmt.Errorf("load runtime log summary: %w", err)
	}
	logSummaryRows, err := db.QueryContext(ctx, `
		SELECT
			COALESCE(payload->>'log_level', ''),
			COALESCE(payload->'details'->>'component', ''),
			COALESCE(payload->'details'->>'action', ''),
			COUNT(*)
		FROM events
		WHERE event_name = 'platform.runtime_log'
		  AND run_id = $1::uuid
		  AND payload->>'log_level' = ANY($2::text[])
		  AND ($3 = '' OR payload->'details'->>'component' = $3)
		GROUP BY payload->>'log_level', payload->'details'->>'component', payload->'details'->>'action'
		ORDER BY COUNT(*) DESC, payload->'details'->>'component', payload->'details'->>'action'
		LIMIT 12
	`, runID, pq.Array(logLevels), componentFilter)
	if err != nil {
		return fmt.Errorf("load runtime log rollup: %w", err)
	}
	defer logSummaryRows.Close()
	for logSummaryRows.Next() {
		var item runStatusRuntimeSummary
		if err := logSummaryRows.Scan(&item.Level, &item.Component, &item.Action, &item.Count); err != nil {
			return fmt.Errorf("scan runtime log rollup: %w", err)
		}
		report.RuntimeLogSummary = append(report.RuntimeLogSummary, item)
	}
	if err := logSummaryRows.Err(); err != nil {
		return fmt.Errorf("read runtime log rollup: %w", err)
	}
	logRows, err := db.QueryContext(ctx, `
		SELECT
			COALESCE(payload->>'log_level', ''),
			COALESCE(payload->'details'->>'component', ''),
			COALESCE(payload->'details'->>'action', ''),
			COALESCE(payload->'details'->>'error', ''),
			created_at
		FROM events
		WHERE event_name = 'platform.runtime_log'
		  AND run_id = $1::uuid
		  AND payload->>'log_level' = ANY($2::text[])
		  AND ($3 = '' OR payload->'details'->>'component' = $3)
		ORDER BY created_at DESC
		LIMIT $4
	`, runID, pq.Array(logLevels), componentFilter, logLimitForStatus(opts))
	if err != nil {
		return fmt.Errorf("load runtime logs: %w", err)
	}
	defer logRows.Close()
	for logRows.Next() {
		var item runStatusRuntimeLog
		if err := logRows.Scan(&item.Level, &item.Component, &item.Action, &item.Error, &item.CreatedAt); err != nil {
			return fmt.Errorf("scan runtime logs: %w", err)
		}
		report.RuntimeLogs = append(report.RuntimeLogs, item)
	}
	if err := logRows.Err(); err != nil {
		return fmt.Errorf("read runtime logs: %w", err)
	}
	return nil
}

func logLimitForStatus(opts runStatusOptions) int {
	if opts.LogsOnly || opts.LogsAllLevels || strings.TrimSpace(opts.Component) != "" {
		return 100
	}
	return 20
}

func deriveRunStatusHeuristics(report runStatusReport) []string {
	eventCounts := map[string]int{}
	for _, item := range report.EventCounts {
		eventCounts[strings.TrimSpace(item.EventName)] = item.Count
	}
	activeDeliveries := 0
	for _, item := range report.Deliveries {
		switch strings.ToLower(strings.TrimSpace(item.Status)) {
		case "pending", "in_progress":
			activeDeliveries += item.Count
		}
	}
	heuristics := make([]string, 0, 4)
	terminalScoring := eventCounts["scoring/vertical.marginal"] + eventCounts["scoring/vertical.rejected"] + eventCounts["vertical.shortlisted"]
	if activeDeliveries == 0 && eventCounts["scoring/scoring.requested"] > 0 && terminalScoring == 0 {
		heuristics = append(heuristics, "run appears settled after scoring started but no terminal scoring outcome was emitted")
	}
	if strings.EqualFold(strings.TrimSpace(report.RunTableStatus), "running") && activeDeliveries == 0 && !report.LastEventAt.IsZero() {
		heuristics = append(heuristics, "runs table still says running, but there are no pending or in-progress deliveries")
	}
	if len(report.DeadLetters) > 0 {
		heuristics = append(heuristics, "dead letters exist for this run")
	}
	return heuristics
}

func printRunStatusReport(w io.Writer, report runStatusReport) {
	if w == nil {
		return
	}
	fmt.Fprintf(w, "Run %s\n", report.RunID)
	if strings.TrimSpace(report.RootEventType) != "" {
		fmt.Fprintf(w, "Root: %s (%s)\n", report.RootEventType, report.RootEventID)
	}
	if strings.TrimSpace(report.RunTableStatus) != "" {
		fmt.Fprintf(w, "Run Status: %s\n", report.RunTableStatus)
	}
	fmt.Fprintf(w, "Started: %s\n", report.StartedAt.Format(time.RFC3339))
	if !report.LastEventAt.IsZero() {
		fmt.Fprintf(w, "Last Event: %s\n", report.LastEventAt.Format(time.RFC3339))
	}
	if report.EndedAt != nil && !report.EndedAt.IsZero() {
		fmt.Fprintf(w, "Ended: %s\n", report.EndedAt.Format(time.RFC3339))
	}
	fmt.Fprintf(w, "Summary: events=%d deliveries=%d dead_letters=%d agent_turns=%d runtime_warn_errors=%d\n",
		report.EventCount,
		len(report.Deliveries),
		len(report.DeadLetters),
		len(report.AgentTurns),
		report.WarnErrorLogCount,
	)
	if len(report.Heuristics) > 0 {
		fmt.Fprintln(w, "\nHeuristics:")
		for _, item := range report.Heuristics {
			fmt.Fprintf(w, "  %s\n", item)
		}
	}
	if len(report.EventCounts) > 0 {
		fmt.Fprintln(w, "\nEvent Counts:")
		for _, item := range report.EventCounts {
			fmt.Fprintf(w, "  %s  %d\n", item.EventName, item.Count)
		}
	}
	if len(report.Deliveries) > 0 {
		fmt.Fprintln(w, "\nDeliveries:")
		for _, item := range report.Deliveries {
			fmt.Fprintf(w, "  %s  status=%s  count=%d\n", item.SubscriberID, item.Status, item.Count)
		}
	}
	if len(report.AgentTurns) > 0 {
		fmt.Fprintln(w, "\nAgent Turns:")
		for _, item := range report.AgentTurns {
			fmt.Fprintf(w, "  %s  turns=%d errors=%d last=%s\n", item.AgentID, item.Turns, item.ErrorCount, item.LastAt.Format(time.RFC3339))
		}
	}
	if len(report.DeadLetters) > 0 {
		fmt.Fprintln(w, "\nDead Letters:")
		for _, item := range report.DeadLetters {
			fmt.Fprintf(w, "  %s  entity=%s  type=%s  handler=%s  at=%s\n",
				item.OriginalEvent,
				item.EntityID,
				item.FailureType,
				item.HandlerNode,
				item.CreatedAt.Format(time.RFC3339),
			)
			if strings.TrimSpace(item.ErrorMessage) != "" {
				fmt.Fprintf(w, "    error=%s\n", item.ErrorMessage)
			}
		}
	}
	if len(report.RuntimeLogs) > 0 {
		if len(report.RuntimeLogSummary) > 0 {
			fmt.Fprintln(w, "\nRuntime Log Summary:")
			for _, item := range report.RuntimeLogSummary {
				fmt.Fprintf(w, "  %s  %s/%s  count=%d\n",
					strings.ToUpper(item.Level),
					item.Component,
					item.Action,
					item.Count,
				)
			}
		}
		fmt.Fprintln(w, "\nRuntime Warnings/Errors:")
		for _, item := range report.RuntimeLogs {
			fmt.Fprintf(w, "  %s  %s/%s  at=%s\n",
				strings.ToUpper(item.Level),
				item.Component,
				item.Action,
				item.CreatedAt.Format(time.RFC3339),
			)
			if strings.TrimSpace(item.Error) != "" {
				fmt.Fprintf(w, "    error=%s\n", item.Error)
			}
		}
	}
	if len(report.RecentEvents) > 0 {
		fmt.Fprintln(w, "\nRecent Events:")
		for _, item := range report.RecentEvents {
			fmt.Fprintf(w, "  %s  entity=%s  at=%s\n",
				item.EventName,
				item.EntityID,
				item.CreatedAt.Format(time.RFC3339),
			)
		}
	}
}

func verifyBundle(ctx context.Context, source semanticview.Source) error {
	if source == nil {
		return errors.New("semantic source is required")
	}
	credentialStore, err := buildCredentialStore()
	if err != nil {
		return fmt.Errorf("configure credentials: %w", err)
	}
	_, err = runtime.ValidateWorkflowContractSurface(ctx, source, runtime.DefaultWorkflowContractValidationOptions(credentialStore))
	return err
}

func loadRuntimeConfig(path string) (*config.Config, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return defaultRuntimeConfig()
	}
	cfg, err := config.Load(path)
	if err != nil {
		return nil, fmt.Errorf("load %s: %w", path, err)
	}
	return cfg, nil
}

func defaultRuntimeConfig() (*config.Config, error) {
	mode := strings.TrimSpace(os.Getenv("SWARM_LLM_RUNTIME_MODE"))
	if mode == "" {
		if strings.TrimSpace(os.Getenv("ANTHROPIC_API_KEY")) != "" && strings.TrimSpace(os.Getenv("SWARM_CLAUDE_DEFAULT_MODEL")) != "" {
			mode = "api"
		} else {
			mode = "cli_test"
		}
	}
	cfg := &config.Config{
		Runtime: config.RuntimeConfig{
			MaxConcurrentAgents: envInt("SWARM_RUNTIME_MAX_CONCURRENT_AGENTS", 10),
			EventPollInterval:   envDuration("SWARM_RUNTIME_EVENT_POLL_INTERVAL", time.Second),
			RecoveryOnStartup:   envBool("SWARM_RUNTIME_RECOVERY_ON_STARTUP", false),
		},
		Database: config.DatabaseConfig{
			Host:     envOrDefault("SWARM_DB_HOST", envOrDefault("PGHOST", "127.0.0.1")),
			Port:     envInt("SWARM_DB_PORT", envInt("PGPORT", 5432)),
			Name:     envOrDefault("SWARM_DB_NAME", envOrDefault("PGDATABASE", "swarm")),
			User:     envOrDefault("SWARM_DB_USER", envOrDefault("PGUSER", "postgres")),
			Password: envOrDefault("SWARM_DB_PASSWORD", envOrDefault("PGPASSWORD", "postgres")),
			SSLMode:  envOrDefault("SWARM_DB_SSLMODE", "disable"),
			PoolSize: envInt("SWARM_DB_POOL_SIZE", 5),
		},
		LLM: config.LLMConfig{
			RuntimeMode: mode,
			Session: config.LLMSessionConfig{
				LockTTL:               envDuration("SWARM_LLM_SESSION_LOCK_TTL", 10*time.Second),
				RotateAfterTurns:      envInt("SWARM_LLM_SESSION_ROTATE_AFTER_TURNS", 40),
				RotateOnParseFailures: envInt("SWARM_LLM_SESSION_ROTATE_ON_PARSE_FAILURES", 3),
			},
			ClaudeAPI: config.ClaudeAPIConfig{
				DefaultModel: envOrDefault("SWARM_CLAUDE_DEFAULT_MODEL", ""),
				HaikuModel:   envOrDefault("SWARM_CLAUDE_HAIKU_MODEL", ""),
				MaxRetries:   envInt("SWARM_CLAUDE_API_MAX_RETRIES", 1),
				RetryBackoff: envDuration("SWARM_CLAUDE_API_RETRY_BACKOFF", 2*time.Second),
			},
			ClaudeCLI: config.ClaudeCLIConfig{
				Command:              envOrDefault("SWARM_CLAUDE_CLI_COMMAND", "claude"),
				Timeout:              envDuration("SWARM_CLAUDE_CLI_TIMEOUT", 15*time.Minute),
				OutputFormat:         envOrDefault("SWARM_CLAUDE_CLI_OUTPUT_FORMAT", "stream-json"),
				Retries:              envInt("SWARM_CLAUDE_CLI_RETRIES", 1),
				NoSessionPersistence: envBool("SWARM_CLAUDE_CLI_NO_SESSION_PERSISTENCE", false),
				UseTMux:              envBool("SWARM_CLAUDE_CLI_USE_TMUX", false),
			},
		},
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}

func envOrDefault(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}

func envInt(key string, fallback int) int {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback
	}
	value, err := strconv.Atoi(raw)
	if err != nil {
		return fallback
	}
	return value
}

func loadRepoDotEnv(repo string) error {
	repo = strings.TrimSpace(repo)
	if repo == "" {
		return nil
	}
	path := filepath.Join(repo, ".env")
	return loadDotEnvFile(path)
}

func loadDotEnvFile(path string) error {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil
	}
	file, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for lineNo := 1; scanner.Scan(); lineNo++ {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if strings.HasPrefix(line, "export ") {
			line = strings.TrimSpace(strings.TrimPrefix(line, "export "))
		}
		key, value, ok := strings.Cut(line, "=")
		if !ok {
			return fmt.Errorf("%s:%d: expected KEY=VALUE", path, lineNo)
		}
		key = strings.TrimSpace(key)
		if key == "" {
			return fmt.Errorf("%s:%d: empty env key", path, lineNo)
		}
		if _, exists := os.LookupEnv(key); exists {
			continue
		}
		value = strings.TrimSpace(value)
		if unquoted, err := strconv.Unquote(value); err == nil {
			value = unquoted
		} else if len(value) >= 2 {
			if (strings.HasPrefix(value, "'") && strings.HasSuffix(value, "'")) || (strings.HasPrefix(value, "\"") && strings.HasSuffix(value, "\"")) {
				value = value[1 : len(value)-1]
			}
		}
		if err := os.Setenv(key, value); err != nil {
			return fmt.Errorf("%s:%d: set %s: %w", path, lineNo, key, err)
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	return nil
}

func envDuration(key string, fallback time.Duration) time.Duration {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback
	}
	value, err := time.ParseDuration(raw)
	if err != nil {
		return fallback
	}
	return value
}

func envBool(key string, fallback bool) bool {
	raw := strings.TrimSpace(strings.ToLower(os.Getenv(key)))
	switch raw {
	case "1", "true", "yes", "on":
		return true
	case "0", "false", "no", "off":
		return false
	case "":
		return fallback
	default:
		return fallback
	}
}

func normalizeContractsRoot(path string) (string, error) {
	root := strings.TrimSpace(path)
	if root == "" {
		return "", errors.New("contracts path is required")
	}
	root = filepath.Clean(root)
	if _, err := os.Stat(filepath.Join(root, "package.yaml")); err == nil {
		return root, nil
	}
	parent := filepath.Dir(root)
	if parent != root {
		if _, err := os.Stat(filepath.Join(parent, "package.yaml")); err == nil {
			return parent, nil
		}
	}
	return "", fmt.Errorf("no package.yaml found under %s", path)
}

func asString(v any) string {
	switch typed := v.(type) {
	case string:
		return typed
	case []byte:
		return string(typed)
	default:
		return ""
	}
}

func resolveContractsPath(repoRoot, raw string) string {
	if resolved := resolvePath(repoRoot, raw); strings.TrimSpace(resolved) != "" {
		return resolved
	}
	if discovered := strings.TrimSpace(runtimecontracts.DefaultWorkflowContractsDir(repoRoot)); discovered != "" {
		return discovered
	}
	return ""
}

func resolvePath(repoRoot, path string) string {
	path = strings.TrimSpace(path)
	if path == "" || filepath.IsAbs(path) {
		return path
	}
	return filepath.Join(repoRoot, path)
}

func buildStores(ctx context.Context, storeMode string, cfg *config.Config) (storeBundle, error) {
	switch strings.ToLower(strings.TrimSpace(storeMode)) {
	case "postgres":
		dsn := store.DSNFromConfig(cfg.Database)
		pg, err := store.NewPostgresStore(dsn)
		if err != nil {
			return storeBundle{}, err
		}
		if err := pg.Ping(ctx); err != nil {
			return storeBundle{}, err
		}
		if _, err := pg.BindSchemaCapabilities(ctx); err != nil {
			return storeBundle{}, err
		}
		return storeBundle{
			Postgres:          pg,
			SQLDB:             pg.DB,
			EventStore:        pg,
			SessionRegistry:   sessions.NewPostgresRegistry(pg.DB, cfg.LLM.Session.LockTTL),
			ConversationStore: pg,
			ManagerStore:      pg,
			ScheduleStore:     pg,
			TurnStore:         pg,
		}, nil
	default:
		return storeBundle{}, fmt.Errorf("store mode %q is unsupported; postgres is required", strings.TrimSpace(storeMode))
	}
}

func initializeStateStores(ctx context.Context, stores storeBundle, bundle *runtimecontracts.WorkflowContractBundle) (string, error) {
	if stores.Postgres == nil || bundle == nil {
		return "store wiring ready", nil
	}
	platformPlans, err := store.GeneratePlatformTableDDLs(bundle.Platform)
	if err != nil {
		return "", fmt.Errorf("platform-owned tables: %w", err)
	}
	entityPlans, err := store.GenerateEntityTableDDLs(bundle.WorkflowEntitySchema())
	if err != nil {
		return "", fmt.Errorf("entity_schema tables: %w", err)
	}
	statePlans, err := store.GenerateNodeStateTableDDLs(bundle.NodeEntries())
	if err != nil {
		return "", fmt.Errorf("state_schema tables: %w", err)
	}
	plans := append(platformPlans, entityPlans...)
	plans = append(plans, statePlans...)
	if err := stores.Postgres.EnsureSchemaTables(ctx, plans); err != nil {
		return "", err
	}
	tableNames := make([]string, 0, len(plans))
	totalColumns := 0
	for _, plan := range plans {
		tableNames = append(tableNames, fmt.Sprintf("%s(%d)", strings.TrimSpace(plan.TableName), plan.ColumnCount))
		totalColumns += plan.ColumnCount
	}
	sort.Strings(tableNames)
	slog.Info("swarm boot state stores", "tables", len(plans), "columns", totalColumns, "detail", strings.Join(tableNames, ", "))
	if len(tableNames) == 0 {
		return "verified 0 generated tables", nil
	}
	return fmt.Sprintf("verified %d generated tables (%s)", len(plans), strings.Join(tableNames, ", ")), nil
}

type swarmWorkflowModule struct {
	bundle         *runtimecontracts.WorkflowContractBundle
	source         semanticview.Source
	workflow       *runtimepipeline.WorkflowDefinition
	nodes          []runtimepipeline.WorkflowNode
	guardRegistry  runtimepipeline.GuardRegistry
	actionRegistry runtimepipeline.ActionRegistry
}

func newSwarmWorkflowModule(repoRoot, contractsRoot, platformSpecPath string) (runtimepipeline.WorkflowModule, *runtimecontracts.WorkflowContractBundle, error) {
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, contractsRoot, platformSpecPath)
	if err != nil {
		return nil, nil, err
	}
	source := semanticview.Wrap(bundle)
	workflow, err := runtimepipeline.LoadWorkflowDefinition(source)
	if err != nil {
		return nil, nil, err
	}
	nodes, err := runtimepipeline.LoadWorkflowNodes(source)
	if err != nil {
		return nil, nil, err
	}
	return &swarmWorkflowModule{
		bundle:         bundle,
		source:         source,
		workflow:       workflow,
		nodes:          nodes,
		guardRegistry:  runtimepipeline.NewContractGuardRegistry(source),
		actionRegistry: runtimepipeline.NewContractActionRegistry(source),
	}, bundle, nil
}

func (m *swarmWorkflowModule) SemanticSource() semanticview.Source { return m.source }
func (m *swarmWorkflowModule) WorkflowDefinition() *runtimepipeline.WorkflowDefinition {
	return m.workflow
}
func (m *swarmWorkflowModule) WorkflowNodes() []runtimepipeline.WorkflowNode {
	return append([]runtimepipeline.WorkflowNode(nil), m.nodes...)
}
func (m *swarmWorkflowModule) GuardRegistry() runtimepipeline.GuardRegistry { return m.guardRegistry }
func (m *swarmWorkflowModule) ActionRegistry() runtimepipeline.ActionRegistry {
	return m.actionRegistry
}

func logBootSkeleton(source semanticview.Source, contractsRoot, platformSpecPath string, report runtimebootverify.Report, stateStoreSummary string) {
	warningCounts := make(map[string]int, len(report.Findings))
	for _, finding := range report.Warnings() {
		warningCounts[strings.TrimSpace(finding.CheckID)]++
	}
	steps := []struct {
		index int
		name  string
		note  string
	}{
		{1, "load_platform_spec", fmt.Sprintf("loaded %s", filepath.Clean(platformSpecPath))},
		{2, "walk_flow_tree", fmt.Sprintf("discovered %d flow(s)", len(source.FlowSchemaEntries()))},
		{3, "construct_paths", "constructed hierarchical contract paths from package tree"},
		{4, "register_templates", fmt.Sprintf("registered %d template flow(s)", templateFlowCount(source))},
		{5, "build_registries", fmt.Sprintf("nodes=%d agents=%d events=%d tools=%d", len(source.NodeEntries()), len(source.AgentEntries()), len(source.ResolvedEventCatalog()), len(source.ToolEntries()))},
		{6, "resolve_subscriptions", "subscription resolution skeleton in place; full validation lands in CP4"},
		{7, "validate_pins", fmt.Sprintf("validated flow pins and event wiring; warnings=%d", warningCounts["event_chain_integrity"]+warningCounts["event_consumer_exists"]+warningCounts["event_producer_exists"])},
		{8, "validate_required_agents", "validated required_agents coverage and state-machine targets"},
		{9, "validate_tools", fmt.Sprintf("validated tools and prompt coverage; warnings=%d", warningCounts["prompt_exists"]+warningCounts["tool_resolution"])},
		{10, "validate_permissions", "validated platform permission requirements during boot verification"},
		{11, "validate_platform_version", fmt.Sprintf("loaded platform spec version %s for workflow %s", strings.TrimSpace(source.PlatformSpec().Platform.Version), strings.TrimSpace(source.WorkflowVersion()))},
		{12, "initialize_state_stores", fmt.Sprintf("%s (contracts=%s)", strings.TrimSpace(stateStoreSummary), contractsRoot)},
		{13, "start_system_nodes", "delegated to runtime.Start()"},
		{14, "start_agents", "delegated to runtime.Start()"},
		{15, "ready", "health server transitions to ready after runtime start"},
	}
	for _, step := range steps {
		slog.Info("swarm boot", "step", fmt.Sprintf("%02d", step.index), "name", step.name, "detail", step.note)
	}
	for _, finding := range report.Warnings() {
		slog.Warn("swarm boot validation warning", "check_id", finding.CheckID, "location", finding.Location, "detail", finding.Message)
	}
}

func templateFlowCount(source semanticview.Source) int {
	count := 0
	for _, flow := range source.FlowSchemaEntries() {
		if strings.EqualFold(strings.TrimSpace(flow.Mode), "template") {
			count++
		}
	}
	return count
}

func newHealthServer(addr string, ready *atomic.Bool, toolGateway *runtimemcp.Gateway, dashboardOpts dashboardserver.Options) *http.Server {
	mux := http.NewServeMux()
	dashboardHandler := dashboardserver.NewHandler(dashboardOpts)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		if !ready.Load() {
			http.Error(w, "booting", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ready\n"))
	})
	if toolGateway != nil {
		gatewayHandler := toolGateway.Handler()
		mux.Handle("/mcp", gatewayHandler)
		mux.Handle("/tools/", gatewayHandler)
	}
	mux.Handle("/api/", dashboardHandler)
	mux.Handle("/api", dashboardHandler)
	return &http.Server{
		Addr:              strings.TrimSpace(addr),
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
}

func serveHealth(server *http.Server) {
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Printf("health server stopped: %v", err)
	}
}

func shutdownHealthServer(server *http.Server) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Printf("health server shutdown failed: %v", err)
	}
}

func logReadySummary(source semanticview.Source, contractsRoot, healthAddr string) {
	log.Printf(
		"swarm runtime ready contracts=%s flows=%d nodes=%d agents=%d events=%d health=%s",
		contractsRoot,
		len(source.FlowSchemaEntries()),
		len(source.NodeEntries()),
		len(source.AgentEntries()),
		len(source.ResolvedEventCatalog()),
		healthAddr,
	)
}

func dashboardServerOptions(supervisor *runtimeProjectSupervisor, stores storeBundle, ready *atomic.Bool, rotateAfterTurns int, credentialStore runtimecredentials.Store) dashboardserver.Options {
	var (
		agents        dashboardserver.AgentReader
		mailbox       dashboardserver.MailboxReader
		conversations dashboardserver.ConversationReader
		observability dashboardserver.ObservabilityReader
		agentControl  dashboardserver.AgentController
		runtimeCtl    dashboardserver.RuntimeController
	)
	if stores.Postgres != nil {
		agents = dashboardserver.NewSQLAgentReader(stores.Postgres.DB, stores.Postgres, rotateAfterTurns)
		mailbox = stores.Postgres
		conversations = dashboardserver.NewSQLConversationReader(stores.Postgres.DB, stores.Postgres)
		observability = dashboardserver.NewSQLObservabilityReader(stores.Postgres.DB)
	}
	if supervisor != nil {
		agentControl = dashboardDynamicAgentControl{supervisor: supervisor}
		runtimeCtl = dashboardDynamicRuntimeControl{supervisor: supervisor}
	}
	healthFn := func(ctx context.Context) (map[string]any, error) {
		source := semanticview.Source(nil)
		if supervisor != nil {
			source = supervisor.CurrentSource()
		}
		checks := map[string]any{
			"runtime": map[string]any{
				"ready": ready != nil && ready.Load(),
			},
		}
		if source != nil {
			checks["runtime"] = map[string]any{
				"ready":  ready != nil && ready.Load(),
				"flows":  len(source.FlowSchemaEntries()),
				"nodes":  len(source.NodeEntries()),
				"agents": len(source.AgentEntries()),
				"events": len(source.ResolvedEventCatalog()),
			}
		}
		if stores.Postgres != nil {
			dbErr := stores.Postgres.Ping(ctx)
			checks["database"] = map[string]any{
				"ok": dbErr == nil,
			}
			if dbErr != nil {
				checks["database_error"] = dbErr.Error()
			}
		}
		return checks, nil
	}
	var sourceProvider builderpkg.SourceProvider
	var runtimeProvider builderpkg.RuntimeProvider
	var projectControl builderpkg.ProjectController
	if supervisor != nil {
		sourceProvider = supervisor.CurrentSource
		runtimeProvider = supervisor.CurrentRuntime
		projectControl = supervisor
	}
	builderHandler := builderpkg.NewHandler(builderpkg.Options{
		Health:         healthFn,
		Instances:      runtimepipeline.NewWorkflowInstanceStore(stores.SQLDB),
		Runtime:        runtimeCtl,
		Credentials:    credentialStore,
		AuthToken:      strings.TrimSpace(os.Getenv("SWARM_BUILDER_AUTH_TOKEN")),
		Version:        "swarm-dev",
		CurrentSource:  sourceProvider,
		CurrentRuntime: runtimeProvider,
		ProjectControl: projectControl,
	})
	return dashboardserver.Options{
		Health:        healthFn,
		Agents:        agents,
		AgentControl:  agentControl,
		Mailbox:       mailbox,
		Instances:     runtimepipeline.NewWorkflowInstanceStore(stores.SQLDB),
		Conversations: conversations,
		Observability: observability,
		Runtime:       runtimeCtl,
		AuthToken:     strings.TrimSpace(os.Getenv("SWARM_OPERATOR_AUTH_TOKEN")),
		Version:       "swarm-dev",
		Builder:       builderHandler,
	}
}

func buildCredentialStore() (runtimecredentials.Store, error) {
	path := strings.TrimSpace(os.Getenv("SWARM_CREDENTIALS_FILE"))
	if path == "" {
		var err error
		path, err = runtimecredentials.DefaultFilePath()
		if err != nil {
			return nil, err
		}
	}
	fileStore, err := runtimecredentials.NewFileStore(path)
	if err != nil {
		return nil, err
	}
	return runtimecredentials.NewOverlayStore(runtimecredentials.NewEnvStore(), fileStore), nil
}

func configuredWorkspaceLifecycle(db *sql.DB, repoRoot, contractsRoot string, source semanticview.Source) *workspace.DockerManager {
	manager := workspace.NewDockerManager(db)
	cfg := workspace.DefaultDockerConfig()
	if dataDir := strings.TrimSpace(filepath.Join(repoRoot, "data")); dataDir != "" {
		cfg.SharedDataSource = dataDir
	}
	if contractsDir := strings.TrimSpace(contractsRoot); contractsDir != "" {
		cfg.ContractsSource = contractsDir
	}
	manager.SetConfig(cfg)
	manager.SetSemanticSource(source)
	return manager
}

func repoRoot() string {
	dir, err := os.Getwd()
	if err != nil {
		log.Fatalf("resolve cwd: %v", err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			log.Fatalf("locate repo root from %s", dir)
		}
		dir = parent
	}
}

func closeDB(db *sql.DB) {
	if db == nil {
		return
	}
	if err := db.Close(); err != nil {
		log.Printf("close db: %v", err)
	}
}
