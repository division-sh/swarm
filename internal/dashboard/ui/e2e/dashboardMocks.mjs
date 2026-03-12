const NOW = "2026-03-08T12:00:00Z";

const vertical = {
  id: "v-alpha",
  slug: "alpha-ai",
  name: "Alpha AI",
  stage: "ready_for_review",
  geography: "US",
  mode: "saas_gap",
  workflow_current_state: "validation",
  stage_entered_at: "2026-03-05T08:00:00Z",
  active_timer_count: 1,
  revision_count: 2,
  template_version: "2.1.0",
  created_at: "2026-03-01T09:00:00Z",
  updated_at: NOW,
  composite_score: 8.7,
};

const agents = [
  {
    id: "holding-manager",
    role: "holding-manager",
    state: "stuck",
    status: "healthy",
    vertical_slug: "holding",
    current_task_id: "task-alpha-review",
    turn_count: 18,
    turn_limit: 24,
    turns_24h: 12,
    total_tokens_24h: 182000,
    pending_events: 3,
    oldest_pending_age_sec: 3600,
    in_flight_turn: false,
    last_tool: { name: "vertical_scorer", ok: false, result: "timeout" },
    stuck_reason: "waiting on human review",
    failures_24h: 2,
    dead_letters_24h: 1,
    near_breaker: true,
    session_id: "sess-hold-1",
    runtime_mode: "runtime",
    lock_owner: "lease-1",
    lease_expires_at: "2026-03-08T12:05:00Z",
    last_active_at: NOW,
  },
  {
    id: "empire-coordinator",
    role: "coordinator",
    state: "running",
    status: "healthy",
    vertical_slug: "holding",
    current_task_id: "",
    turn_count: 9,
    turn_limit: 24,
    turns_24h: 9,
    total_tokens_24h: 94000,
    pending_events: 0,
    oldest_pending_age_sec: 0,
    in_flight_turn: true,
    in_flight_seconds: 80,
    last_tool: { name: "campaign_builder", ok: true, result: "ok" },
    session_id: "sess-coord-1",
    runtime_mode: "runtime",
    last_active_at: NOW,
  },
  {
    id: "alpha-builder",
    role: "builder",
    state: "idle",
    status: "healthy",
    vertical_slug: "alpha-ai",
    current_task_id: "",
    turn_count: 3,
    turn_limit: 24,
    turns_24h: 3,
    total_tokens_24h: 12000,
    pending_events: 1,
    oldest_pending_age_sec: 900,
    in_flight_turn: false,
    last_tool: { name: "landing_page", ok: true, result: "updated" },
    session_id: "sess-builder-1",
    runtime_mode: "runtime",
    last_active_at: NOW,
  },
];

const tasks = [
  {
    id: "task-alpha-review",
    status: "open",
    priority: "p1",
    category: "validation",
    description: "Review Alpha AI validation package and approve or reject.",
    requesting_agent: "holding-manager",
    vertical_slug: "alpha-ai",
    created_at: "2026-03-07T14:00:00Z",
    assigned_to: "",
    deadline: "2026-03-09T12:00:00Z",
  },
  {
    id: "task-alpha-followup",
    status: "completed",
    priority: "p2",
    category: "research",
    description: "Gather additional competitor evidence.",
    requesting_agent: "empire-coordinator",
    vertical_slug: "alpha-ai",
    created_at: "2026-03-06T09:00:00Z",
    assigned_to: "operator",
    deadline: "",
  },
];

const mailboxItems = [
  {
    id: "mail-alpha-1",
    type: "approval_request",
    status: "pending",
    priority: "high",
    from_agent: "holding-manager",
    created_at: "2026-03-08T11:00:00Z",
    summary: "Alpha AI ready for review",
    decided_action: "",
    vertical_slug: "alpha-ai",
  },
];

const conversations = [
  { agent_id: "holding-manager", updated_at: NOW, message_count: 2 },
  { agent_id: "empire-coordinator", updated_at: NOW, message_count: 1 },
];

const conversationDetail = {
  messages: [
    { role: "system", text: "Coordinate holding reviews." },
    { role: "assistant", text: "Alpha AI is ready for review." },
  ],
  turns: [
    {
      id: "turn-1",
      started_at: "2026-03-08T11:30:00Z",
      finished_at: "2026-03-08T11:31:00Z",
      mode: "runtime",
      tool_calls: [{ name: "vertical_scorer", ok: false }],
    },
  ],
};

const runtimeLogs = [
  {
    id: "log-1",
    ts: NOW,
    created_at: NOW,
    source: "holding-manager",
    component: "workflow",
    level: "error",
    type: "runtime.log",
    message: "Validation timer exceeded.",
    error_code: "MCP_TIMEOUT",
    vertical: "alpha-ai",
    vertical_id: "alpha-ai",
    agent_id: "holding-manager",
  },
  {
    id: "log-2",
    ts: NOW,
    created_at: NOW,
    source: "empire-coordinator",
    component: "runtime",
    level: "info",
    type: "runtime.log",
    message: "Coordinator healthy.",
    error_code: "",
    vertical: "",
    vertical_id: "",
    agent_id: "empire-coordinator",
  },
];

const incidents = [
  {
    code: "MCP_TIMEOUT",
    count: 2,
    last_seen: NOW,
    component: "workflow",
    level: "warn",
  },
];

const events = [
  {
    id: "evt-1",
    type: "vertical.ready_for_review",
    event_type: "vertical.ready_for_review",
    created_at: NOW,
    source: "holding-manager",
    source_agent: "holding-manager",
    subscriber: "mailbox",
    vertical: "alpha-ai",
  },
];

const graphNodes = [
  { id: "holding-manager", kind: "agent", group: "holding", role: "manager", status: "healthy", vertical_slug: "holding" },
  { id: "vertical.ready_for_review", kind: "event", group: "workflow", label: "vertical.ready_for_review" },
  { id: "mailbox", kind: "mailbox", group: "system", label: "Mailbox" },
  { id: "human-review", kind: "human", group: "system", label: "Human Review" },
  { id: "alpha-builder", kind: "agent", group: "opco", role: "builder", status: "healthy", vertical_slug: "alpha-ai", system_prompt: "You operate the Alpha AI build lane." },
];

const graphEdges = [
  { from: "holding-manager", to: "vertical.ready_for_review", kind: "producer", event_type: "vertical.ready_for_review", label: "emits" },
  { from: "vertical.ready_for_review", to: "mailbox", kind: "routing", event_type: "vertical.ready_for_review", label: "routes" },
  { from: "mailbox", to: "human-review", kind: "routing", event_type: "mailbox.item.created", label: "review" },
  { from: "human-review", to: "alpha-builder", kind: "routing", event_type: "vertical.approved", label: "launch" },
];

const workflowGraph = {
  nodes: graphNodes,
  edges: graphEdges,
  meta: {
    workflow_name: "empire",
    workflow_version: "2.1.0",
    platform_version: "2.1.0",
    stages: ["discovery", "scoring", "validation", "mailbox", "opco"],
    rubrics: ["universal"],
    workflow_stages: ["discovery", "scoring", "validation", "mailbox", "opco"],
    timer_events: ["timer.portfolio_digest"],
    event_stage_map: {
      "vertical.ready_for_review": ["validation"],
      "mailbox.item.created": ["mailbox"],
      "vertical.approved": ["opco"],
    },
    sources: ["contracts/workflow-schema.yaml"],
    node_count: graphNodes.length,
    edge_count: graphEdges.length,
  },
  flow_events: [
    {
      id: "flow-1",
      timestamp: NOW,
      event_type: "vertical.ready_for_review",
      source_node: "holding-manager",
      target_nodes: ["mailbox"],
      vertical_slug: "alpha-ai",
    },
    {
      id: "flow-2",
      timestamp: NOW,
      event_type: "vertical.approved",
      source_node: "human-review",
      target_nodes: ["alpha-builder"],
      vertical_slug: "alpha-ai",
    },
  ],
};

const holdingData = {
  campaigns: [],
  verticals: [vertical],
  agent_counts: { holding: 2, alpha_ai: 1 },
  summary: { total: 1 },
  workflow_summary: {
    drift: 1,
    active_timers: 1,
    revisioned: 1,
    stale: 1,
  },
};

const health = {
  runtime: { running: true, loaded_agents: agents.length },
  postgres: { active_connections: 4, max_connections: 50 },
  auth: { oauth_token_configured: true, auth_errors_1h: 0, auth_errors_24h: 1 },
  containers: [{ name: "dashboard", status: "running" }],
  spend: {
    api_cost_24h_cents: 1234,
    api_cost_daily_avg_7d_cents: 1100,
    infra_cost_24h_cents: 456,
    spend_ledger_24h_cents: 1690,
  },
  workflow_audit: {
    warnings: ["alpha-ai stage drifted from database stage"],
  },
  contracts: {
    paths: {
      workflow: "contracts/workflow-schema.yaml",
      platform: "contracts/platform/platform-spec.yaml",
      verification: "contracts/verification-gates.yaml",
    },
    workflow: {
      name: "empire",
      version: "2.1.0",
      stage_ids: ["discovery", "scoring", "validation", "mailbox", "opco"],
      transition_count: 8,
      timer_count: 2,
    },
    platform: {
      version: "2.1.0",
      compliance_rule_count: 4,
    },
    verification: {
      count: 6,
      priority_counts: { must_pass: 4 },
      status: "definitions-only",
      latest_results: "not persisted",
    },
  },
  vertical_health: [
    { vertical_id: vertical.id, slug: vertical.slug, health_status: "healthy", deploy_status: "live" },
  ],
};

const promptState = {
  effective_prompt: "You are the holding manager. Keep portfolio execution moving.",
  source: "default",
  has_override: false,
  updated_at: NOW,
};

const promptDiff = {
  original: promptState.effective_prompt,
  modified: `${promptState.effective_prompt}\n\nEscalate urgent blockers immediately.`,
};

function json(body, status = 200) {
  return {
    status,
    contentType: "application/json; charset=utf-8",
    body: JSON.stringify(body),
  };
}

function deepClone(value) {
  return JSON.parse(JSON.stringify(value));
}

function filterTasks(items, status) {
  if (!status || status === "all") return items;
  return items.filter((task) => task.status === status);
}

function deriveTaskStats(items) {
  return items.reduce((acc, task) => {
    const key = String(task.status || "").trim() || "unknown";
    acc[key] = (acc[key] || 0) + 1;
    return acc;
  }, {});
}

function deriveMailboxSummary(items) {
  return items.reduce((acc, item) => {
    const status = String(item.status || "").toLowerCase();
    if (status === "pending") acc.pending += 1;
    if (status === "approved") acc.approved += 1;
    if (status === "rejected") acc.rejected += 1;
    if (status === "deferred") acc.deferred += 1;
    if (status !== "pending") acc.decided += 1;
    if (status === "pending" && String(item.priority || "").toLowerCase() === "critical") acc.critical += 1;
    return acc;
  }, { pending: 0, approved: 0, rejected: 0, deferred: 0, critical: 0, decided: 0 });
}

function filterMailbox(items, status) {
  if (!status || status === "all") return items;
  return items.filter((item) => String(item.status || "") === status);
}

function filterLogs(items, searchParams) {
  const source = searchParams.get("source") || "";
  const level = searchParams.get("level") || "";
  const errorCode = searchParams.get("error_code") || "";
  return items.filter((log) => {
    if (source && log.source !== source) return false;
    if (level && log.level !== level) return false;
    if (errorCode && log.error_code !== errorCode) return false;
    return true;
  });
}

export async function installDashboardMocks(page) {
  const taskState = deepClone(tasks);
  const mailboxState = deepClone(mailboxItems);
  const runtimeLogState = deepClone(runtimeLogs);

  await page.addInitScript(() => {
    class MockEventSource {
      constructor(url) {
        this.url = url;
        this.readyState = 1;
        this.onopen = null;
        this.onmessage = null;
        this.onerror = null;
        queueMicrotask(() => {
          if (typeof this.onopen === "function") this.onopen({ type: "open" });
        });
      }
      addEventListener() {}
      removeEventListener() {}
      close() {
        this.readyState = 2;
      }
    }
    window.EventSource = MockEventSource;
  });

  await page.route("https://fonts.googleapis.com/**", (route) => route.fulfill({ status: 204, body: "" }));
  await page.route("https://fonts.gstatic.com/**", (route) => route.fulfill({ status: 204, body: "" }));

  await page.route(/http:\/\/127\.0\.0\.1:\d+\/(?:api|dashboard\/api)\//, async (route) => {
    const req = route.request();
    const url = new URL(req.url());
    const path = url.pathname;

    if (req.method() !== "GET") {
      if (path.startsWith("/api/tasks/")) {
        const [, , , taskID, action] = path.split("/");
        const task = taskState.find((item) => item.id === taskID);
        if (!task) {
          await route.fulfill(json({ error: `Unknown task ${taskID}` }, 404));
          return;
        }
        if (action === "claim") {
          task.status = "assigned";
          task.assigned_to = "operator";
        } else if (action === "complete") {
          const body = req.postDataJSON?.() || {};
          task.status = "completed";
          task.result_text = body.result_text || "";
          task.outcome = body.outcome || "success";
          task.follow_up_needed = !!body.follow_up_needed;
        } else if (action === "reject") {
          const body = req.postDataJSON?.() || {};
          task.status = "rejected";
          task.reject_reason = body.reason || "";
        }
        await route.fulfill(json({ ok: true, task }));
        return;
      }

      if (path.startsWith("/api/mailbox/") && path.endsWith("/decide")) {
        const [, , , mailboxID] = path.split("/");
        const item = mailboxState.find((entry) => entry.id === mailboxID);
        if (!item) {
          await route.fulfill(json({ error: `Unknown mailbox item ${mailboxID}` }, 404));
          return;
        }
        const body = req.postDataJSON?.() || {};
        const action = String(body.action || "").trim();
        item.decided_action = action;
        if (action === "approve") item.status = "approved";
        else if (action === "reject" || action === "kill") item.status = "rejected";
        else if (action === "more-data" || action === "defer" || action === "revise") item.status = "deferred";
        else if (action) item.status = "approved";
        await route.fulfill(json({ ok: true, item }));
        return;
      }

      await route.fulfill(json({ ok: true, message: "mock action completed" }));
      return;
    }

    if (path === "/dashboard/api/overview") {
      await route.fulfill(json({
        generated_at: NOW,
        agents_active: 2,
        events_24h: 14,
      }));
      return;
    }
    if (path === "/dashboard/api/agents") {
      await route.fulfill(json({
        states: { running: 1, idle: 1, stuck: 1, terminated: 0 },
        agents,
      }));
      return;
    }
    if (path === "/dashboard/api/digest") {
      await route.fulfill(json({
        current: { text: "Alpha AI remains the top review candidate this week." },
        last_compiled: { at: NOW },
      }));
      return;
    }
    if (path === "/api/tasks") {
      const status = url.searchParams.get("status") || "open";
      await route.fulfill(json({
        tasks: filterTasks(taskState, status),
        weekly_budget: {
          approved_this_week: 1,
          max_tasks_per_week: 5,
          reset_day: "monday",
          week_start_utc: "2026-03-02T00:00:00Z",
        },
      }));
      return;
    }
    if (path === "/api/tasks/stats") {
      await route.fulfill(json(deriveTaskStats(taskState)));
      return;
    }
    if (path === "/api/mailbox") {
      const status = url.searchParams.get("status") || "all";
      await route.fulfill(json({
        summary: deriveMailboxSummary(mailboxState),
        items: filterMailbox(mailboxState, status),
      }));
      return;
    }
    if (path === "/dashboard/api/control/targets") {
      await route.fulfill(json({
        targets: [
          { agent_id: "holding-manager", role: "holding-manager", vertical_slug: "holding", status: "stuck" },
          { agent_id: "alpha-builder", role: "builder", vertical_slug: "alpha-ai", status: "idle" },
          { agent_id: "empire-coordinator", role: "coordinator", vertical_slug: "holding", status: "running" },
        ],
      }));
      return;
    }
    if (path === "/dashboard/api/health") {
      await route.fulfill(json(health));
      return;
    }
    if (path === "/dashboard/api/funnel") {
      await route.fulfill(json({
        throughput: {
          discoveries_14d: 12,
        },
        stuck: [
          {
            vertical_slug: vertical.slug,
            stage: vertical.workflow_current_state,
            age_sec: 5400,
          },
        ],
      }));
      return;
    }
    if (path === "/dashboard/api/pipeline/shards") {
      await route.fulfill(json({
        scans: [
          {
            scan_id: "scan-us-1",
            geography: "US",
            mode: "market_scan",
            shards_failed: 1,
            shards_stuck: 0,
            status: "partial",
          },
        ],
      }));
      return;
    }
    if (path === "/dashboard/api/pipeline/shards/scan-us-1") {
      await route.fulfill(json({
        shards: [
          { shard_id: "us-1", status: "failed", last_error: "timeout" },
          { shard_id: "us-2", status: "ok", last_error: "" },
        ],
      }));
      return;
    }
    if (path === `/dashboard/api/verticals/${vertical.slug}/trace`) {
      await route.fulfill(json({
        trace: [
          { created_at: NOW, type: "vertical.discovered", source_agent: "empire-coordinator" },
          { created_at: NOW, type: "vertical.ready_for_review", source_agent: "holding-manager" },
        ],
      }));
      return;
    }
    if (path === "/api/verticals") {
      await route.fulfill(json({ verticals: [vertical] }));
      return;
    }
    if (path === "/dashboard/api/holding") {
      await route.fulfill(json(holdingData));
      return;
    }
    if (path === "/dashboard/api/holding/vertical") {
      await route.fulfill(json({
        vertical,
        business_model: "SaaS",
        opportunity: "Automate competitor monitoring",
        artifacts: [],
        agents,
        events,
        mailbox: mailboxState,
        spend: { last_30d_cents: 1234, all_time_cents: 3456 },
      }));
      return;
    }
    if (path === "/api/events") {
      await route.fulfill(json({ events }));
      return;
    }
    if (path === "/api/events/evt-1") {
      await route.fulfill(json({
        id: "evt-1",
        created_at: NOW,
        type: "vertical.ready_for_review",
        source_agent: "holding-manager",
        payload: { vertical_slug: vertical.slug },
        deliveries: [{ agent_id: "mailbox", status: "delivered" }],
      }));
      return;
    }
    if (path === "/api/runtime/logs") {
      await route.fulfill(json({ runtime_logs: filterLogs(runtimeLogState, url.searchParams) }));
      return;
    }
    if (path === "/api/runtime/incidents") {
      await route.fulfill(json({ incidents }));
      return;
    }
    if (path === "/dashboard/api/conversations") {
      await route.fulfill(json({ conversations }));
      return;
    }
    if (path === "/dashboard/api/conversations/holding-manager") {
      await route.fulfill(json(conversationDetail));
      return;
    }
    if (path === "/dashboard/api/conversations/empire-coordinator") {
      await route.fulfill(json({ messages: [{ role: "assistant", text: "Coordinator online." }], turns: [] }));
      return;
    }
    if (path === "/dashboard/api/conversations/holding-manager/artifacts") {
      await route.fulfill(json({
        prompt: promptState.effective_prompt,
        latest_turn: conversationDetail.turns[0],
      }));
      return;
    }
    if (path === "/dashboard/api/conversations/empire-coordinator/artifacts") {
      await route.fulfill(json({ prompt: "Coordinate the empire." }));
      return;
    }
    if (path === "/api/graph") {
      await route.fulfill(json({
        nodes: graphNodes,
        edges: graphEdges,
      }));
      return;
    }
    if (path === "/api/pipeline/graph") {
      await route.fulfill(json(workflowGraph));
      return;
    }
    if (path === "/api/agents/holding-manager/prompt") {
      await route.fulfill(json(promptState));
      return;
    }
    if (path === "/api/agents/holding-manager/prompt/diff") {
      await route.fulfill(json(promptDiff));
      return;
    }
    if (path === "/api/agents/empire-coordinator/prompt") {
      await route.fulfill(json({
        ...promptState,
        effective_prompt: "You are the empire coordinator.",
      }));
      return;
    }
    if (path === "/api/agents/empire-coordinator/prompt/diff") {
      await route.fulfill(json(promptDiff));
      return;
    }
    if (path === "/api/agents/alpha-builder/prompt") {
      await route.fulfill(json({
        ...promptState,
        effective_prompt: "You are the Alpha AI builder.",
      }));
      return;
    }
    if (path === "/api/agents/alpha-builder/prompt/diff") {
      await route.fulfill(json(promptDiff));
      return;
    }

    await route.fulfill(json({ error: `No mock for ${path}` }, 404));
  });
}

export function trackPageErrors(page) {
  const errors = [];
  page.on("pageerror", (error) => {
    const message = typeof error?.message === "string"
      ? error.message
      : typeof error?.toString === "function"
        ? error.toString()
        : "";
    if (!message || message === "[object Event]") return;
    errors.push(error);
  });
  return errors;
}
