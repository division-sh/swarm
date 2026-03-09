import { useMemo } from "react";

function normalize(value) {
  return String(value || "").trim();
}

function verticalKeys(vertical) {
  const keys = new Set();
  const slug = normalize(vertical?.slug);
  const id = normalize(vertical?.id);
  if (slug) keys.add(slug);
  if (id) keys.add(id);
  return keys;
}

function itemVerticalKeys(item) {
  const keys = new Set();
  [
    item?.vertical_slug,
    item?.vertical_id,
    item?.vertical,
    item?.slug,
  ].forEach((value) => {
    const next = normalize(value);
    if (next) keys.add(next);
  });
  return keys;
}

function matchesVertical(item, keys) {
  if (!item || keys.size === 0) return false;
  for (const key of itemVerticalKeys(item)) {
    if (keys.has(key)) return true;
  }
  return false;
}

function sortTasks(tasks) {
  const order = { open: 0, assigned: 1, pending_review: 2, approved: 3, deferred: 4, completed: 5, rejected: 6, expired: 7 };
  return [...tasks].sort((a, b) => (
    (order[a.status] ?? 99) - (order[b.status] ?? 99)
      || `${a.priority || ""}`.localeCompare(`${b.priority || ""}`)
      || `${a.id || ""}`.localeCompare(`${b.id || ""}`)
  ));
}

function sortMailbox(items) {
  const statusOrder = { pending: 0, timed_out: 1, approved: 2, rejected: 3 };
  const priorityOrder = { critical: 0, high: 1, normal: 2, low: 3 };
  return [...items].sort((a, b) => (
    (statusOrder[a.status] ?? 99) - (statusOrder[b.status] ?? 99)
      || (priorityOrder[a.priority] ?? 99) - (priorityOrder[b.priority] ?? 99)
      || `${a.id || ""}`.localeCompare(`${b.id || ""}`)
  ));
}

function sortAgents(agents) {
  return [...agents].sort((a, b) => {
    const aRole = String(a.role || "");
    const bRole = String(b.role || "");
    const aScore = aRole.includes("ceo") ? 0 : aRole.includes("coordinator") ? 1 : 2;
    const bScore = bRole.includes("ceo") ? 0 : bRole.includes("coordinator") ? 1 : 2;
    return aScore - bScore
      || `${a.state || ""}`.localeCompare(`${b.state || ""}`)
      || `${a.id || a.agent_id || ""}`.localeCompare(`${b.id || b.agent_id || ""}`);
  });
}

export function derivePortfolioDownstreamState({
  verticals,
  focusSummary,
  tasks,
  mailboxItems,
  targets,
  agents,
}) {
  const rows = Array.isArray(verticals) ? verticals : [];
  const allTasks = Array.isArray(tasks) ? tasks : [];
  const allMailbox = Array.isArray(mailboxItems) ? mailboxItems : [];
  const allTargets = Array.isArray(targets) ? targets : [];
  const allAgents = Array.isArray(agents) ? agents : [];

  const byKey = {};

  for (const vertical of rows) {
    const keys = verticalKeys(vertical);
    if (keys.size === 0) continue;

    const relatedTasks = sortTasks(allTasks.filter((task) => matchesVertical(task, keys)));
    const relatedMailbox = sortMailbox(allMailbox.filter((item) => matchesVertical(item, keys)));
    const relatedTargets = sortAgents(allTargets.filter((target) => matchesVertical(target, keys)));
    const relatedAgents = sortAgents(allAgents.filter((agent) => matchesVertical(agent, keys)));

    const snapshot = {
      vertical,
      relatedTasks,
      relatedMailbox,
      relatedTargets,
      relatedAgents,
      primaryTask: relatedTasks[0] || null,
      primaryMailbox: relatedMailbox[0] || null,
      primaryTarget: relatedTargets[0] || null,
      primaryAgent: relatedAgents[0] || relatedTargets[0] || null,
      summary: {
        tasks: relatedTasks.length,
        mailbox: relatedMailbox.length,
        pendingMailbox: relatedMailbox.filter((item) => item.status === "pending").length,
        agents: relatedAgents.length,
        targets: relatedTargets.length,
      },
    };

    for (const key of keys) {
      byKey[key] = snapshot;
    }
  }

  const focusKey = normalize(focusSummary?.key);
  const current = (focusKey && byKey[focusKey]) || {
    vertical: focusSummary?.vertical || null,
    relatedTasks: [],
    relatedMailbox: [],
    relatedTargets: [],
    relatedAgents: [],
    primaryTask: null,
    primaryMailbox: null,
    primaryTarget: null,
    primaryAgent: null,
    summary: { tasks: 0, mailbox: 0, pendingMailbox: 0, agents: 0, targets: 0 },
  };

  return {
    current,
    byKey,
  };
}

export function usePortfolioDownstreamState({
  verticals,
  focusSummary,
  tasks,
  mailboxItems,
  targets,
  agents,
}) {
  return useMemo(() => derivePortfolioDownstreamState({
    verticals,
    focusSummary,
    tasks,
    mailboxItems,
    targets,
    agents,
  }), [agents, focusSummary, mailboxItems, targets, tasks, verticals]);
}
