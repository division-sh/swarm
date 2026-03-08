import { fetchAgents } from "./agents.js";
import { fetchJSON } from "./client.js";
import { fetchHealth } from "./health.js";

export async function fetchOverview() {
  return (await fetchJSON("/dashboard/api/overview")) || {};
}

export async function fetchDigest(top = 10) {
  return (await fetchJSON(`/dashboard/api/digest?top=${encodeURIComponent(top)}`)) || null;
}

export async function fetchTasks(status) {
  const p = new URLSearchParams();
  p.set("status", status || "open");
  p.set("limit", "250");
  const d = await fetchJSON(`/api/tasks?${p.toString()}`);
  return {
    tasks: d.tasks || [],
    weekly_budget: d.weekly_budget || {},
  };
}

export async function fetchTaskStats() {
  return (await fetchJSON("/api/tasks/stats")) || null;
}

export async function fetchMailbox(status) {
  const d = await fetchJSON(`/api/mailbox?status=${encodeURIComponent(status || "all")}&limit=150`);
  return {
    summary: d.summary || {},
    items: d.items || [],
  };
}

export async function fetchTargets() {
  const d = await fetchJSON("/dashboard/api/control/targets");
  return d.targets || [];
}

export async function fetchDashboardHealth() {
  return (await fetchHealth()) || {};
}

export async function fetchDashboardAgents() {
  return fetchAgents();
}
