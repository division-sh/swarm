import React from "react";
import AgentTable from "../../components/AgentTable.tsx";
import StatusDot from "../../components/StatusDot.tsx";
import { useAgentTriageState } from "./useAgentTriageState.ts";

function counts(agents) {
  return (agents || []).reduce((acc, a) => {
    const key = a.state || "idle";
    acc[key] = (acc[key] || 0) + 1;
    return acc;
  }, {});
}

export default function AgentsView({ state, actions }) {
  const { agentsResp, groupedAgents, agentSearch, selectedAgentID } = state;
  const { setAgentSearch, setSelectedAgentID, renderAgentDropdown, navigateToTask } = actions;
  const triage = useAgentTriageState({ groupedAgents, agentsResp });

  return (
    <section>
      <div className="head">
        <h2>Agent Activity</h2>
        <div className="stack tiny">
          <input className="agent-search" aria-label="Search agents" placeholder="Search agents…" value={agentSearch} onChange={(e) => setAgentSearch(e.target.value)} />
          <select aria-label="Agent focus mode" value={triage.focus} onChange={(e) => triage.setFocus(e.target.value)} title="Focus">
            <option value="all">all agents</option>
            <option value="attention">attention queue</option>
            <option value="pinned">pinned only</option>
          </select>
          <select aria-label="Agent state filter" value={triage.stateFilter} onChange={(e) => triage.setStateFilter(e.target.value)} title="State">
            <option value="all">all states</option>
            <option value="stuck">stuck</option>
            <option value="running">running</option>
            <option value="idle">idle</option>
            <option value="terminated">terminated</option>
          </select>
          <span className="badge">visible {triage.visibleHolding.length + triage.visibleOpcos.reduce((acc, group) => acc + group.agents.length, 0)}</span>
          <span className="badge b-stuck"><StatusDot state="stuck" />attention {triage.summary.attention}</span>
          <span className="badge b-running">leases {triage.summary.leases}</span>
          <span className="badge b-idle">pins {triage.summary.pinned}</span>
          <span className="badge b-running"><StatusDot state="running" />running {agentsResp.states.running || 0}</span>
          <span className="badge b-idle"><StatusDot state="idle" />idle {agentsResp.states.idle || 0}</span>
          <span className="badge b-stuck"><StatusDot state="stuck" />stuck {agentsResp.states.stuck || 0}</span>
        </div>
      </div>
      <div className="body scroll">
        {triage.summary.total > 0 ? (
          <div className="stack tiny" style={{ marginBottom: 12 }}>
            <span className="badge">pending {triage.summary.pending}</span>
            <span className="badge">near breaker {triage.summary.breaker}</span>
            <span className="badge">failed tools {triage.summary.failedTools}</span>
          </div>
        ) : null}

        {triage.attentionAgents.length > 0 && triage.focus !== "pinned" ? (
          <details className="agent-group" open>
            <summary>
              <div className="group-head">
                <strong>Attention Queue</strong>
                <span className="badge">total {triage.attentionAgents.length}</span>
                <span className="badge b-stuck"><StatusDot state="stuck" />stuck {counts(triage.attentionAgents).stuck || 0}</span>
                <span className="badge b-running">pending {triage.attentionAgents.filter((agent) => Number(agent.pending_events || 0) > 0).length}</span>
                <span className="badge b-idle">leases {triage.attentionAgents.filter((agent) => !!agent.lock_owner || !!agent.in_flight_turn).length}</span>
              </div>
            </summary>
            <div className="group-body">
              <AgentTable
                agents={triage.attentionAgents}
                selectedAgentID={selectedAgentID}
                onSelectAgent={setSelectedAgentID}
                renderDropdown={renderAgentDropdown}
                onNavigateTask={navigateToTask}
                pinnedAgentIDs={triage.pinnedAgentIDs}
                onTogglePinned={triage.togglePinned}
                sortMode="attention"
              />
            </div>
          </details>
        ) : null}

        {triage.pinnedAgents.length > 0 ? (
          <details className="agent-group" open={triage.focus === "pinned"}>
            <summary>
              <div className="group-head">
                <strong>Pinned Agents</strong>
                <span className="badge">total {triage.pinnedAgents.length}</span>
                <span className="badge b-stuck"><StatusDot state="stuck" />stuck {counts(triage.pinnedAgents).stuck || 0}</span>
              </div>
            </summary>
            <div className="group-body">
              <AgentTable
                agents={triage.pinnedAgents}
                selectedAgentID={selectedAgentID}
                onSelectAgent={setSelectedAgentID}
                renderDropdown={renderAgentDropdown}
                onNavigateTask={navigateToTask}
                pinnedAgentIDs={triage.pinnedAgentIDs}
                onTogglePinned={triage.togglePinned}
                sortMode="attention"
              />
            </div>
          </details>
        ) : null}

        <details className="agent-group" open>
          <summary>
            <div className="group-head">
              <strong>Holding</strong>
              <span className="badge">total {triage.visibleHolding.length}</span>
              <span className="badge b-running"><StatusDot state="running" />run {counts(triage.visibleHolding).running || 0}</span>
              <span className="badge b-idle"><StatusDot state="idle" />idle {counts(triage.visibleHolding).idle || 0}</span>
              <span className="badge b-stuck"><StatusDot state="stuck" />stuck {counts(triage.visibleHolding).stuck || 0}</span>
            </div>
          </summary>
          <div className="group-body">
            <AgentTable
              agents={triage.visibleHolding}
              selectedAgentID={selectedAgentID}
              onSelectAgent={setSelectedAgentID}
              renderDropdown={renderAgentDropdown}
              onNavigateTask={navigateToTask}
              pinnedAgentIDs={triage.pinnedAgentIDs}
              onTogglePinned={triage.togglePinned}
            />
          </div>
        </details>

        {triage.visibleOpcos.map((g) => {
          const c = counts(g.agents);
          const hasStuck = (c.stuck || 0) > 0;
          return (
            <details className="agent-group" key={g.slug} open={hasStuck}>
              <summary>
                <div className="group-head">
                  <strong>OpCO: {g.slug}</strong>
                  <span className="badge">total {g.agents.length}</span>
                  <span className="badge b-running"><StatusDot state="running" />run {c.running || 0}</span>
                  <span className="badge b-idle"><StatusDot state="idle" />idle {c.idle || 0}</span>
                  <span className="badge b-stuck"><StatusDot state="stuck" />stuck {c.stuck || 0}</span>
                </div>
              </summary>
              <div className="group-body">
                <AgentTable
                  agents={g.agents}
                  selectedAgentID={selectedAgentID}
                  onSelectAgent={setSelectedAgentID}
                  renderDropdown={renderAgentDropdown}
                  onNavigateTask={navigateToTask}
                  pinnedAgentIDs={triage.pinnedAgentIDs}
                  onTogglePinned={triage.togglePinned}
                />
              </div>
            </details>
          );
        })}
        {triage.visibleHolding.length === 0 && triage.visibleOpcos.length === 0 && triage.pinnedAgents.length === 0 && triage.attentionAgents.length === 0 ? (
          <div className="empty-state">No agents match the current filters.</div>
        ) : null}
      </div>
    </section>
  );
}
