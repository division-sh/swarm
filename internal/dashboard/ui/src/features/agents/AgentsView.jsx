import React from "react";
import StatusDot from "../../components/StatusDot.jsx";

function counts(agents) {
  return (agents || []).reduce((acc, a) => {
    const key = a.state || "idle";
    acc[key] = (acc[key] || 0) + 1;
    return acc;
  }, {});
}

export default function AgentsView({
  domain,
  controls,
  actions,
  AgentTable,
}) {
  const { agentsResp, groupedAgents } = domain;
  const { agentSearch, setAgentSearch, selectedAgentID, setSelectedAgentID } = controls;
  const { renderAgentDropdown, navigateToTask } = actions;

  return (
    <section>
      <div className="head">
        <h2>Agent Activity</h2>
        <div className="stack tiny">
          <input className="agent-search" placeholder="Search agents…" value={agentSearch} onChange={(e) => setAgentSearch(e.target.value)} />
          <span className="badge b-running"><StatusDot state="running" />running {agentsResp.states.running || 0}</span>
          <span className="badge b-idle"><StatusDot state="idle" />idle {agentsResp.states.idle || 0}</span>
          <span className="badge b-stuck"><StatusDot state="stuck" />stuck {agentsResp.states.stuck || 0}</span>
        </div>
      </div>
      <div className="body scroll">
        <details className="agent-group" open>
          <summary>
            <div className="group-head">
              <strong>Holding</strong>
              <span className="badge">total {groupedAgents.holding.length}</span>
              <span className="badge b-running"><StatusDot state="running" />run {counts(groupedAgents.holding).running || 0}</span>
              <span className="badge b-idle"><StatusDot state="idle" />idle {counts(groupedAgents.holding).idle || 0}</span>
              <span className="badge b-stuck"><StatusDot state="stuck" />stuck {counts(groupedAgents.holding).stuck || 0}</span>
            </div>
          </summary>
          <div className="group-body">
            <AgentTable
              agents={groupedAgents.holding}
              selectedAgentID={selectedAgentID}
              onSelectAgent={setSelectedAgentID}
              renderDropdown={renderAgentDropdown}
              onNavigateTask={navigateToTask}
            />
          </div>
        </details>

        {groupedAgents.opcos.map((g) => {
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
                />
              </div>
            </details>
          );
        })}
      </div>
    </section>
  );
}
