import React from "react";
import CopyID from "../../components/CopyID.jsx";
import { fmtTime, relTime } from "../../lib/format.ts";
import { deriveMailboxDerivedState } from "./useMailboxDerivedState.ts";

export default function ControlView({ state, actions, onOpenWorkflowTrace, onOpenPortfolio, onOpenRelatedTaskForVertical }) {
  const {
    targets,
    mailbox,
    controlOutput,
    controlTarget,
    directiveMessage,
    chatMessage,
    chatMode,
    verticalName,
    verticalGeo,
    verticalSlug,
    requeueEventID,
    requeueAgentID,
    resetConfirm,
    mailStatus,
    mailboxID,
    mailboxAction,
    mailboxNotes,
    selectedMailboxItem,
  } = state;
  const {
    setControlTarget,
    setDirectiveMessage,
    setChatMessage,
    setChatMode,
    setVerticalName,
    setVerticalGeo,
    setVerticalSlug,
    setRequeueEventID,
    setRequeueAgentID,
    setResetConfirm,
    setMailStatus,
    setMailboxID,
    setMailboxAction,
    setMailboxNotes,
    setSelectedMailboxItem,
    sendDirective,
    sendChat,
    restartControlTarget,
    replayControlTarget,
    createVertical,
    requeueEvent,
    seedOrg,
    pauseRuntime,
    resumeRuntime,
    resetDBAndSeed,
    wipeDB,
    decideMailbox,
    quickMailboxDecide,
  } = actions;
  const resetOK = (resetConfirm || "").trim() === "RESET";
  const mailboxDerived = deriveMailboxDerivedState({ mailbox, selectedMailboxItem });
  const selectedMailbox = mailboxDerived.selected;

  return (
    <div className="layout-two">
      <section>
        <div className="head"><h2>Control Panel</h2><span className="tiny">execute actions</span></div>
        <div className="body scroll">
          <div className="control-card">
            <div className="tiny">Target Agent</div>
            <select value={controlTarget} onChange={(e) => setControlTarget(e.target.value)} style={{ width: "100%", marginTop: 4 }}>
              {targets.map((t) => <option key={t.agent_id} value={t.agent_id}>{t.agent_id} | {t.role || "-"} | {t.vertical_slug || t.status || "-"}</option>)}
            </select>
          </div>

          <div className="control-card">
            <div className="tiny">Directive</div>
            <textarea style={{ width: "100%", marginTop: 4 }} placeholder="Message to agent" value={directiveMessage} onChange={(e) => setDirectiveMessage(e.target.value)} />
            <div className="stack" style={{ marginTop: 6 }}>
              <button disabled={!directiveMessage.trim()} onClick={() => sendDirective(controlTarget, directiveMessage)}>Send Directive</button>
            </div>
          </div>

          <div className="control-card">
            <div className="tiny">Chat</div>
            <textarea style={{ width: "100%", marginTop: 4 }} placeholder="Chat message" value={chatMessage} onChange={(e) => setChatMessage(e.target.value)} />
            <div className="stack" style={{ marginTop: 6 }}>
              <select value={chatMode} onChange={(e) => setChatMode(e.target.value)}>
                <option value="live">live</option>
                <option value="async">async</option>
              </select>
              <button disabled={!chatMessage.trim()} onClick={() => sendChat(controlTarget, chatMode, chatMessage)}>Send Chat</button>
            </div>
          </div>

          <div className="control-card">
            <div className="tiny">Agent Recovery</div>
            <div className="stack" style={{ marginTop: 4 }}>
              <button className="btn-secondary" onClick={() => {
                if (!window.confirm(`Restart agent "${controlTarget}"? This will interrupt any in-progress work.`)) return;
                restartControlTarget(controlTarget);
              }}>Restart</button>
              <button className="btn-secondary" onClick={() => {
                if (!window.confirm(`Replay backlog for "${controlTarget}"?`)) return;
                replayControlTarget(controlTarget);
              }}>Replay Backlog</button>
            </div>
          </div>

          <div className="control-card">
            <div className="tiny">Create Vertical + OpCo</div>
            <div className="stack" style={{ marginTop: 4 }}>
              <input placeholder="Vertical name" value={verticalName} onChange={(e) => setVerticalName(e.target.value)} />
              <input placeholder="Geography" value={verticalGeo} onChange={(e) => setVerticalGeo(e.target.value)} />
              <input placeholder="slug (optional)" value={verticalSlug} onChange={(e) => setVerticalSlug(e.target.value)} />
              <button onClick={() => createVertical({ name: verticalName, geography: verticalGeo, slug: verticalSlug })}>Create</button>
            </div>
          </div>

          <div className="control-card">
            <div className="tiny">Event Requeue</div>
            <div className="stack" style={{ marginTop: 4 }}>
              <input className="mono" style={{ minWidth: 180 }} placeholder="event id" value={requeueEventID} onChange={(e) => setRequeueEventID(e.target.value)} />
              <select value={requeueAgentID} onChange={(e) => setRequeueAgentID(e.target.value)}>
                <option value="">all delivered recipients</option>
                {targets.map((t) => <option key={t.agent_id} value={t.agent_id}>{t.agent_id}</option>)}
              </select>
              <button className="btn-secondary" onClick={() => requeueEvent({ eventID: requeueEventID, agentID: requeueAgentID })}>Requeue</button>
            </div>
          </div>

          <div className="control-card">
            <div className="tiny">Org Bootstrap + Danger Zone</div>
            <div className="stack" style={{ marginTop: 4 }}>
              <button onClick={() => seedOrg()}>Seed Org</button>
              <button className="btn-secondary" onClick={() => pauseRuntime()}>Pause</button>
              <button className="btn-secondary" onClick={() => resumeRuntime()}>Resume</button>
            </div>
            <details style={{ marginTop: 8 }}>
              <summary className="tiny" style={{ cursor: "pointer" }}>Danger Zone</summary>
              <div className="stack" style={{ marginTop: 8 }}>
                <input className="mono" placeholder='type RESET to unlock' value={resetConfirm} onChange={(e) => setResetConfirm(e.target.value)} />
                <button className="btn-danger" disabled={!resetOK} onClick={() => resetDBAndSeed(resetConfirm, setResetConfirm)}>Reset DB + Seed</button>
                <button className="btn-danger" disabled={!resetOK} onClick={() => wipeDB(resetConfirm, setResetConfirm)}>Wipe DB</button>
              </div>
            </details>
          </div>

          <div className="json" style={{ maxHeight: 160 }}>{JSON.stringify(controlOutput, null, 2)}</div>
        </div>
      </section>

      <section>
        <div className="head">
          <h2>Mailbox + Decisions</h2>
          <select value={mailStatus} onChange={(e) => setMailStatus(e.target.value)}>
            <option value="all">all</option>
            <option value="pending">pending</option>
            <option value="approved">approved</option>
            <option value="rejected">rejected</option>
            <option value="timed_out">timed_out</option>
          </select>
        </div>
        <div className="body scroll">
          <div className="metrics-grid" style={{ marginBottom: 10 }}>
            <div className={`metric-card${mailboxDerived.summary.pending > 0 ? " warn" : ""}`}>
              <div className="metric-label">Pending</div>
              <div className="metric-value">{mailboxDerived.summary.pending}</div>
              <div className="tiny">{mailbox.summary.pending || 0} reported in summary</div>
            </div>
            <div className={`metric-card${mailboxDerived.summary.critical > 0 ? " warn" : ""}`}>
              <div className="metric-label">Critical</div>
              <div className="metric-value">{mailboxDerived.summary.critical}</div>
              <div className="tiny">Highest-priority human decisions</div>
            </div>
            <div className="metric-card">
              <div className="metric-label">Decided</div>
              <div className="metric-value">{mailboxDerived.summary.decided}</div>
              <div className="tiny">{mailbox.summary.decided || 0} total resolved</div>
            </div>
            <div className="metric-card">
              <div className="metric-label">Loaded</div>
              <div className="metric-value">{mailboxDerived.summary.loaded}</div>
              <div className="tiny">Current filter: {mailStatus}</div>
            </div>
          </div>

          <div className="row body" style={{ marginBottom: 10 }}>
            <div className="card">
              <div className="tiny" style={{ marginBottom: 8 }}>Critical Queue</div>
              <div className="body" style={{ gap: 8 }}>
                {mailboxDerived.queue.critical.length > 0 ? mailboxDerived.queue.critical.map((item) => (
                  <div key={item.id} className="health-kv">
                    <div>
                      <div>{item.summary || item.type || item.id}</div>
                      <div className="tiny">{[item.from_agent, item.vertical_slug || item.vertical_id, item.priority].filter(Boolean).join(" · ")}</div>
                    </div>
                    <button className="btn-secondary" onClick={() => {
                      setSelectedMailboxItem(item.id);
                      setMailboxID(item.id);
                    }}>Review</button>
                  </div>
                )) : (
                  <div className="empty-state">No critical mailbox items in this filter.</div>
                )}
              </div>
            </div>

            <div className="card">
              <div className="tiny" style={{ marginBottom: 8 }}>Selected Request</div>
              {selectedMailbox ? (
                <div className="body" style={{ gap: 8 }}>
                  <div className="health-kv"><span>Type</span><span className="badge">{selectedMailbox.type || "-"}</span></div>
                  <div className="health-kv"><span>Status</span><span>{selectedMailbox.status || "-"}</span></div>
                  <div className="health-kv"><span>Priority</span><span>{selectedMailbox.priority || "-"}</span></div>
                  <div className="health-kv"><span>From</span><span className="mono">{selectedMailbox.from_agent || "-"}</span></div>
                  <div className="health-kv"><span>Vertical</span><span className="mono">{selectedMailbox.vertical_slug || selectedMailbox.vertical_id || "-"}</span></div>
                  <div className="tiny">{selectedMailbox.summary || "No request summary provided."}</div>
                  <div className="stack" style={{ marginTop: 6 }}>
                    {selectedMailbox.vertical_slug || selectedMailbox.vertical_id ? (
                      <>
                        <button className="btn-secondary" onClick={() => onOpenWorkflowTrace?.(selectedMailbox.vertical_slug || selectedMailbox.vertical_id)}>Workflow</button>
                        <button className="btn-secondary" onClick={() => onOpenPortfolio?.(selectedMailbox.vertical_slug || selectedMailbox.vertical_id)}>Portfolio</button>
                        <button className="btn-secondary" onClick={() => onOpenRelatedTaskForVertical?.(selectedMailbox.vertical_slug || selectedMailbox.vertical_id)}>Related Task</button>
                      </>
                    ) : null}
                  </div>
                </div>
              ) : (
                <div className="empty-state">Select a mailbox item to review the request context and apply a decision.</div>
              )}
            </div>
          </div>

          <div className="tiny" style={{ marginTop: 8 }}>Mailbox Decision</div>
          <div className="stack" style={{ marginBottom: 8 }}>
            <input className="mono" style={{ minWidth: 120 }} placeholder="mailbox id" value={mailboxID} onChange={(e) => setMailboxID(e.target.value)} />
            <select value={mailboxAction} onChange={(e) => setMailboxAction(e.target.value)}>
              <option value="approve">approve</option>
              <option value="reject">reject</option>
              <option value="more-data">more-data</option>
              <option value="kill">kill</option>
              <option value="revise">revise</option>
              <option value="skip">skip</option>
              <option value="respond">respond</option>
            </select>
            <input placeholder="notes" value={mailboxNotes} onChange={(e) => setMailboxNotes(e.target.value)} />
            <button onClick={() => decideMailbox(mailboxID, mailboxAction, mailboxNotes)}>Decide</button>
          </div>

          <div className="body scroll" style={{ maxHeight: "52vh", padding: 0 }}>
            <table>
              <thead><tr><th>ID</th><th>Request</th><th>Status</th><th>Priority</th><th>Vertical</th><th>Agent</th><th>Age</th><th>Action</th></tr></thead>
              <tbody>
                {mailbox.items.length === 0 ? (
                  <tr><td colSpan={8} className="empty-state">No mailbox items</td></tr>
                ) : mailbox.items.map((m) => {
                  const expanded = selectedMailboxItem === m.id;
                  return (
                    <React.Fragment key={m.id}>
                      <tr style={{ cursor: "pointer" }} onClick={() => {
                        const next = expanded ? "" : m.id;
                        setSelectedMailboxItem(next);
                        setMailboxID(next);
                      }}>
                        <td><CopyID id={m.id} /></td>
                        <td>
                          <div>{m.summary || m.type || "-"}</div>
                          <div className="tiny">{m.type || "-"}</div>
                        </td>
                        <td>{m.status}</td>
                        <td>{m.priority}</td>
                        <td className="mono">{m.vertical_slug || m.vertical_id || "-"}</td>
                        <td>{m.from_agent}</td>
                        <td><span title={fmtTime(m.created_at)}>{relTime(m.created_at)}</span></td>
                        <td>
                          {m.status === "pending" ? (
                            <div className="stack" onClick={(e) => e.stopPropagation()}>
                              <button onClick={() => quickMailboxDecide(m.id, "approve")}>approve</button>
                              <button className="btn-secondary" onClick={() => {
                                if (!window.confirm(`Reject mailbox item from ${m.from_agent}?`)) return;
                                quickMailboxDecide(m.id, "reject");
                              }}>reject</button>
                            </div>
                          ) : m.decided_action || "-"}
                        </td>
                      </tr>
                      {expanded ? (
                        <tr>
                          <td colSpan={8} className="agent-drop-cell">
                            <div style={{ padding: "10px 14px" }}>
                              <div className="tiny" style={{ marginBottom: 8 }}>Request Details</div>
                              <div className="health-kv"><span>Status</span><span>{m.status || "-"}</span></div>
                              <div className="health-kv"><span>Priority</span><span>{m.priority || "-"}</span></div>
                              <div className="health-kv"><span>Vertical</span><span className="mono">{m.vertical_slug || m.vertical_id || "-"}</span></div>
                              <div className="health-kv"><span>From</span><span className="mono">{m.from_agent || "-"}</span></div>
                              <div className="health-kv"><span>Created</span><span title={fmtTime(m.created_at)}>{relTime(m.created_at)}</span></div>
                              {m.summary ? <div className="tiny" style={{ marginTop: 8 }}>{m.summary}</div> : null}
                              <div className="stack" style={{ marginTop: 8 }}>
                                <button onClick={() => quickMailboxDecide(m.id, "approve")}>Approve</button>
                                <button className="btn-secondary" onClick={() => quickMailboxDecide(m.id, "more-data")}>More Data</button>
                                <button className="btn-secondary" onClick={() => {
                                  if (!window.confirm(`Reject mailbox item from ${m.from_agent}?`)) return;
                                  quickMailboxDecide(m.id, "reject");
                                }}>Reject</button>
                              </div>
                              <details style={{ marginTop: 10 }}>
                                <summary className="tiny" style={{ cursor: "pointer" }}>Raw Request Data</summary>
                                <pre className="json" style={{ maxHeight: 240, marginTop: 8 }}>{JSON.stringify(
                                  Object.fromEntries(Object.entries(m).filter(([k]) => !["id"].includes(k))),
                                  null, 2
                                )}</pre>
                              </details>
                            </div>
                          </td>
                        </tr>
                      ) : null}
                    </React.Fragment>
                  );
                })}
              </tbody>
            </table>
          </div>
        </div>
      </section>
    </div>
  );
}
