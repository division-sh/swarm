import React from "react";
import CopyID from "../../components/CopyID.jsx";
import { fmtTime, relTime } from "../../lib/format.js";

export default function ControlView({ state, actions }) {
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
            <div className="stack" style={{ marginTop: 6 }}>
              <input className="mono" placeholder='type RESET to unlock' value={resetConfirm} onChange={(e) => setResetConfirm(e.target.value)} />
              <button className="btn-danger" disabled={!resetOK} onClick={() => resetDBAndSeed(resetConfirm, setResetConfirm)}>Reset DB + Seed</button>
              <button className="btn-danger" disabled={!resetOK} onClick={() => wipeDB(resetConfirm, setResetConfirm)}>Wipe DB</button>
            </div>
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
          <div className="stack tiny">
            <span className="badge">pending {mailbox.summary.pending || 0}</span>
            <span className="badge">critical {mailbox.summary.critical || 0}</span>
            <span className="badge">decided {mailbox.summary.decided || 0}</span>
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
              <thead><tr><th>ID</th><th>Type</th><th>Status</th><th>Priority</th><th>Agent</th><th>Age</th><th>Action</th></tr></thead>
              <tbody>
                {mailbox.items.length === 0 ? (
                  <tr><td colSpan={7} className="empty-state">No mailbox items</td></tr>
                ) : mailbox.items.map((m) => {
                  const expanded = selectedMailboxItem === m.id;
                  return (
                    <React.Fragment key={m.id}>
                      <tr style={{ cursor: "pointer" }} onClick={() => setSelectedMailboxItem(expanded ? "" : m.id)}>
                        <td><CopyID id={m.id} /></td>
                        <td>{m.type}</td>
                        <td>{m.status}</td>
                        <td>{m.priority}</td>
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
                          <td colSpan={7} className="agent-drop-cell">
                            <div style={{ padding: "10px 14px" }}>
                              <div className="tiny">Request Details</div>
                              <pre className="json" style={{ maxHeight: 240 }}>{JSON.stringify(
                                Object.fromEntries(Object.entries(m).filter(([k]) => !["id"].includes(k))),
                                null, 2
                              )}</pre>
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
