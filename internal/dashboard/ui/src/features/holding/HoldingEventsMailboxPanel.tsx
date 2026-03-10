import React from "react";
import { formatDollars, relTime } from "../../lib/format.ts";

export default function HoldingEventsMailboxPanel({ events, mailbox, spend }) {
  return (
    <div className="row">
      <div className="holding-detail-section">
        <div className="tiny" style={{ marginBottom: 6 }}>Recent Events</div>
        {(events || []).length === 0 ? (
          <div className="empty-state">No events</div>
        ) : (
          <table>
            <thead><tr><th>When</th><th>Type</th><th>Source</th></tr></thead>
            <tbody>
              {(events || []).slice(0, 20).map((event) => (
                <tr key={event.id}>
                  <td>{relTime(event.created_at)}</td>
                  <td>{event.type}</td>
                  <td>{event.source_agent}</td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>
      <div className="holding-detail-section">
        <div className="tiny" style={{ marginBottom: 6 }}>Mailbox + Spend</div>
        <div className="stack" style={{ marginBottom: 6 }}>
          <span className="badge">30d spend: {formatDollars((spend && spend.last_30d_cents) || 0)}</span>
          <span className="badge">all-time: {formatDollars((spend && spend.all_time_cents) || 0)}</span>
        </div>
        {(mailbox || []).length === 0 ? (
          <div className="empty-state">No mailbox items</div>
        ) : (
          <table>
            <thead><tr><th>When</th><th>Type</th><th>Status</th><th>Summary</th></tr></thead>
            <tbody>
              {(mailbox || []).slice(0, 20).map((item) => (
                <tr key={item.id}>
                  <td>{relTime(item.created_at)}</td>
                  <td>{item.type}</td>
                  <td>{item.status}</td>
                  <td>{item.summary || "-"}</td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>
    </div>
  );
}
