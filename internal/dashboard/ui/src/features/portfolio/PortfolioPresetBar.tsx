import React from "react";

function PresetButton({ label, count, onClick }) {
  return (
    <button className="btn-secondary" onClick={onClick}>
      {label}
      <span className="badge" style={{ marginLeft: 6 }}>{count}</span>
    </button>
  );
}

export default function PortfolioPresetBar({
  presetCounts,
  savedViews,
  onApplyPreset,
  onSaveView,
  onApplySavedView,
}) {
  return (
    <div className="health-card" style={{ marginBottom: 10 }}>
      <div className="stack" style={{ justifyContent: "space-between", marginBottom: 8 }}>
        <div>
          <div className="tiny">Portfolio Presets</div>
          <div>Quick-open the common triage slices and save your current portfolio context for later.</div>
        </div>
      </div>

      <div className="stack" style={{ marginBottom: 8, flexWrap: "wrap" }}>
        <PresetButton label="Drift Only" count={presetCounts.drift} onClick={() => onApplyPreset("drift")} />
        <PresetButton label="Timers" count={presetCounts.timers} onClick={() => onApplyPreset("timers")} />
        <PresetButton label="Revisions" count={presetCounts.revisions} onClick={() => onApplyPreset("revisions")} />
        <PresetButton label="Stale" count={presetCounts.stale} onClick={() => onApplyPreset("stale")} />
        <PresetButton label="Human Needed" count={presetCounts.humanNeeded} onClick={() => onApplyPreset("humanNeeded")} />
        <PresetButton label="Shard Failures" count={presetCounts.shardFailures} onClick={() => onApplyPreset("shardFailures")} />
      </div>

      <div className="row3">
        {savedViews.map((view, index) => (
          <div key={`portfolio-saved-view:${index}`} className="health-card">
            <div className="tiny">Saved View {index + 1}</div>
            <div style={{ minHeight: 20 }}>{view ? view.label : "Empty slot"}</div>
            <div className="tiny" style={{ marginBottom: 6 }}>
              {view ? `${view.subview} • ${view.focusKey || "no focus"} • ${view.holdingFilter}` : "Save the current triage context"}
            </div>
            <div className="stack">
              <button className="btn-secondary" onClick={() => onSaveView(index)}>Save</button>
              <button className="btn-secondary" disabled={!view} onClick={() => onApplySavedView(index)}>Open</button>
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}
