import React from "react";
import HoldingArtifactPayload from "./artifacts/HoldingArtifactPayload.jsx";

export default function HoldingArtifactsPanel({ artifacts }) {
  return (
    <div className="holding-detail-section">
      <div className="tiny" style={{ marginBottom: 6 }}>Artifacts</div>
      {(artifacts || []).map(([label, payload]) => (
        <details key={label} className="holding-artifact-card" open={label === "Business Brief" || label === "Scores"}>
          <summary>{label}</summary>
          <HoldingArtifactPayload label={label} payload={payload} />
        </details>
      ))}
    </div>
  );
}
