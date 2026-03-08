import React from "react";

export default function StatusDot({ state }) {
  const cls = `status-dot status-dot-${state || "idle"}`;
  return <span className={cls} />;
}
