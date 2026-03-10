import React from "react";

export default function Toasts({ items }) {
  if (!items || items.length === 0) return null;
  return (
    <div className="toast-container">
      {items.map((t) => (
        <div key={t.id} className={`toast toast-${t.type}`}>{t.msg}</div>
      ))}
    </div>
  );
}
