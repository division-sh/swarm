import React, { useEffect, useState } from "react";

export default function Modal({ title, onClose, copyText, children, className = "" }) {
  const [copied, setCopied] = useState(false);

  useEffect(() => {
    function onKey(e) {
      if (e.key === "Escape") onClose();
    }
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [onClose]);

  return (
    <div className="modal-overlay" onClick={onClose}>
      <div className={`modal-container ${className}`.trim()} onClick={(e) => e.stopPropagation()}>
        <div className="modal-header">
          <div className="modal-title">{title}</div>
          <div className="stack">
            {copyText ? (
              <button className="btn-secondary" onClick={() => {
                navigator.clipboard.writeText(copyText).catch(() => {});
                setCopied(true);
                setTimeout(() => setCopied(false), 1500);
              }}>{copied ? "Copied!" : "Copy"}</button>
            ) : null}
            <button className="btn-secondary modal-close" onClick={onClose}>&times;</button>
          </div>
        </div>
        <div className="modal-body">{children}</div>
      </div>
    </div>
  );
}
