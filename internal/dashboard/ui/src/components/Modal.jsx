import React, { useEffect, useId, useRef, useState } from "react";

export default function Modal({ title, onClose, copyText, children, className = "" }) {
  const [copied, setCopied] = useState(false);
  const titleID = useId();
  const containerRef = useRef(null);
  const closeRef = useRef(null);

  useEffect(() => {
    const previousOverflow = document.body.style.overflow;
    const previousActive = document.activeElement;
    document.body.style.overflow = "hidden";
    window.requestAnimationFrame(() => {
      (closeRef.current || containerRef.current)?.focus();
    });

    function trapTab(event) {
      if (event.key !== "Tab" || !containerRef.current) return;
      const nodes = Array.from(containerRef.current.querySelectorAll(
        'button, [href], input, select, textarea, [tabindex]:not([tabindex="-1"])',
      )).filter((node) => !node.hasAttribute("disabled"));
      if (nodes.length === 0) return;
      const first = nodes[0];
      const last = nodes[nodes.length - 1];
      if (event.shiftKey && document.activeElement === first) {
        event.preventDefault();
        last.focus();
      } else if (!event.shiftKey && document.activeElement === last) {
        event.preventDefault();
        first.focus();
      }
    }

    function onKey(e) {
      if (e.key === "Escape") onClose();
      trapTab(e);
    }
    window.addEventListener("keydown", onKey);
    return () => {
      document.body.style.overflow = previousOverflow;
      if (previousActive && typeof previousActive.focus === "function") {
        previousActive.focus();
      }
      window.removeEventListener("keydown", onKey);
    };
  }, [onClose]);

  return (
    <div className="modal-overlay" onClick={onClose}>
      <div
        ref={containerRef}
        className={`modal-container ${className}`.trim()}
        onClick={(e) => e.stopPropagation()}
        role="dialog"
        aria-modal="true"
        aria-labelledby={titleID}
        tabIndex={-1}
      >
        <div className="modal-header">
          <div id={titleID} className="modal-title">{title}</div>
          <div className="stack">
            {copyText ? (
              <button className="btn-secondary" onClick={() => {
                navigator.clipboard.writeText(copyText).catch(() => {});
                setCopied(true);
                setTimeout(() => setCopied(false), 1500);
              }}>{copied ? "Copied!" : "Copy"}</button>
            ) : null}
            <button ref={closeRef} className="btn-secondary modal-close" onClick={onClose} aria-label="Close dialog">&times;</button>
          </div>
        </div>
        <div className="modal-body">{children}</div>
      </div>
    </div>
  );
}
