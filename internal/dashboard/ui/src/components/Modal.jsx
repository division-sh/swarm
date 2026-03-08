import * as Dialog from "@radix-ui/react-dialog";
import React, { useEffect, useId, useState } from "react";

export default function Modal({ title, onClose, copyText, children, className = "" }) {
  const [copied, setCopied] = useState(false);
  const titleID = useId();

  useEffect(() => {
    if (!copied) return undefined;
    const timer = window.setTimeout(() => setCopied(false), 1500);
    return () => window.clearTimeout(timer);
  }, [copied]);

  return (
    <Dialog.Root open onOpenChange={(open) => { if (!open) onClose(); }}>
      <Dialog.Portal>
        <Dialog.Overlay className="modal-overlay" />
        <Dialog.Content
          className={`modal-container ${className}`.trim()}
          aria-labelledby={titleID}
          aria-describedby={undefined}
        >
          <div className="modal-header">
            <Dialog.Title id={titleID} className="modal-title">{title}</Dialog.Title>
            <div className="stack">
              {copyText ? (
                <button className="btn-secondary" onClick={() => {
                  navigator.clipboard.writeText(copyText).catch(() => {});
                  setCopied(true);
                }}>{copied ? "Copied!" : "Copy"}</button>
              ) : null}
              <Dialog.Close asChild>
                <button className="btn-secondary modal-close" aria-label="Close dialog" autoFocus>&times;</button>
              </Dialog.Close>
            </div>
          </div>
          <div className="modal-body">{children}</div>
        </Dialog.Content>
      </Dialog.Portal>
    </Dialog.Root>
  );
}
