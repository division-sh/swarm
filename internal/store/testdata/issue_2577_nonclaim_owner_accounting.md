# #2577 Non-Claim Capture Owner Accounting

The existing private `sessionpersistence.CaptureStore` owns non-executable setup
disposition persistence. No SDK/serve/runtime consumer receives SQL authority.

Registry delta: 17 exact new signatures replace seven retired signatures (net
ten), all in `capture.go` or `nonclaim.go` under the existing private-backend
disposition. These are findings, not a new allowance or collector exception.

- Six signatures belong to the renamed complete-row reader: its local Rows and
  QueryContext, Scan, Next, Err, Close. Scan additionally reads the two disposition
  columns and validates their digest before pending/duplicate/quota filtering.
- One is Capture's call to that reader instead of the old pending-only reader.
- Three are the constructor's schema probe: query, local Rows, complete close.
  Unsupported old capture layouts fail without ALTER, copying or compatibility.
- Six belong to SettleNonClaim: local Tx, BeginTx, readAllRows, guarded UPDATE,
  Commit and deferred Rollback. Only the private compiled projection's opaque
  non-claim fact can supply disposition; original capture and responsibility
  checks precede commit. The normal claim transaction remains separate.
- One is NonClaimReceipts' validated read-only call to the complete-row reader.

Consumers are Capture duplicate/quota admission, Pending, PendingPublications,
normal business/claim retirement, incoming reconciliation and non-claim readback.
They validate every row, including history, through the same complete reader.
Disposed evidence is retained, never arbitrarily deleted or turned into business
authority. Named existing fixture operations cover rollback and corruption.

The native issuer census gains only a read-only alias for the closed NonClaim
type; it now checks actual TypeSpec aliases rather than allowing any use of a
type name. New factory/raw-value counterexamples must remain rejected. No native
constructor is exported, no foreign issuer path is admitted, and no raw carrier,
blanket exemption, threshold or debt policy is changed.
