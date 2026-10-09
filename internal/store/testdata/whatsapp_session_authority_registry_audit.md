# WhatsApp Session Authority Classification (#2577)

The two changed `channelonboarding/owner.go` ExecContext fingerprints remain
`private-backend`: the existing operation update and activation publication
now also write the exact private session-account provenance. Their selected
mutation owners, revision fences and principal-before-activation locks are
unchanged. The registry records the extra nullable argument, not another writer.

The six `session_state_unix.go` findings are `private-domain-adapter`: the
unexported database field and local variable, SQLite open, single-connection
limit, SDK-container construction, and database close. This owner opens only
the restrictive, exclusively possessed connection directory's `provider.db`.
It does not obtain a selected runtime database or execute business SQL. Close
joins the SDK occurrence and private-store work before releasing possession;
failed joining retains it. The existing concrete state and store-fence tests
prove these boundaries. The CLI raw-SQL ledger independently names this exact
provider-private-state file and reason.

No raw authority disposition or scanner policy was added or widened. Only
these exact reviewed findings are classified before registry regeneration.

## Native Claim And Recovery Checkpoint

The four new selected-store effective methods are `typed-public-facade`:
`SettleSessionChannelClaim` accepts only an opaque native-issued claim, and
`LoadOperatorChannelClaimReceipt` returns detached immutable receipt data.
Neither accepts SQL, a transaction callback, a caller text grant or a table
selector. Both stores delegate to their existing operator-channel backend owner.

The eighteen new/changed backend findings remain `private-backend`. The extra
claim receipt argument records original authorization. `session_claim.go`
performs the existing claim settlement in the selected owned transaction,
checking native lifetime and the exact original parent revision, and reads
receipt identity/authorization/fingerprint through the backend's existing
query and timestamp codecs. Begin and confirmation invoke the same private
parent-revision fence. The SQL-bearing helper parameter never escapes the
backend; no root-store currentness reader is called under its transaction.
The extracted claim parent preflight retains the same checks and lock order;
its private transaction parameter and call replace the larger inline body.

The six `claim_recovery.go` findings are `private-domain-adapter`, like the
existing private capture/publication retirement owner. Its transaction reads
validated pending rows and deletes only the matched connection's local spool
row after exact selected-store receipt/fingerprint reconciliation. It has no
selected runtime SQL or new execution authority. Missing/conflicting history
retains the capture; actual native retirement/reopen proofs cover the path.

These are exact additions to the existing owner inventory, not a wildcard
package allowance, new disposition, scanner-policy change or fixture waiver.
