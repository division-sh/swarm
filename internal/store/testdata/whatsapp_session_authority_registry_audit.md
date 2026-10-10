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

## Native Publication Commit Checkpoint

Eleven new SQL-bearing findings are `private-backend`. The channel onboarding
preflight consumes its existing operation/activation codecs, the operator
identity binding codec and the principal lock in the same selected inbound
transaction. It shares semantic predicates with fresh responsibility admission;
it neither re-enters a root-store reader nor performs provider I/O under SQL
ownership. Both event backends invoke that preflight before standing admission
and publication writes. The three SQL transaction parameters remain inside the
backend. No transaction is returned, retained or committed by these helpers.

The two inbound lifetime callback findings are `typed-process-local`: the
`WithNativeLifetime` cleanup result stops a context callback and cancels its
derived context; the private `nativeCommitContext.cancel` field is that context
cancel function. Neither callback receives SQL, selects a store or authorizes
an effect. The context's synchronous `Err` projection preserves native
lifetime checks at the existing transaction owner's COMMIT-admission boundary,
including when that owner retains a cancellation-independent SQL context.

The private native fact on prepared bus commands cannot be constructed through
the facade. Generic publication refuses it; inbound validation requires the
matching sealed input. The selected transaction owns the full commit, and
runtime dispatch consumes only its acknowledged evidence. These are typed
transfers, not new raw SQL permissions. Classification changes only these
thirteen exact findings; no scanner, disposition or fixture allowance changes.
The owner-local extraction composes native admission inside each existing
standing-target preflight. Four changed call fingerprints retain
`private-backend`: the two native calls move from the commit callbacks into
those preflights, and the two standing calls now transfer the complete typed
command instead of only its request. Regeneration removes the obsolete four
fingerprints. The same transaction, checks and lock order are preserved.

## Rebased Native Activity And Session Preparation Inventory

Regeneration after the master rebase exposed 34 unclassified fingerprints in
17 declarations. They include previously omitted preparation/standing/activity
sites, not just fingerprints changed by the rebase. Each exact finding is now
assigned to an existing owner; no collector, policy or debt allowance changed.

Nine findings are `typed-process-local`: `requireNativePublicationAdmission`,
`nativeChannelActivityLaunch`, `withNativeChannelActivityLaunch`,
`ConsumeNativeChannelActivity`, `prepareSessionActivityIntents`, and the two
coordinator declarations. The callbacks carry currentness/cleanup or select the
original typed execution owner. The private context key carries a single-use
launch tied to the newly started attempt, source, activation, tool and input.
Neither exposes SQL or a caller-constructible grant. The session activity
projection and native activity handoff tests exercise rejection and release.

Eight findings in `sessionStandingBindingCurrent` and
`sessionStandingActivationCurrent` are `private-backend`. They consume the
existing selected transaction, principal/binding locks and exact onboarding
and activation records. Confirmed responsibility retains standing service
identity while disconnected; it does not grant execution. The standing
responsibility tests cover missing, foreign, retired and canceled evidence.
The two facade methods are `typed-public-facade`: typed operation in, boolean
currentness out, with no transaction/callback/raw store authority.

Ten prekey fixture findings are `private-backend`: `SDKPreKeyInventory`,
`SetSDKPreKeyInsertFailure` and `SetSDKPreKeyCommitFailure`. These named private
fixture operations observe one account or install the exact bounded batch and
COMMIT failures; they do not expose generic SQL to callers. Four findings in
`sdkTransaction` and `DoDecryptionTxn` are also `private-backend`. The private
context scope serializes the original account's SDK transaction across loaded
handles and rejects foreign/expired nesting. Prekey preparation tests prove
inventory identity, batch and COMMIT rollback, cancellation, lock order, close
and process-death durability.

The changed `ObserveIntent` QueryContext fingerprint remains `private-backend`:
the existing read-only intent observation owner consumes typed query filters.
Served native Inbox, authored reply and process-death tests use this owner;
no selected-store handle or SQL is returned to the test caller.
