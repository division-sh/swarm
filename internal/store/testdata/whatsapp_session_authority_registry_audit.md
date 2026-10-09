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
