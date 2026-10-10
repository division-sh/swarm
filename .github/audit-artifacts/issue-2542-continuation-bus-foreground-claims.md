# Cohort 84: foreground claim and sibling replay fixture

The two native backend branches return only the existing event-store and
lifecycle/completion roles. There is no raw pool, dialect placeholder, generic
getter or reconstructed PostgreSQL coordinator. Both EventBus actors consume
the exact original selected store while keeping independent foreground and
sibling execution paths. The same run origin, default source and IDs are
materialized through the original lifecycle fixture.

The finite whole-root codemod preserves acknowledged foreground return before
release, sibling zero-settlement during the claim, and sibling zero-settlement
after foreground completion. Every event field, signal, limit and normal-path
timeout remains unchanged. Safe release-once cleanup joins both the publishing
worker and actual foreground bus work before native fixture close, including
assertion failures. No delegation hides a coordinator escape.

The unchanged real root runs under race on both stores. Existing broad approval
and raw_sql_policy cover this routine original-owner repair. No production
behavior, spec, new framework or tracking decision changes. Other publication
fixture siblings remain individually counted until migrated.
