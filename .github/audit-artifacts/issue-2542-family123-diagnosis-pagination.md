# Cohort 123: exact delivery diagnosis pagination fixture

Binding: approved #2542/#2151 native selected construction and lifecycle recipes.
Retire the entire raw DB/dialect callback from this two-backend fixture. Existing
RequireRun replaces the two reconstructed run recipes, preserving scenario_setup,
the original run ID and started time. The consumer explicitly requires the existing
RunFixtureStore port; no new delegate, raw getter, query or persistence owner.

Both original routes remain, with different exact entity/reply identities. The
unchanged public diagnosis proof derives and sorts both delivery IDs, checks total
pending cardinality two, page size one, exact first/second identity, nonempty first
cursor and empty final cursor. Both native stores run under race.

The finite whole-function rewrite permits only construction/setup cuts. Its source
and hostile identity/page/cursor/assertion controls reject residual raw authority
and proof weakening. Other diagnosis fixtures remain parent debt. No measured
flake reduction, production semantic change or final closure is claimed.
