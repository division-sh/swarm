# #2542 served scenario and publication-fault dependency

Track: FULL. Local checkpoint, not parent closure or merge approval.
Predecessor: 07e3cce858308112e45070ec35d5c73983c81460.
Governing approvals: issue comments6192298462 and6192732056. Catch-up method
approved in6192622179, with the immediate pinned-master start condition amended
by6193465504. No further migration wave starts before catch-up. The authoritative scenario-profile contract remains
platform-spec.yaml's run_scenario_execution_profiles section; no platform
semantics or schema changed in this wave.

## Boundary and owners

The concept is original selected-store construction/lifetime and exact physical
observation through served scenario execution, retained restart, publication
rollback and fault cleanup. The parent remains fixture-authority drift in
#2542/#2151. This closes this dependency, not the entire parent or all testsql
escapes. Generic testsql corruption/schema/count helpers remain counted.

- selected.OpenRuntime constructs a fresh PostgreSQL owner for each actual
  serve occurrence, retaining only its private DSN location across restart.
- The existing runtime-persistence capture rebinds the original reader each
  occurrence. No raw pool, reconstructed store or coordinator survives in these
  served helpers. The restart proof also rejects reuse of the previous reader.
- scenarioexecutionpersistence owns complete physical profile identity/bytes and
  global profile cardinality. Global run/event/entity counts reuse existing
  named ports and retain independent observation cuts.
- Existing delivery read_projections owns the exact global delivered-node count
  and ordered best-effort canonical-routing diagnostic. Generated Telegram
  readback composes existing exact event identity/run/payload readers in one
  original snapshot, with unchanged latest-by-created_at selection.
- Delivery owns the materialization cut and exactly two fixed private delivery
  existence witnesses. Pipeline persistence owns replay-scope fault DDL;
  API-idempotency owns event.publish completion fault DDL. Witnesses accept only
  native dialect, never caller SQL/identifiers/predicates. They cannot leave
  private store owners. The selected-writer receipt prearms cleanup before an
  install attempt and retains it across uncertain COMMIT acknowledgment.

All six old dialect installers and their private validator are deleted after
all seven fault calls migrate. The obsolete testsql delivery-SQL owner entry is
deleted, shrinking the guard's closed set. No guard allowance is added. The two
served rollback roots retain every public error/identity/residue/idempotency
assertion and now require the exact intended late-failure marker. Missing prior
facts cannot masquerade as the desired cut. The residue helper is unchanged.
Runtime consumer shutdown/join precedes fault removal while the enclosing
serve process still owns the open store; an unsuccessful shutdown cannot
authorize removal. The component template fixture registers cleanup before
constructing its bus/coordinator/manager so those consumers join first.

One derivative setup consumer, the acknowledgment-loss root, also required
propagation when the shared raw return disappeared. Its original delegated
commit-loss injection and exact stored-response replay assertions are unchanged;
its three global counts reuse existing owners. This accounts for the delta
from indicative117/77 to actual119/79, without adding another family.

## Execution and hostile proof

Receipts are under /home/youmew/.cache/swarm-2542-family145/.
GOTOOLCHAIN=go1.26.8 and GOMAXPROCS=2; focused roots only, no local tier.

| Manifestation | Disposition | Exact proof |
| --- | --- | --- |
| Derived/authored profile/restart raw lifetime | execution-proven through the same corrected path | scenario-profile-focused.log: both original roots, both stores, PASS7.303s; full run/profile/digest/byte equality and single-record assertions retained |
| Scaffold private-store close/removal | execution-proven through the same corrected path | scenario-scaffold-focused.log: both original archetypes PASS5.459s; a native profile read refuses after store close and filesystem removal remains asserted |
| Generated-input profile/readback | execution-proven through the same corrected path | scenario-generated-focused.log: both stores PASS1.836s, original payload/no-provider-call/settlement assertions |
| Canonical routing node counts | execution-proven through the same corrected path | scenario-canonical-routing.log: all six examples PASS5.105s |
| Three late publication cuts | execution-proven through the same corrected path | scenario-publication-faults.log: both original rollback roots and acknowledgment-loss sibling, both stores, PASS20.786s; exact intended markers and complete residue assertions |
| Component materialization rollback | execution-proven through the same corrected path | scenario-template-fault-race.log: original native template root PASS5.355s under race; complete prepared-tree snapshot equality retained |
| Uncertain install/restore cleanup | execution-proven through the same corrected path | scenario-fault-cleanup-race.log: all three cuts x both stores x committed/rolled-back install/restore, PASS15.166s; native driver COMMIT interception and physical trigger/function census, repeated cleanup stays inert |
| Exact profile/global count and invalid read scopes | execution-proven through the same corrected path | scenario-native-controls-final-race.log PASS16.129s; complete profile bytes/identities, foreign-run global count, absence/foreign/expired/canceled scope refusal |
| Missing prerequisites and foreign event/run/template facts | execution-proven through the same corrected path | same native-controls race receipt; actual trigger execution rejects unrelated native delivery and lifecycle facts, zero provisional event/API receipt |
| Closed/canceled/foreign fault ownership | execution-proven through the same corrected path | scenario-closed-fault-owner-race.log PASS8.822s; canceled removal retains responsibility, exact restoration precedes closed-owner refusal |
| Delivery witnesses weakened to unrelated existence | reproduced and fixed | scenario-hostile-api-red.log and scenario-hostile-replay-red.log: each deliberate source overlay fails both stores at the exact prerequisite assertion; unmodified native proof green |
| Wrong-template delivery/lifecycle witness | execution-proven through the same corrected path | scenario-wrong-template-race.log PASS49.189s: all three cuts on both stores refuse the wrong template with the exact prerequisite marker, no residue and no completed API receipt |
| Fixed-witness consumer escape | execution-proven through the same corrected path | scenario-closed-witness-consumers.log PASS2.834s: canonical source walk and AST enumeration find exactly one use of each expression, only in its designated private fault owner |

No timeout, workload, backend cell or assertion was weakened. The two hostile
overlays reside outside the checkout; they did not change the candidate tree.
The old aggregate history run timed out at240s and the combined metadata run at
120s; neither is a pass. Named source-fork history PASS72.818s, metadata
retirement root PASS107.439s, dependency root PASS12.482s, and finite/history
packet PASS96.389s provide separated receipts, without changing committed
budgets or rerunning a local tier.

## Preservation and accounting

The existing finite history owner captures22 exact signed declarations across
the complete four-file caller/retirement set. Eighteen recipes append and four
finite Successors advance; all22 transitions append. Historical Before/After,
order and unrelated rows remain immutable. Recipe inventory becomes1856 and
transition inventory1194. The complete source manifest SHA256 is
4040570dee0ab72b903aeebaeb4d10fee10b40f4428a5172765aaf3beba459c8.
Exact additive read-owner and three earlier census-source comparisons restore
their authenticated predecessors; hostile mutations fail. These are source
composition, not classification changes or accounting credit. The census's
collector remains b5bdd1d92df22693c570dab217a46c8a184f37085f496653d59b732c12399a4d.

The first ratchet refused the uncomposed older native/source fingerprints.
After exact authorized inverses, downward PASS109.012s and read-only
PASS107.901s:119 findings/79 raw removed, zero additions. Physical4042/2300 ->
3923/2221; excluded67 unchanged. Baseline SHA256:
f2e5f3f5a27bb736b2e69af48602b8e6584575589acfb00f95186b5ef492c9f8.
Existing holds194/155 remain uncredited, so proposed qualification-adjusted
remaining4236/2455 ->4117/2376. Acceptance is reviewer-b's decision.

Focused structural guards pass, including closed delivery SQL ownership,
event-admitted persistence and replay/delivery separation. Pinned U1000 v0.8.1
passed for affected packages (scenario-unused-pinned.log, exit0). The signed
b8427628ad complexity comparison against07e3cce858 passed with equal reviewed
policy: no hotspot-count increase; cognitive557/cyclomatic259 at least30,
maximum302/186 unchanged (scenario-complexity.log). Subsequent edits add only
test controls and this audit, not authored declarations. The two added test
files also pass the pinned cognitive check at the existing threshold.
New proof function complexity was reduced by helper extraction, not a waiver.
Unfiltered vet reports existing unkeyed-literal warnings in unchanged sourcefork,
scenario-setup and standing diagnostic tests. Vet with composites disabled
passes; this is not a claim of clean unfiltered whole-package vet.

Watchlist decision: refine the existing #2542 fixture-authority dependency and
receipt ledger; no new issue or architecture framework. Exact private trigger
witness composition is an accepted owner boundary, not a general SQL API.
No new hold is claimed or released. The194/155 prior holds and #2353 runner
cleanup qualification stay open. Integration/rebase, eventual exact-head
preflight and hosted qualification remain outstanding; no PR/push is requested
from this local checkpoint.
