#2735 conflict-only rebase onto delivered #2744.

Published original: 95464fccbd3d3efa4238b71325d3685f25c93644.
Withdrawn first rebase: 7dc9ac1eab1391d2371e4accb150ed635085d4d3.
Current master base: d282de4ddb5e690c60e30c0534c8db4c9e33fcf5.
New signed source: 378df08141bb97463c4bf88a47749ed3965071a2.
Source tree: 1440abaaa827ef1b95b67b1245407ff749f53c45.

Range-diff: 26 patches unchanged, 1 obsolete comparison-only patch dropped.
The sole conflict is embedded_spec_successors_1801_test.go. It is restored to
the exact delivered #2744 bytes, as explicitly required by the orchestrator.
All five files edited by #2744 are byte-identical to master, including the
deletion of the obsolete population test and its private comparison helpers.
N19's superseded patch only served that now-deleted root. Its helper references
were confined to that same file/root; no live consumer depends on it.
No runtime, memo, production semantics, budget or timeout changes.
No additional test deletion beyond preserving already-landed #2744.

11 focused surviving owner/negative-control roots pass, no skips, no failures.
Fresh exact-head preflight is running; older and incomplete receipts are not
claimed as completion. No PR push or hosted dispatch. Source is on the backup
ref agent-d/backup-2735-2744-rebase-20261019; previous signed heads are preserved.

Original hosted RED remains a known pre-existing #2701 recovery-scope
manifestation, with #2735 contribution UNCLASSIFIED. No diagnostic run, product
repair or retry-to-green was made. One normal qualification follows reviewer-e
LIGHT confirmation and a passing exact-head preflight. Recurrence of the exact
#2701 signature routes there, with no further run.
