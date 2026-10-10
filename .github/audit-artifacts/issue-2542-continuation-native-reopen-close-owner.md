# Native Reopen Fixture Close Ownership

Approved #2542 shared-fixture cleanup and joined-teardown territory. The
existing PostgreSQL and SQLite location-based reopen fixtures registered an
independent cleanup for each later peer. A consumer registered after initial
construction therefore lost the late peer before its own teardown ran.
This is fixture resource ordering, not a production lifecycle redesign.

Red-first race probe on both backends deterministically observed database-is-
closed from the consumer cleanup. The same test now proves both distinct
native owners remain readable through consumer teardown and both lose read
authority after the fixture ends. No sleep, retry, timeout increase or fake
grant is introduced.

The permanent protocol test belongs to the existing construction role: it
tests original-location construction, late-peer resource lifetime and final
closure, not arbitrary business fixture setup. Its initial placement in
storetest was rejected by the unchanged census as two new anonymous native
constructor sites. No collector, baseline permission or filename exemption
was added to admit that placement; the canonical construction-role policy
is unchanged.

The two existing native constructors now register their resource close owner
once, before returning the first handle. Later location-based handles join
that same fixture-owned close set; its cleanup closes peers in reverse order.
Creation, canonical schema bootstrap, native coordinator, payload admission,
original location and reopen function are preserved. The mutex protects the
close set; it is not a second runtime writer coordinator.

Existing callers automatically consume the corrected owner, including timer
restart and activity terminal replay. Focused both-backend location, cleanup
and restart proofs cover this consumption. Two finite recipes independently
preserve every non-cleanup statement; hostile controls reject loss of native
admission, wrong close targets and substituted reopeners. Global guards,
census/registry, unused and complexity remain required before the increment
push. Receipts: /home/youmew/.cache/swarm-2542-local-20261008/reopen-cleanup-*.

Catalog pool reconstruction is the next caller propagation, not claimed
closed here. Remaining raw reads, runtime dependency assembly, parent debt,
zero-debt guards, fork deadlines and final integrated qualification remain
open under #2542. No runtime spec change, new issue, framework or compatibility
permission is needed; existing watchlist mapping suffices.
