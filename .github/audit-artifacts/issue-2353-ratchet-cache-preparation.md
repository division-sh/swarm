# Persistence Authority Ratchet Cache Preparation

Review-branch prototype only. No PR, merge, hosted qualification or timing-budget
increase is authorized by this artifact. Starting source is master
395fd6ade8acbab4d07f338d0dda3d09caf58609.

## Preserved Meaning

The trusted base still comes from `debtTrustedBase`, and is materialized from
that exact Git commit. No caller-selected baseline is introduced. The current
checkout, including modified and untracked owned source, is always scanned fresh.
All active/test variants, inactive-source treatment, selected-boundary findings,
site identities and multiplicities remain under the existing collectors.

`authorityDebtRatchet` and its three comparisons are unchanged: actual against
head baseline, head baseline against trusted baseline, and actual against the
trusted source to prevent resurrection. Bootstrap lineage, refresh permissions,
baseline parsing, source scope, diagnostic formatting and final count output are
unchanged. No baseline debt row is added, deleted or reclassified.

## Disposable Base Analysis

The cache key binds the full base commit SHA, the current collector/projection/
finding/serialization owners and checkout walker, module versions, and the Go
runtime/toolchain/build context (including type-checker GODEBUG settings).
Records contain the exact aggregated census, not allowed-debt permission data.
The existing strict baseline codec preserves sorted identities and multiplicity;
the outer record validates its schema, identity, checksum and canonical encoding.

A miss, invalid record, inaccessible cache or local module replacement recomputes
the full original source census. Publication is atomic and optional, and only
censuses of the materialized immutable Git archive populate the cache. Live head
is never published as an immutable source: Git can report clean while ignored Go
source remains live. Cache records live outside the checkout under the user cache.

As with shared Go build caches, cache producers/storage must be trusted. The
checksum detects corruption; it is not a cryptographic attestation from Git.
Untrusted PRs must not publish shared base-cache records.

## Collector Identity

The orchestration itself is fingerprinted, so the implementation cannot honestly
keep the old collector fingerprint. The prototype uses the existing exact,
metadata-only transition pattern from b42ea974646e7b666459091174645baa501d91da16871813568db23858def7b7
to 9838ebeda35431046f2c855e7b941997a846b1abc7ccbcf6a1f1fd7243c3e669.
Both source/destination digests, bootstrap identity and every debt row must match
exactly. Reverse/future/foreign transitions and row/multiplicity changes refuse.
The new cache implementation is included in the permission fingerprint; only the
destination constant is outside it to avoid self-reference. This transition is
prepared for review, not already approved. The collector value in the existing
count log changes accordingly; its format and all counts retain their meaning.

## Proof And Measurement Plan

Focused race/count-three controls cover round-trip identity/multiplicity, returned
map isolation, source/analyzer/toolchain invalidation, corrupted/duplicate/unknown
records, unavailable storage, local replacements, fresh untracked head authority,
resurrection detection, ignored-live-source isolation and the exact
metadata transition. Vet, the touched-package census/registry/guard sweep and the
complete core structural-owner selection are run before server2 measurement.

On an explicitly handed-over server2 slot, compare unchanged master with the
candidate cold cache and the same candidate warm cache, using the original
`TestPersistenceAuthorityDebtRatchet` selector, count=1 and ten-minute test timeout.
Keep Go build/module caches shared and isolate only the analysis-cache directory.
Preserve raw test JSON and `/usr/bin/time -v` wall/CPU/RSS receipts. Verify that
cold and warm ratchet count/diagnostic output is identical, and that the warmed
base-cache record is reused rather than rewritten. Timings are reported separately
at the measured SHA, not invented here.

## Hosted Integration Still Required

No CI workflow is changed by this prototype. An entirely cold hosted runner still
does both censuses. A warm-base result is not evidence that first-use GitHub CI is
fixed. Shared reuse needs exact-key restore and a protected-master producer; PR
jobs must be read-only consumers. Existing master proof replay can skip the scan,
so merely adding a directory to an actions cache does not guarantee that the next
base has a populated record. Reviewer/user disposition must settle that publication
path before this is proposed as a complete CI fix. No second validator, discarded
assertion, wider timeout, vendored package or permission-baseline inflation is
part of the preparation.

## Server2 Diagnostic Measurements

Measured prototype 44c6fff7a971695b125ca1fa8c719da5839d6c7d on the explicitly
handed-over server2 window, 2026-10-08 15:18:52-15:21:10 UTC. Server2 was released
immediately after completion. Go 1.25.5 linux/amd64, 16 logical CPUs; shared Go
build/module caches retained. Only the analysis cache used a new isolated XDG
directory. Original ten-minute test timeout, selector, assertions and source
selection remained unchanged. All three commands exited zero.

| Case | Ratchet body | Whole package | Command wall | User/system CPU | Peak RSS KiB |
| --- | --- | --- | --- | --- | --- |
| Unchanged master 395fd6ade | 23.01s | 37.831s | 53.58s | 185.34/19.46s | 4,390,364 |
| Prototype, cold analysis cache | 23.91s | 38.636s | 56.54s | 193.83/19.63s | 4,437,648 |
| Same prototype, warm base cache | 11.90s | 26.337s | 28.27s | 73.36/10.09s | 4,425,792 |

The command was `go test -run TestPersistenceAuthorityDebtRatchet -count=1 -json
./internal/store`, measured with `/usr/bin/time -v`. Its unanchored selector also
runs the existing rejection controls and `TestPersistenceAuthorityDebtRatchetIsSelectedInEveryTier`;
the latter takes about fourteen seconds and is not omitted or credited as part
of the ratchet body's speedup. Command-wall differences also include build-cache
warming; prefer the actual ratchet body comparison for the estimated benefit.

The warm base entry was 6,762,289 bytes; its size/mtime did not change across the
warm run. Cold and warm debt count output matched byte-for-byte: 55,398 findings,
39,817 raw-operation sites, 14,592 actual debt, 14,850 inherited baseline,
10,811 confirmed raw-operation debt, 67 excluded-source occurrences. Master has
the same counts but naturally uses its predecessor 236391687 instead of 395fd6ade,
and the old collector fingerprint; no fabricated event/base override was used.

This is one diagnostic triplet, not hosted qualification. Warm ratchet-body time
fell about 50%, but cold time did not improve and peak RSS did not materially
decrease. If similar ratios hold for the reported 480-690s hosted body, a valid
warm base entry could bring it to roughly 240-345s; that estimate is not a measured
2-vCPU result or a ten-minute-timeout guarantee.

After measurement the optional live-head publication shortcut was removed:
Git-clean source may still include ignored Go inputs. The current prototype
only caches immutable archives, and has an ignored-source regression. The above
timings remain explicitly attributed to 44c6fff7a, not relabeled as measurements
of the subsequent safety correction; the base-hit path is unchanged.

Original artifacts: server2 `/home/youmew/.local/state/agent-d-ratchet-cache-44c6-benchmark`.
Small local copy: `/home/youmew/.local/state/agent-d-2566/ci-37736838792/ratchet-cache-server2-44c6`.
Raw test JSON SHA256s:

- before: e692e9f64d428ecf399ac1061f306724ed5fe2236f127576969752b9d2fe5555
- cold: f7ff40c92b8611b8bbdbe7bfbe95518d017f40e8a6bd22637f1ee6952cf64c3e
- warm: d1aa6d6bd38683564772b07957754c81303a499ac1f29d95cbae6f1f53f52553

## Single-Job Benefit And Minimal Hosted Wiring

This persistent base cache gives no initial cold single-job speedup: the ratchet
already computes its base exactly once. The actual hosted unit separately runs
`TestNativeFixtureFamiliesDoNotReceiveRawAuthority`, which performs another head
census. Sharing one complete, freshness-proven head census between those two
consumers could remove that duplicate within one process (three loads to two),
without CI configuration. It is not implemented or measured by this prototype;
it must cover dirty, untracked and ignored source, build context and result
isolation rather than caching only by HEAD.

Minimal cross-run wiring requires an exact-key restore of the base entry before
the proof unit, plus protected-master computation/publication of an immutable HEAD
archive entry for subsequent PR bases. The producer uses the existing collector,
not another validator. PRs never save shared entries; key/version mismatch always
falls back to the complete scan. A publisher must also run when master proof
replay skips the normal census. No such workflow/publisher is added here; this
remains a user-owned CI configuration decision, with one fresh head census worth
of producer work per otherwise-unpopulated base.
