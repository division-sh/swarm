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
the full original source census. Publication is atomic and optional. A successful
head census may populate a future immutable-base entry only if the committed head
is clean and unchanged before and after scanning. It is never used to skip the
current head scan. Cache records live outside the checkout under the user cache.

As with shared Go build caches, cache producers/storage must be trusted. The
checksum detects corruption; it is not a cryptographic attestation from Git.
Untrusted PRs must not publish shared base-cache records.

## Collector Identity

The orchestration itself is fingerprinted, so the implementation cannot honestly
keep the old collector fingerprint. The prototype uses the existing exact,
metadata-only transition pattern from b42ea974646e7b666459091174645baa501d91da16871813568db23858def7b7
to f2ecc012b58131daa84827eae558631ff96247d556c1b328b3db4c1172734f6f.
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
resurrection detection, dirty/changing-head publication refusal and the exact
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
