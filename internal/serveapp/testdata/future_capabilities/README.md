# Receiver Post-Revision Future Capability

Completed ordinary arrival-join history is a separate #642 capability. Its exact
G19 success oracle and reproduction instructions are documented in
[fork_retained_join_README.md](fork_retained_join_README.md).

Binding disposition: https://github.com/division-sh/swarm/issues/2167#issuecomment-5588834719 D3.

These are explicitly unsupported future success oracles, not passing tests,
skips, or expected-failure wrappers. No post-revision scope capability is approved.
The original finished emission occurs after the selected producer frontier and
the current owning store rejects that history. Never change R or weaken the guard.

`fork_receiver_post_revision_success_test.go.txt` is the complete pre-D3 ownership
test source, preserving all original success/ownership/effect/isolation assertions.
`fork_receiver_geometry_future_test.go.txt` is a self-contained executable clone
of that full file. Only package-level function/type identifiers and their Go
identifier references are renamed to avoid collisions. Function bodies, literal
values, assertions and original helper behavior are otherwise retained. It does
NOT invoke the changed effect-only journey or the extracted delivery helper.

Original source snapshot SHA256 before EOF whitespace normalization:
`7b44e7c90a992d4856b9cb0a1eb5c390cfeed873f50c638b337e1af82179b51f`.
Committed snapshot SHA256 (only the extra trailing blank line removed):
`ce1f31892de9c30f4082936141e88b2b0ea77920525769ed4d9e0c2b9424a21b`.
Self-contained clone SHA256:
`f7fe3f71c28d6cf760275808fefc3fb5f85730c902021c5fabc12021a1e69ced`.

To reproduce, use a Go overlay mapping a new virtual file
`internal/serveapp/fork_receiver_geometry_future_test.go` to the absolute path of
`fork_receiver_geometry_future_test.go.txt`. Run the desired
`^TestFuturePostRevisionOriginal...$` explicitly with `go test -overlay`; broad future
matrices still require the capacity runner. Expected current result is RED, not
coverage credit. No current served test file must be replaced or blanked.

Verified diagnostic overlay: `/tmp/2167-D3-geometry-future-overlay.json` maps:

- New virtual `/home/youmew/dev/swarm/worktrees/agent-a-2167-fork/internal/serveapp/fork_receiver_geometry_future_test.go`
  to the self-contained clone above in this worktree.
- `/home/youmew/dev/swarm/worktrees/agent-a-2167-fork/internal/runtime/runforkexecution/runtime_container.go`
  to `/tmp/2167-D3-future-owner/runtime_container.go`, the current exact source
  plus one diagnostic print immediately before returning the existing store
  guard error. No control flow or behavior is changed; no production file edited.
  The second mapping may be omitted to reproduce the original assertion alone.

Exact executed command (from the worktree):

```sh
go test -overlay /tmp/2167-D3-geometry-future-overlay.json ./internal/serveapp -run '^TestFuturePostRevisionOriginalSelectedForkOptionalReceiverOwnershipBothStores$/^default_sqlite$/^entityless$' -count=1 -timeout=90s -v
```

Receipt `/tmp/2167-D3-geometry-future-selfcontained.log`: RED1.430s, ordinary
source succeeds, then exact `source_committed_replay_scope_advanced_after_fork_point`
at container_publish_pre_source_load. The original HTTP success assertion fails.
This is one SQLite cell, not a full future matrix or a passing refusal test.

Active geometry counterparts are explicitly named `...NoticeEffectBothStores`:
they preserve optional/required/acquiring policies, sibling/permutation/repeated
source geometry and every final delivery/ownership/isolation/retry check while
asserting authored mailbox business effects instead of a later finished event.
`TestSelectedForkReceiverPostRevisionPolicyRefusalBothStores` owns current-policy
exact refusal and cleanup proof. Report its executed geometries independently;
a required-existing refusal alone is not the full paired geometry matrix.
