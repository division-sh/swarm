# Issue #2630: Coding-Base Combination Control

2026-10-12, branch agent-g/2630-additional-value-assignability,
base 5182245eda6305028e71dc8ad0a2500eb42e50a7. Production and spec unchanged.
The relation and direct consumer files match the approved audit base770b99f93.
Gate6122667610 approved repair-first; #2447 extraction remains frozen.

## Approved-Class Baseline

New test scaffolding covers admitted enum, oneOf and equality, combinations,
omitted/true/schema-any policies, root/object/array/additional placements,
all three compiled-result producers, positive controls and error ordering.
The original three restrictions x omitted/true x four placements, and each
result producer, reproduce incorrect acceptance three times with race detection:

```text
go test ./internal/runtime/contracts \
  -run '^TestSchemaRelation2630(AdditionalValues|CompiledResultEntrances)$/^(enum|oneof|equality)$/^(omitted|true)$' \
  -race -count=3 -timeout=3m -json
```

Receipt2630-coding-base-exact-red3.jsonl SHA256:
a396633b0d014e94bf61ce17680e8df62f8cb5f0ace01dd62275332b4e455378.
24 relation leaves and18 producer leaves each RED3 at expected-refusal checks,
after source-valid/target-invalid witness controls. This is baseline evidence,
not qualification. The initial wider aggregate failed at an unexpected positive
constructor control and is retained as failed diagnostic evidence only.

## Additional Positive-Control Limitation

R10's intended identical declared-source control also reveals this existing
limitation, distinct from the incorrect unrestricted-source acceptance:

```json
{
  "type": "object",
  "properties": {
    "mode": {
      "type": "any",
      "oneOf": [{"type":"string"},{"type":"integer"}],
      "x-swarm-equalTo": "peer"
    },
    "peer": {"type":"any"}
  }
}
```

Public AdmitToolInputSchemaMap admits the whole schema. It validates
mode=approved, peer=approved, and whole.Equal(whole) is true. However, proving
whole.ValidateAssignableTo(whole) returns:

```text
matching.mode oneOf schemas are not provably assignable unless identical
```

The same occurs with an additional enum(approved), three times each under race.
The private union decision calls source.Equal(target) on extracted properties;
property.Equal(property) is false because standalone property.Project rejects
x-swarm-equalTo outside a declared object-property context. The property is
valid in its admitted containing object, but standalone projection loses that
context. No new schema restriction or second implication interpreter found.

```text
go test ./internal/runtime/contracts \
  -run '^TestSchemaRelation2630MatchingUnionEqualityControl$' \
  -race -count=3 -timeout=3m -json
```

Receipt2630-matching-combination-red.jsonl SHA256:
12fe14c7bea91defab3c6db40697be5bb40e53431e76472485a4ff41aa1509a2.
Both leaves RED3. No race report, provider, network or database execution.
Receipt directory:
/home/youmew/.local/state/agent-g-2509-postmerge-preaudit-20261012/.

## Required Classification Before Production Edits

This is a refusal/comparison-context limitation, not the approved acceptance
bug. The finite owner deliberately rejects unproved unions, so do not infer
permission to broaden union proof from the example. R17's unrestricted-source
refusal cases still belong to #2630; making matching union/equality producers
accepted would change another decision, beyond the approved single branch.

Request reviewer-g's bounded disposition:
- Keep the limitation separate on existing #2349/watchlist, preserve existing
  conservative refusal, and use a closed source with no constrained property as
  the genuinely admitted positive producer control for these combinations; or
- Widen the explicit repair gate to the context-preserving identity comparison
  and its consumers/proofs before implementing that additional semantic change.

No production fix, caller exception, new comparison framework or extraction
has been made. The new maintained-test draft remains RED; it is not a ready PR
or passing characterization. Do not push qualification or run a whole tier until
the positive-control and scope decision is explicitly recorded.
