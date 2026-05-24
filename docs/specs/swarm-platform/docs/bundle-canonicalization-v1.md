# Bundle Canonicalization v1

Status: source-authority design for #979. This document defines the
canonical bundle hash rule that later platform-spec, OpenRPC, storage, and
runtime implementation work must consume. It does not by itself change
runtime behavior. Promotion into `platform-spec.yaml` remains owned by #1012.

## Purpose

Multi-bundle persistence needs one stable identity for the loaded bundle. The
identity must be content-addressed, reproducible across operating systems, and
safe to persist independently of the original contracts directory.

The canonical name for this identity is `bundle_hash`.

Existing `bundle_fingerprint` and `bundle_ref.fingerprint` surfaces are legacy
aliases during #1001 migration. They are not a second semantic owner.

## Hash Format

The v1 hash string is:

```text
bundle-v1:sha256:<64 lowercase hex characters>
```

The digest is SHA-256 over the v1 preimage stream defined below.

Legacy values in the older format `sha256:<64 lowercase hex characters>` are
not v1 bundle hashes. They can be accepted only by explicitly gated migration
or compatibility entry points during #1001. New persisted bundles and new
multi-bundle lifecycle rows must use the v1 format after the implementation
work consumes this document.

## Canonical Entries

The hash input is an ordered list of canonical entries. Each entry has:

- a canonical label, encoded as UTF-8
- canonical content bytes

The canonical label is independent of the local absolute filesystem path. The
label path separator is `/`. Labels must be valid UTF-8 and Unicode NFC. Labels
must not contain empty segments, `.`, `..`, NUL, or backslash. Labels are
compared by raw UTF-8 byte order, with no locale or Unicode collation.

If two discovered inputs produce the same canonical label, canonicalization
fails closed. Implementations must not use absolute path as a tie-breaker.

### Entry Labels

The effective platform spec is labeled:

```text
platform/platform-spec.yaml
```

Bundle-local entries are labeled:

```text
bundle/<relative path from contracts root>
```

The relative path is cleaned, slash-normalized, and must remain inside the
contracts root after symlink checks. The label preserves filename case. Case
collision detection is performed after slash normalization and NFC validation
by applying ASCII-only fold to the full label: bytes `0x41` through `0x5a`
are converted to `0x61` through `0x7a`; all other bytes are unchanged. If two
labels have the same ASCII-folded byte sequence but different raw label bytes,
canonicalization fails. Non-ASCII bytes are never locale-folded; host filesystem
case sensitivity and locale settings are not part of the rule.

## Canonical Byte Inputs

The input set is derived from the loaded contract tree after contract discovery
has succeeded. Missing optional files do not create entries. Missing required
contract files fail during contract load before canonicalization.

The effective platform spec and root `package.yaml` are required inputs. If
either cannot be read, canonicalization fails closed. A platform-spec content
change intentionally changes the bundle hash because the runtime contract
authority used to interpret the bundle changed.

Included inputs are:

| Input class | Entry policy | Content policy |
| --- | --- | --- |
| Effective platform spec | `platform/platform-spec.yaml` | YAML canonicalization |
| Root package manifest | `bundle/package.yaml` | YAML canonicalization |
| Root contract files | `schema.yaml`, `types.yaml`, `entities.yaml`, `nodes.yaml`, `events.yaml`, `agents.yaml`, `tools.yaml`, `policy.yaml` when present | YAML canonicalization |
| Root prompts tree | every non-ignored regular file under `prompts/` | prompt text canonicalization |
| Discovered project packages | each package manifest plus `nodes.yaml`, `events.yaml`, `agents.yaml`, `tools.yaml`, `policy.yaml` when present | YAML canonicalization |
| Package prompts trees | every non-ignored regular file under each package `prompts/` | prompt text canonicalization |
| Discovered flow contract files | each flow `schema.yaml`, `types.yaml`, `entities.yaml`, `nodes.yaml`, `events.yaml`, `agents.yaml`, `tools.yaml`, `policy.yaml` when present | YAML canonicalization |
| Flow prompts trees | every non-ignored regular file under each flow `prompts/` | prompt text canonicalization |
| Flow data trees | every non-ignored regular file under each flow `data/` | raw bytes |

The discovered project package tree is the same contract tree used by the
loader: root package references from `packages`, `children`, and `subpackages`,
plus flow-local `package.yaml` manifests discovered under declared flow
directories. Package and flow discovery errors fail before canonicalization.

Excluded inputs are:

- executable source code, Python modules, MCP binaries, scripts, and container
  images
- repository metadata such as `.git/`
- generated caches and editor/OS junk matched by the ignored-path rules below
- verification gates, tooling locks, DDL files, and agent config maps unless a
  later v2 canonicalization rule explicitly adds them
- root-level or package-level `data/` directories; v1 includes only per-flow
  `data/` directories because that is the current contract model's loaded data
  boundary

Adding a new input class changes hash semantics and requires a new hash version
or an explicit migration rule.

## Preimage Framing

The SHA-256 preimage starts with exactly these 21 bytes:

```text
73 77 61 72 6d 2d 62 75 6e 64 6c 65 2d 68 61 73 68 2d 76 31 0a
```

Those bytes are the ASCII string `swarm-bundle-hash-v1` followed by one LF
byte (`0x0a`). The preimage does not include a carriage return, a literal
backslash byte, a literal `n` byte, Markdown fence bytes, or an additional
blank line.

For each canonical entry sorted by canonical label, append:

1. unsigned 64-bit big-endian byte length of the label
2. label bytes
3. unsigned 64-bit big-endian byte length of the content
4. content bytes

The length framing is part of the rule. Implementations must not use
delimiter-only framing.

## YAML Canonicalization

YAML inputs are parsed into a JSON-compatible data model and emitted as
canonical JSON bytes.

The allowed data model is:

- object with string keys
- array
- string
- finite number
- boolean
- null

The YAML parser must fail closed for:

- duplicate mapping keys after string normalization
- non-string mapping keys
- custom or unknown tags
- non-finite numbers
- alias cycles
- multiple YAML documents in one file
- invalid UTF-8

Standard YAML tags are allowed only when they resolve into the allowed data
model. For example, `!!str` is allowed because it produces a string value.
Tags such as `!!binary` are not allowed in YAML contract files; binary content
belongs in `data/` and is hashed as raw bytes. Date/time tags are unsupported;
authors must encode timestamps as strings when they are contract data.

Anchors and aliases are resolved before JSON emission. Anchor names are not
content. Two YAML files that differ only by anchor names hash the same. A file
that inlines an aliased value hashes the same as the alias form only when the
resolved JSON data model is identical.

Comments, YAML document markers, indentation, line endings, and mapping key
order are presentation and are not content.

Canonical JSON emission follows RFC 8785 JSON Canonicalization Scheme (JCS)
serialization, with this v1 numeric profile:

- YAML numeric scalars must resolve to JSON numbers, not strings
- accepted numbers must be finite IEEE 754 binary64 values
- integer-valued numbers must be within the I-JSON safe integer range
  `-9007199254740991` through `9007199254740991`
- negative zero is rejected, including spellings such as `-0`, `-0.0`, and
  `-0e0`
- non-finite values, unsupported YAML numeric forms, and values outside this
  profile fail canonicalization before emission
- `1`, `1.0`, and `1e0` all emit as the JCS JSON number `1`

Implementations must parse numeric scalars with enough precision to enforce
this profile before converting to the JCS number model. Host-language default
JSON marshaling is acceptable only when it is proven to match this profile.

Other canonical JSON emission rules:

- UTF-8 output
- no insignificant whitespace
- object keys sorted by raw UTF-8 byte order
- strings escaped with JSON escape rules
- no trailing newline

## `$ref` Policy

`$ref` is hashed as literal YAML data. v1 does not resolve `$ref` targets before
hashing.

Consequences:

- a file with `$ref: ./schema.yaml` and a file with the referenced schema
  inlined do not hash the same unless their literal YAML data model is the
  same
- changing a referenced file changes the bundle hash only if that referenced
  file is also an included canonical input
- reference validity is owned by contract verification, not by bundle
  canonicalization

This policy keeps bundle identity tied to the authored contract source and
avoids hidden dependency traversal.

## Prompt Text Canonicalization

Prompt files are text inputs. They must be valid UTF-8.

Prompt canonicalization:

- strips a leading UTF-8 byte order mark when present
- converts CRLF and CR line endings to LF
- appends one final LF when the file is non-empty and does not already end in LF
- preserves all other bytes, including trailing spaces, tabs, and blank lines

Prompt files are never parsed as YAML for hashing, even when their extension is
`.yaml` or `.yml`.

## `data/` Rules

Files under included flow `data/` directories are data inputs. Their canonical
content is the exact raw byte sequence read from the file.

No text normalization, YAML parsing, BOM stripping, permission bits, ownership,
modification times, access times, or directory metadata are included in the
hash. Directory entries are not hash entries.

The persisted `data_blob`, when implemented, must be serialized from the same
ordered flow `data/` entry list. The archive container metadata must be
deterministic: sorted file order, normalized slash paths, no directory entries,
zero or fixed timestamps, fixed uid/gid, and fixed permission mode. If
compression is used, compression metadata and timestamps must also be
deterministic. The archive container bytes are not the hash input; the
canonical entry list is.

## Symlink and Ignored-File Policy

Canonicalization uses lstat-style discovery. Symlinks are not followed.

Rules:

- an explicit fixed input path that is a symlink fails canonicalization
- any non-ignored symlink under a recursive `prompts/` or `data/` walk fails
  canonicalization
- broken symlinks fail if encountered as non-ignored inputs
- directory symlinks are not traversed
- hard links are treated as regular files by content

Recursive walks ignore these directory names at any depth:

```text
.git
.hg
.svn
.idea
.vscode
__pycache__
.pytest_cache
.mypy_cache
.ruff_cache
```

Recursive walks ignore these file names or suffixes at any depth:

```text
.DS_Store
Thumbs.db
*~
*.swp
*.swo
*.tmp
*.bak
.#*
```

The bundle's `.gitignore` or global git ignore configuration is not consulted.
Local ignore configuration must not change the canonical hash.

## Cross-Platform Stability Proof

Implementation of this spec must add a golden corpus and run it on Linux,
macOS, and Windows before claiming implementation closure.

The corpus must include:

- equivalent YAML with different key order, comments, indentation, line
  endings, anchors, aliases, and explicit standard scalar tags
- YAML duplicate-key and unsupported-tag fixtures that fail closed
- prompt text with CRLF, CR, missing final newline, trailing spaces, and a UTF-8
  byte order mark
- binary `data/` files containing NUL bytes and bytes that are invalid UTF-8
- ignored files and ignored directories under recursive walks
- symlink fixtures on platforms that support them, with platform-specific skips
  only for the symlink creation step
- a path-order fixture that proves root-independent slash labels and byte-order
  sorting
- one full bundle fixture with a published expected
  `bundle-v1:sha256:<hex>` value

Focused unit tests are not enough for closure. The implementation proof must
show that the same corpus produces the same hash on all supported operating
systems.

## Relationship to Current Implementation

`internal/runtime/contracts/bundle_identity.go` is the starting implementation
evidence, not the v1 semantic owner. It currently computes a legacy
`sha256:<hex>` fingerprint and must be reconciled after this document is
ratified.

Known divergences that implementation work must address:

| Current behavior | v1 rule |
| --- | --- |
| emits `sha256:<hex>` | emits `bundle-v1:sha256:<hex>` |
| frames entries with label/content NUL delimiters | frames entries with magic plus explicit 64-bit lengths |
| sorts by label and absolute path tie-break | sorts by canonical label only; duplicate labels fail |
| applies extension-based YAML parsing to any `.yaml` / `.yml` file | applies YAML canonicalization only to known YAML contract inputs and platform spec |
| normalizes all non-YAML files as text | prompt files use text canonicalization; flow `data/` files use raw bytes |
| trims trailing spaces and trailing blank lines in text | prompt text preserves all non-line-ending content |
| follows file symlinks via `os.Stat` and silently skips missing files | v1 rejects symlinks and fails closed for discovered invalid inputs |
| has no explicit ignored-file list | v1 has a fixed ignored-path list and does not use `.gitignore` |
| YAML duplicate-key/tag behavior is parser-default | duplicate keys and unsupported tags fail closed |

These divergences mean existing `bundle_fingerprint` values cannot be treated
as v1 `bundle_hash` values by string comparison.

## Migration Relationship to #1001

#1001 owns the public rename from `bundle_fingerprint` / `bundle_ref` to
`bundle_hash`. That migration is a naming and surface migration; it must not
create dual semantic ownership.

Required migration posture:

- `bundle_hash` is the canonical field name for v1 identity.
- Legacy `bundle_fingerprint` values in existing rows are legacy fingerprints,
  not v1 hashes.
- A legacy `sha256:<hex>` value can become a v1 `bundle_hash` only by
  recomputing the bundle from available canonical inputs under this v1 rule.
- Rows whose original bundle inputs are unavailable remain legacy/unavailable
  per the multi-bundle lifecycle migration plan.
- During a dual-accept window, old parameter names may be accepted only as
  aliases that normalize into the canonical `bundle_hash` model where the
  value is a v1 hash.
- New multi-bundle persisted bundle rows and new run rows created after the v1
  implementation lands must write the canonical v1 hash.

The platform-spec/OpenRPC promotion in #1012 must consume this rule and state
the public schema, error behavior, and generated proof artifacts. Runtime,
store, API, CLI, and dashboard consumers must then consume the promoted
platform owner and the reconciled implementation owner.

## Closure Boundaries

This document closes the source-authority gap for bundle canonicalization v1.

It does not close:

- platform-spec/OpenRPC promotion (#1012)
- runtime/store/API/CLI/schema implementation
- current `bundle_identity.go` reconciliation and golden tests
- #1001 dual-accept rename implementation
- multi-bundle lifecycle behavior outside bundle hash identity

Those consumers remain live tracked work under #979, #1001, #1011, and #1012.
