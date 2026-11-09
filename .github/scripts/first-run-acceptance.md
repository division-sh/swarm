# Fresh-environment archive check

`release.yml` runs one Linux check on its verified candidate archive and
manifest, before publication. It uses the existing build/package owners;
the driver never rebuilds Swarm. No tag, release or dispatch is initiated.

The outer Linux host needs Python's standard library, Docker and the pinned
prerequisite image. The new non-root environment has no mounts, checkout,
Go, Docker/socket, inherited credentials or provider network. Only archive
inputs are copied in. All Swarm commands are actual processes with PTYs.

From the repository root, with an explicitly identified artifact directory:

```bash
python3 .github/scripts/test-first-run-acceptance.py -v
python3 .github/scripts/first-run-acceptance.py \
  --assets "$ASSETS" --output "$NEW_OUTPUT_DIRECTORY" \
  --version "$VERSION" --source "$SOURCE_SHA" --kind candidate
```

The output directory must not exist. A first failure returns nonzero, joins
owned processes and retains sanitized transcripts plus `receipt.json`;
rerunning or replacing that receipt is not automatic. Read `complete`,
`failure` and cleanup fields, not merely a successful container removal.

The fixture targets post-#3001 commands. Published v0.7.1 keeps its own dot
spelling and historical receipt. A local Linux-only candidate manifest is
not the workflow's complete verified Linux/macOS artifact set. No result
here proves a published download, real token, provider delivery, guided
composer, unaided user budget or native macOS first run. The secret is
synthetic, entered through two hidden prompts; startup receives no event.
