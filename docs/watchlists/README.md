# Watchlists

The canonical watchlist format is now YAML.

Use these files as structured failure-class / maintenance maps:

- [semantic-correctness.yaml](/Users/youmew/dev/swarm/docs/watchlists/semantic-correctness.yaml)
- [operator-surfaces.yaml](/Users/youmew/dev/swarm/docs/watchlists/operator-surfaces.yaml)
- [runtime-operations.yaml](/Users/youmew/dev/swarm/docs/watchlists/runtime-operations.yaml)
- [maintenance-and-cleanup.yaml](/Users/youmew/dev/swarm/docs/watchlists/maintenance-and-cleanup.yaml)

Why YAML:
- better nesting for failure-class tries
- more consistent canonical-owner and manifestation fields
- easier future validation and tooling

Usage rule:
- map failure-class / parity / semantic-drift issues to a watchlist node during intake or pre-audit when possible
- update the relevant node when review discovers a broader class, missed sibling manifestation, or better canonical owner understanding
