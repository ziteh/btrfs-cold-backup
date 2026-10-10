# Take an incremental's parent from the newest lower level

## Context and Problem Statement

A level N backup was always sent relative to the newest level N-1 backup. Completing a backup drops every deeper entry from `latest.yaml`, because those entries were built on the snapshot it supersedes. So after a level 0 completes, every higher level fails for lack of a parent until each level in between has been backed up again. A wrapper has to chain runs (after level N, run N+1, N+2, …) to recover. A schedule also cannot skip a level on purpose, for example by letting the yearly level 0 stand in for the first quarter's level 1.

Which backup should an incremental build on?

## Considered Options

- Keep the parent at level N-1
- Keep superseded entries in `latest.yaml`, and walk each manifest's parent to check the chain, with a new manifest field recording the parent's level
- Unix `dump` semantics: the parent is the newest backup of any lower level, and completing a backup still drops every deeper entry

## Decision Outcome

Chosen option: "Unix `dump` semantics". Completing a backup still drops every deeper entry, so the entries left in `latest.yaml` are always one chain, ordered by level and by time, each entry the parent of the next. The parent is therefore simply the deepest entry below the target level. The existing chain check (HEAD every part of every lower entry) stays correct, and the manifest format does not change.

Keeping superseded entries would let a new level 0 coexist with a chain built on the old one. It needs a manifest walk to tell the chains apart, and it keeps the new chains depending on the old level 0.

### Consequences

- Good, because higher levels build on a new level 0 as soon as it completes, without a wrapper chaining runs.
- Good, because a schedule may skip levels.
- Good, because chains are shorter and stop depending on an old level 0 as soon as a new one completes.
- Good, because the manifest format and `FORMAT_VERSION` are unchanged.
- Bad, because between a lower level's completion and the next backup of an intermediate level, higher levels are cumulative incrementals against an older ancestor, and so are larger.
- Bad, because a restore cannot assume a backup's parent is at the level below. It must follow the `parent_snapshot` its manifest records.
