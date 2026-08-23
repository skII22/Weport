# Export Media Quality Upgrade

## Goal

When an incremental export targets an image or video that already exists, replace the old file only when the newly resolved local source is demonstrably better. This lets rerunning an export repair blurry cached media without allowing a later thumbnail fallback to degrade an existing clear file.

## Image Decision

Compare decoded pixel dimensions first. Replace the destination when the source has a larger pixel area. When dimensions are equal, replace only when the source file contains more bytes. If either image cannot be decoded, keep the existing destination unless the normal overwrite strategy was explicitly selected.

## Video Decision

The video resolver already prefers `_raw.mp4`. During incremental export, a `_raw.mp4` source may replace an existing ordinary video. An ordinary source must not replace a destination already known to be raw. Explicit overwrite behavior remains unchanged.

## Scope And Safety

The check runs only for a destination collision during the current export. Rename mode and explicit overwrite mode keep their existing semantics. No WeChat source files or unrelated historical export directories are modified.

## Validation

Cover larger, equal, smaller, unreadable, and missing destination images, plus raw-to-base and base-to-raw video cases. Run `npm run typecheck` and `npm run build:dir`.
