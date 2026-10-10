# Enable S3 Versioning, defer Object Lock

## Context and Problem Statement

bcb never deletes from S3, but anyone holding the host's S3 credentials can call the S3 API directly. The backup IAM policy grants `s3:PutObject` and `s3:DeleteObject`, so stolen credentials could delete or overwrite backups.

Should the bucket use Versioning, Object Lock (WORM), or both?

## Considered Options

- No protection
- Versioning only
- Versioning + Object Lock on every object (bucket default retention)
- Versioning + Object Lock on level 0 data parts only

## Decision Outcome

Chosen option: "Versioning only", because combined with the least-privilege policy (no `s3:DeleteObjectVersion`, `s3:PutBucketVersioning`, `s3:PutLifecycleConfiguration` or `s3:PutBucketPolicy`), stolen host credentials can only add delete markers or new versions, and earlier versions stay recoverable.

Object Lock adds protection only against the account's root or an admin, at the cost of retention periods that cannot follow bcb's chain-based retention. Object Lock on level 0 data parts only is kept as a todo.

### Consequences

- Good, because deletions and overwrites made with stolen host credentials are recoverable.
- Good, because bcb does not need any change: it never overwrites data parts, so the extra versions are limited to small objects such as `latest.yaml` and the `bcb check` probe.
- Bad, because lifecycle `Expiration` only adds a delete marker on a versioned bucket. Every rule needs `NoncurrentVersionExpiration` and `ExpiredObjectDeleteMarker` as well, or expired backups keep being billed.
- Bad, because overwritten data is recoverable only until `NoncurrentVersionExpiration` runs. Data parts need a long window, or none, so that a compromise can be noticed in time.
- Bad, because neither option stops cost attacks (bulk uploads, retrieval requests). Those are addressed separately with budget alerts, split backup and restore policies, and source IP restriction.
- Bad, because the account's root or an admin can still disable Versioning or delete versions.
