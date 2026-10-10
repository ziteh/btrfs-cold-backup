# Restart an interrupted part's upload from its start

## Context and Problem Statement

An interrupted upload, e.g. a long level 0 stopped so an incremental can run, aborts the part's unfinished multipart upload on the next run (`abort_orphaned_uploads`), and the part is uploaded again from its first chunk.

Should the state record each part's multipart upload ID and the ETags of its finished chunks, so a resumed run continues within the part?

## Considered Options

- Restart the interrupted part
- Resume within the part

## Decision Outcome

Chosen option: "Restart the interrupted part". An interruption costs on average half a part's upload time and nothing on the bill, since uploads are free and aborted multipart uploads aren't stored. Even a level 0 stopped weekly over a slow uplink loses well under one percent of its upload time.

Resuming within a part changes the upload path, the one that most needs to be reliable. It adds state that can point at an upload S3 no longer has (aborted by a lifecycle rule, or by another run), and it depends on `ListParts`, which S3-compatible services support unevenly.

This should be revisited if uplinks get much slower or parts much larger, which would make a restarted part cost a real share of the upload time.

### Consequences

- Good, because the upload path stays simple and a run can be stopped at any point.
- Bad, because each interruption wastes up to one part's upload time.
- Bad, because neither option helps an interruption during the send, which restarts the whole send.
