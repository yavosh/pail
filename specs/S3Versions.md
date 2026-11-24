# S3Versions model

[`S3Versions.tla`](S3Versions.tla) is a TLA+ model of one S3 object key in `internal/store`.
It checks that readers see every version that exists while they read, and that a crash at any step loses nothing.

Run all checks:

```bash
specs/check.sh   # needs Java and tla2tools.jar; set JAVA and TLA2TOOLS if they are elsewhere
```

## What the model covers

The state is the key's files: `objects/<hash>.json`, `versions/<hash>/<id>.json`, and its blobs.
Each step is one file commit, one removal, or one read.

- **Writers**, one at a time under the key lock: `PutObject` and `CompleteUpload` (`install`), and `DeleteObject` with and without a version ID.
- **Lifecycle expiry**, under the exclusive bucket lock. The `SweepLocked` constant decides whether it also holds the key lock.
- **Blob staging**: `PutObject` commits its blob before it takes the key lock. A failed write condition removes the blob.
- **Commit failures**: the `objects/` commit in `install` can fail, which runs its cleanup instead.
- **Versioning changes**, under the exclusive bucket lock.
- **Readers**: `GetObjectVersion` and `HeadObjectVersion`, including the blob-open retry, plus `GetObject`, `HeadObject`, and `ListVersions` for the key. The `Locked` constant decides whether they hold the key lock.
- **Crashes** at any step, and the recovery in `store.Open` (`recoverKeyVersions`, `removeOrphanBlobs`), which can crash again.

`Locked` and `SweepLocked` are `FALSE` for the code before the fix and `TRUE` for the fixed code.

## Properties

| Property | Meaning |
| --- | --- |
| `LookupSound` | A lookup returns the version it asks for if that version exists in every state from its first read to its return. The one exception is the known limit below. |
| `LookupNoGiveUp` | `LookupSound` without the exception. It fails by design: a read gives up after `MaxOpen` overwrites remove the blobs it opens (16 in `objects.go:230`). |
| `ListSound` | A listing includes every version that exists in every state of the listing. |
| `ListAtomic` | A listing returns the key's versions exactly as they were in one state. |
| `KeyHasCurrent` | A key with any version has an `objects/` file, in every state. |
| `RecordsHaveBlobs` | Every record file has its blob. |
| `AtRest` | With no writer running: at most one null version, no stale copy of the current record, the current record is the newest, and the files hold exactly the versions that callers were told about, with no orphan blobs. After a crash, recovery leaves the interrupted operation done entirely or not at all. |

## Checks

| Config | Expect | What it shows |
| --- | --- | --- |
| `S3Versions.cfg` | pass | The fixed code. Every property except `LookupNoGiveUp` holds: 13,703,808 distinct states. |
| `S3VersionsBeforeLookup.cfg` | fail | Before the fix, `LookupSound` is violated. |
| `S3VersionsBeforeList.cfg` | fail | Before the fix, `ListSound` is violated. |
| `S3VersionsSweepUnlocked.cfg` | fail | Readers that hold the key lock are not enough: expiry must hold it too. |
| `S3VersionsOpenLimit.cfg` | fail | The known limit: with `MaxOpen` 2, a read of an existing object gives up. |
| `S3VersionsCrash.cfg` | pass | Crash safety with 2 crashes, failed commits, and expiry: 326,845 distinct states. |

## The bugs

Both interleavings start with a null version N under a delete marker M: an unversioned `PutObject`, then versioning enabled, then `DeleteObject` without a version ID.

**`ListVersions` omits a version** (`ListSound`):

1. `ListVersions` reads `objects/` and gets M (`readAllObjects`, `objects.go:338`).
2. `DeleteObject(versionId=M)` writes N to `objects/` and removes `versions/null.json` (`deleteVersion`, `versions.go:355-358`).
3. The listing reads `versions/<hash>/`, which is now empty (`noncurrent`, `versions.go:172`).

The result lists only M, which no longer exists, and omits N, which existed throughout.
Deleting a delete marker to restore an object is a common S3 pattern.

**A version lookup returns `NoSuchVersion`** (`LookupSound`):

1. `HeadObjectVersion(versionId=null)` reads `objects/` and gets M (`lookup`, `versions.go:102`).
2. `DeleteObject(versionId=M)` promotes N to `objects/` and removes `versions/null.json`.
3. The lookup reads `versions/null.json` and gets ENOENT (`versions.go:116`).
4. `DeleteObject` without a version ID adds marker M2. It saves N to `versions/null.json`, then writes M2 to `objects/` (`install`, `versions.go:235`, `:250`).
5. The lookup reads `objects/` again, gets M2, and returns `ErrNoSuchVersion` (`versions.go:117-122`).

N existed in every state. `GetObjectVersion` fails the same way.

**Lifecycle expiry bypasses the key lock.** `sweepBucket` holds only the exclusive bucket lock. Readers that hold the key lock still race with it.
`S3VersionsSweepUnlocked.cfg` shows the failure. In a Suspended bucket, expiry saves the current version V, commits a null delete marker, and removes the old null version. A listing that read V before the expiry then lists only V. A null version existed throughout.

Each bug has a test that forces it:

- `TestLookupDuringPromotionFindsVersion` and `TestListVersionsDuringPromotionListsVersion` use a gated file system to force the interleavings.
- `TestSweepLifecycleHoldsKeyLock` checks that expiry holds the key lock while it changes the key.

All three fail on the code before the fix and pass on the fix.

## The fix

- `lookupLocked` runs `lookupObject` under the key lock for `GetObjectVersion` and `HeadObjectVersion`. The blob open stays outside the lock.
- `ListVersions` calls `keyVersions` for each key. It takes the key lock, reads `objects/` again, and reads the noncurrent records.
- `expire` holds the key lock while it changes a key. It takes the lock inside the exclusive bucket lock, which is the order writers use.

Every change to a key's files now holds the key lock, so a reader sees each key between two changes.

## Replay against real runs

This was a one-time check. Its traces, hooks, and converter are not committed, so `check.sh` does not repeat it.

Temporary hooks, not committed, wrapped each modeled file operation and read in `internal/store`.
They ran during `go test ./internal/store/ ./test/` on each version of the code.
Under one trace mutex, they logged the key's files before and after each step, the version a lookup found, and the versions a listing returned.
A gated test passes its gate before it takes the trace mutex, so its interleaving appears in the log.
A converter, also not committed, mapped blob IDs, `Seq` values, and version IDs to small integers.
It cut a trace where a test changed files directly, such as a forged crash state.

| Code | Segments | Distinct traces | Steps | Accepted |
| --- | --- | --- | --- | --- |
| Before the fix | 7478 | 103 | 35387 | 103 of 103 |
| Fixed | 7478 | 103 | 35469 | 103 of 103 |

Each step pins the files, staged blobs, phase, reader states, the version each lookup found, and each listing's output.
The traces from before the fix include the three failing tests.
Their racy runs replay step by step and violate `LookupSound` and `ListSound` in the model.
The fixed traces also satisfy `LookupSound`, `ListSound`, and `ListAtomic`.
Each trace set is rejected by the model with the other lock settings.
A model with the promotion order swapped rejects the traces from before the fix.

## Independent review

A separate reviewer checked each action against the code and ran TLC. Its findings led to these changes:

- Lifecycle expiry became its own action with `SweepLocked`, which found the missing lock in the fix.
- `LookupSound` now names its one exception, and `LookupNoGiveUp` documents it as a known limit.
- `ListNoDup` could not fail, so `ListAtomic` replaced it.
- `KeyHasCurrent` now holds in every state, including during recovery.
- The replay now pins lookup and listing results and includes the racy test runs.

## Assumptions

- One writer per key at a time, through the key lock (`store.go:297`). Lock striping only adds blocking.
- Reads that a writer makes under the key lock belong to the step that fixes its plan.
- A crash is a process crash. A commit is an atomic rename, and every committed file survives. The local backend does not fsync, so the order of renames after a power loss is not covered.
- Only the `objects/` commit in `install` fails. Removals that ignore errors succeed (`versions.go:216`, `:253`, `:258`, `:262`, `:344`, `:358`, `:364`, `objects.go:159`, `:307`, and `lifecycle.go:116`).
- `Seq` grows with each versioned write (`versions.go:220-223`), so the model numbers versions in install order.
- Concurrent `PutObject` calls interact only through staged blobs and the key lock.

## What it does not prove

- Behavior outside the bounds: 3 or 4 IDs, 1 or 2 crashes, 3 blob-open attempts, and one reader of each kind.
- Recovery's promotion branch (`versions.go:417-424`). No crash in the model reaches it, so it is defensive. Only a replayed test with a hand-written crash state exercises it.
- Other keys, multipart upload files, ACL and tag rewrites (`updateRecord`), and `CreateBucket` or `DeleteBucket`.
- Failed removals, or failed commits other than the `objects/` commit in `install`.
- `ListObjects`, which reads one file per key.

Tools: OpenJDK 27.0.1 and TLC 2.19 (2024-08-08).
