# MigrateTool

`MigrateTool` ([source](../src/main/java/org/swisspush/reststorage/migration/MigrateTool.java)) is a
generic, task-agnostic coordinator that ensures a set of registered [`Task`](../src/main/java/org/swisspush/reststorage/migration/tasks/Task.java)s
(e.g. [`ClusterPartitionMigrationTask`](ClusterPartitionMigrationTask.MD)) runs **exactly once at a time**
across all application instances that share the same Redis, using a distributed lock.

It knows nothing about what a task actually does - it only handles:
- mutual exclusion (a single distributed lock in Redis),
- keeping the lock alive for the duration of a long-running migration,
- letting every other instance wait for the in-progress migration to finish,
- running the registered tasks sequentially, stopping at the first failure.

## Key layout

| Key                             | Purpose                                     | TTL                                    |
|----------------------------------|----------------------------------------------|-----------------------------------------|
| `rest-storage:migration:lock`   | Distributed lock, value is a random per-acquisition token (`instanceId:UUID`) | 10s, refreshed every 2s while held; **removed (never expires) if the migration fails** - see below |

**Lock safety (ownership token):** the lock's value is not just an identifier for logging - it is a
compare-and-swap token. Both the periodic TTL refresh and the final release run a small Lua script that
only mutates the lock (`PEXPIRE`/`DEL`) if it still holds *this instance's* token, instead of doing so
unconditionally. This matters because a long GC pause (or any other stall) can let the lock's TTL expire
while an instance still believes it holds it; without the token check, that stalled instance could later
refresh or delete a *different* instance's now-legitimately-held lock, breaking mutual exclusion. If a
refresh ever finds the token no longer matches, the instance stops refreshing immediately and treats its
own migration result as untrustworthy (`start()`'s future fails even if the task chain itself reported
success) - see [`MigrateTool.java`](../src/main/java/org/swisspush/reststorage/migration/MigrateTool.java)'s
class Javadoc for the full rationale.

**Failure marker (never auto-expires):** if a registered task genuinely fails (returns `false`/throws),
the owning instance does **not** release the lock. Instead it rewrites the lock's value to
`<token>:FAILED` via a CAS-checked `SET` (which also strips the key's TTL), and leaves it there
permanently. This is deliberate: instances that were only *waiting* on the lock (see
`waitForOtherMigrationCompletion`/`pollLockKey` below) poll the lock's value, not just its existence -
so they can tell "still running" apart from "failed and stuck" and fail their own `start()` future too,
instead of mistaking a disappeared/expired lock for a successfully completed migration. Because the
marker never expires, a human must manually delete `rest-storage:migration:lock` in Redis before the
migration can be retried (after fixing the underlying problem) - this is intentional friction, trading
availability for never silently starting up with `redisClusterPartitioningEnabled=true` over unmigrated
data.

Per-task "done" flags (if a task implements one, like `ClusterPartitionMigrationTask` does) are a
separate concern owned by the task itself - see [ClusterPartitionMigrationTask.MD](ClusterPartitionMigrationTask.MD#completion-flag).

## `start()` control flow

```mermaid
flowchart TD
    A["start()"] --> B{"tasks registered?"}
    B -- no --> Z["complete immediately"]
    B -- yes --> C["acquireLock()\nSET lock token NX PX 10000"]
    C --> D{"lock acquired?"}
    D -- yes --> E["startRefreshTimer()\nEVAL: PEXPIRE only if GET lock == token"]
    E --> F["runTasksSequentially()"]
    F --> H{"lockOwnershipLost during the run?"}
    H -- yes --> J["fail future\n(result untrustworthy, lock may now be another instance's,\nlock left untouched)"]
    H -- no --> I2{"all tasks succeeded?"}
    I2 -- yes --> G1["releaseLock()\nEVAL: DEL only if GET lock == token"]
    G1 --> I["complete future"]
    I2 -- no --> G2["markLockAsFailed()\nEVAL: SET lock token+':FAILED' only if GET lock == token\n(clears TTL - lock never expires)"]
    G2 --> J
    D -- no, held by another instance --> K["waitForOtherMigrationCompletion()\npoll GET lock every 2s"]
    K --> L{"poll result?"}
    L -- "lock key gone" --> I
    L -- "lock value ends with ':FAILED'" --> J
    L -- "Redis error while polling" --> J
    L -- "lock key still exists, not failed" --> K
```

## Multi-node example (3 nodes)

Assume 3 application instances (`node-1`, `node-2`, `node-3`) start up at roughly the same time,
all with `redisClusterPartitioningEnabled=true`, all sharing the same Redis. Each instance builds
its own `MigrateTool` (with a unique `instanceId`) and registers the same tasks (e.g. one
`ClusterPartitionMigrationTask`), then calls `start()` during boot.

```mermaid
sequenceDiagram
    participant N1 as node-1 (MigrateTool)
    participant N2 as node-2 (MigrateTool)
    participant N3 as node-3 (MigrateTool)
    participant R as Redis

    par all nodes start roughly together
        N1->>R: SET migration:lock "node-1:token-a" NX PX 10000
        N2->>R: SET migration:lock "node-2:token-b" NX PX 10000
        N3->>R: SET migration:lock "node-3:token-c" NX PX 10000
    end

    R-->>N1: OK (lock acquired)
    R-->>N2: nil (lock already held)
    R-->>N3: nil (lock already held)

    Note over N1: node-1 owns the lock (holds token-a),<br/>runs tasks sequentially

    loop every 2s while task(s) run
        N1->>R: EVAL refresh script<br/>(PEXPIRE only if GET lock == "node-1:token-a")
    end

    par node-2 and node-3 wait
        loop every 2s
            N2->>R: GET migration:lock
            R-->>N2: "node-1:token-a" (still running)
        end
        loop every 2s
            N3->>R: GET migration:lock
            R-->>N3: "node-1:token-a" (still running)
        end
    end

    alt task(s) succeed
        Note over N1: task(s) finished successfully
        N1->>R: EVAL release script<br/>(DEL only if GET lock == "node-1:token-a")
        N1-->>N1: complete future

        N2->>R: GET migration:lock
        R-->>N2: nil (gone)
        N2-->>N2: complete future

        N3->>R: GET migration:lock
        R-->>N3: nil (gone)
        N3-->>N3: complete future
    else a task fails
        Note over N1: task(s) failed - lock is NOT released
        N1->>R: EVAL mark-failed script<br/>(SET lock "node-1:token-a:FAILED" only if GET lock == "node-1:token-a")
        N1-->>N1: fail future

        N2->>R: GET migration:lock
        R-->>N2: "node-1:token-a:FAILED"
        N2-->>N2: fail future (does not proceed as if migration succeeded)

        N3->>R: GET migration:lock
        R-->>N3: "node-1:token-a:FAILED"
        N3-->>N3: fail future
        Note over R: lock has no TTL now - stays until an<br/>operator manually deletes it and retries
    end
```

Notes on this example:

- **Only `node-1` runs the actual migration work.** `node-2` and `node-3` never execute the task's
  logic themselves in this run - they just wait for the lock to disappear and then continue their
  own startup, assuming the migration is now done (or was already done, see below).
- **Which node wins the race is non-deterministic.** Whichever `SET ... NX` reaches Redis first
  wins; it depends purely on network/scheduling timing, not on instance identity or start order.
- **The lock is refreshed, not fixed-TTL, and ownership is verified on every refresh/release.**
  Because `ClusterPartitionMigrationTask` can take a long time on large data sets, `node-1` refreshes
  the lock's TTL every 2 seconds - but only if the lock still holds `node-1`'s own token (see
  "Lock safety" above). If `node-1` stalls long enough for the TTL to expire and another node takes
  over the lock in the meantime, `node-1`'s subsequent refresh/release calls are no-ops against that
  other node's lock, and `node-1`'s own `start()` future fails (result no longer trustworthy) instead
  of silently corrupting the new owner's lock. If `node-1` crashes outright without releasing the
  lock, the lock still expires after at most 10 seconds (one missed refresh cycle), so the other
  nodes are never blocked forever.
- **Waiting nodes verify success via the lock's own value, and surface their own connectivity
  failures.** `waitForOtherMigrationCompletion()` polls the lock with `GET` (not just `EXISTS`), so it
  can distinguish "gone" (success) from "still there but marked `:FAILED`" (the owning node's task
  failed) from "still there, not marked" (still running) - `node-2`/`node-3` fail their own `start()`
  future in the `:FAILED` case instead of wrongly completing. If a waiting node itself loses its Redis
  connection while polling, its own `start()` future fails too (a poll failure is never mistaken for
  "migration complete").
- **A failed migration blocks all future startups until an operator intervenes.** Unlike a successful
  run, a failed run's lock is never released or allowed to expire (see "Failure marker" above) - every
  node (the failed one and all future restarts of any node) will keep failing to acquire the lock or
  will see the `:FAILED` marker and fail too, until a human deletes `rest-storage:migration:lock` in
  Redis (after addressing the underlying cause). This is a deliberate fail-closed choice: a stuck
  deployment is easier to notice and safer than one that silently proceeds with
  `redisClusterPartitioningEnabled=true` over data that was never actually migrated.
- **Idempotent tasks matter.** Because a crash of `node-1` mid-migration releases the lock only via
  TTL expiry (not a clean release), another node could then acquire the lock and re-run the task from
  scratch. Tasks such as `ClusterPartitionMigrationTask` are written to be safely re-run (renames of
  already-migrated keys are no-ops), and additionally short-circuit entirely once their own
  completion flag is set - see [ClusterPartitionMigrationTask.MD](ClusterPartitionMigrationTask.MD#completion-flag).
- **A later restart is cheap.** If all 3 nodes are restarted again after the migration already
  completed, every node's `MigrateTool` runs `runTasksSequentially()` (winning node) or waits
  (losing nodes) exactly the same way, but the winning node's task(s) recognize their own "done"
  state and return immediately without doing any work - so the lock is only held very briefly.

## Failure handling

- If `acquireLock()` itself fails (e.g. Redis unreachable), `start()` fails immediately without
  running any tasks.
- If a registered task returns `false` or throws, `runTasksSequentially()` stops the chain (no
  further tasks run). The lock is **not** released - it is turned into a permanent `:FAILED` marker
  (see "Failure marker" above) - and `start()`'s future fails with that task's error.
- If lock ownership is lost mid-run (see "Lock safety" above), `start()`'s future fails even when the
  task chain itself completed successfully, since exclusivity can no longer be trusted for that run.
  In this case the lock is left untouched entirely (it may already be a different instance's) - no
  failure marker is written, since this instance no longer owns the lock to mark.
- If a waiting node's own Redis connection fails while polling for another instance's completion,
  that node's `start()` future fails too, instead of being mistaken for a completed migration.
- Nodes that were waiting on the lock **are** notified of a remote failure on the winning node: once
  they observe the lock's `:FAILED` marker via `GET`, they fail their own `start()` future too, instead
  of treating the lock's continued (or eventual) presence/absence as success. Because the marker never
  expires, this failure is "sticky" across restarts until an operator manually clears the lock key.

## See also

- [ClusterPartitionMigrationTask.MD](ClusterPartitionMigrationTask.MD) - the task that currently runs through `MigrateTool`
- [MIGRATION.md](../MIGRATION.md) - operational guide (prerequisites, quick start, troubleshooting, rollback)
- [`MigrateTool.java`](../src/main/java/org/swisspush/reststorage/migration/MigrateTool.java)
- [`MigrateToolTest.java`](../src/test/java/org/swisspush/reststorage/migration/MigrateToolTest.java)
