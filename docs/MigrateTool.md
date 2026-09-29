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
| `rest-storage:migration:lock`   | Distributed lock, holds the owning instance's timestamp as value | 10s, refreshed every 2s while held |

Per-task "done" flags (if a task implements one, like `ClusterPartitionMigrationTask` does) are a
separate concern owned by the task itself - see [ClusterPartitionMigrationTask.MD](ClusterPartitionMigrationTask.MD#completion-flag).

## `start()` control flow

```mermaid
flowchart TD
    A["start()"] --> B{"tasks registered?"}
    B -- no --> Z["complete immediately"]
    B -- yes --> C["acquireLock()\nSET lock NX PX 10000"]
    C --> D{"lock acquired?"}
    D -- yes --> E["startRefreshTimer()\nPEXPIRE every 2s"]
    E --> F["runTasksSequentially()"]
    F --> G["releaseLock()\nstop timer + DEL lock"]
    G --> H{"all tasks succeeded?"}
    H -- yes --> I["complete future"]
    H -- no --> J["fail future"]
    D -- no, held by another instance --> K["waitForOtherMigrationCompletion()\npoll EXISTS lock every 2s"]
    K --> L{"lock key still exists?"}
    L -- yes --> K
    L -- no --> I
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
        N1->>R: SET migration:lock "t1" NX PX 10000
        N2->>R: SET migration:lock "t2" NX PX 10000
        N3->>R: SET migration:lock "t3" NX PX 10000
    end

    R-->>N1: OK (lock acquired)
    R-->>N2: nil (lock already held)
    R-->>N3: nil (lock already held)

    Note over N1: node-1 owns the lock,<br/>runs tasks sequentially

    loop every 2s while task(s) run
        N1->>R: PEXPIRE migration:lock 10000
    end

    par node-2 and node-3 wait
        loop every 2s
            N2->>R: EXISTS migration:lock
            R-->>N2: 1 (still running)
        end
        loop every 2s
            N3->>R: EXISTS migration:lock
            R-->>N3: 1 (still running)
        end
    end

    Note over N1: task(s) finished (success or failure)
    N1->>R: DEL migration:lock
    N1-->>N1: complete/fail future

    N2->>R: EXISTS migration:lock
    R-->>N2: 0 (gone)
    N2-->>N2: complete future

    N3->>R: EXISTS migration:lock
    R-->>N3: 0 (gone)
    N3-->>N3: complete future
```

Notes on this example:

- **Only `node-1` runs the actual migration work.** `node-2` and `node-3` never execute the task's
  logic themselves in this run - they just wait for the lock to disappear and then continue their
  own startup, assuming the migration is now done (or was already done, see below).
- **Which node wins the race is non-deterministic.** Whichever `SET ... NX` reaches Redis first
  wins; it depends purely on network/scheduling timing, not on instance identity or start order.
- **The lock is refreshed, not fixed-TTL.** Because `ClusterPartitionMigrationTask` can take a long
  time on large data sets, `node-1` refreshes the lock's TTL every 2 seconds via `PEXPIRE`. If
  `node-1` crashes without releasing the lock, the lock still expires after at most 10 seconds
  (one missed refresh cycle), so the other nodes are never blocked forever.
- **Waiting nodes don't verify success.** `waitForOtherMigrationCompletion()` only checks that the
  lock key is gone; it does not know or check whether `node-1`'s migration actually succeeded. If
  `node-1`'s task fails, `node-1`'s own `start()` future fails, but `node-2`/`node-3` still see an
  absent lock and complete normally. This is intentional: `MigrateTool` does not implement retry or
  failure propagation across nodes - each node's own logs are the source of truth for whether its
  local `start()` call failed.
- **Idempotent tasks matter.** Because a crash of `node-1` mid-migration releases the lock only via
  TTL expiry (not a clean `DEL`), another node could then acquire the lock and re-run the task from
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
  further tasks run), the lock is still released in a `finally`-like fashion, and `start()`'s future
  fails with that task's error.
- Nodes that were waiting on the lock (`waitForOtherMigrationCompletion`) are **not** notified of
  the failure - they simply see the lock disappear and complete successfully. This is a known
  trade-off of the current polling-based design: operators should watch the logs of whichever node
  won the lock race to confirm actual task success.

## See also

- [ClusterPartitionMigrationTask.MD](ClusterPartitionMigrationTask.MD) - the task that currently runs through `MigrateTool`
- [MIGRATION.md](../MIGRATION.md) - operational guide (prerequisites, quick start, troubleshooting, rollback)
- [`MigrateTool.java`](../src/main/java/org/swisspush/reststorage/migration/MigrateTool.java)
- [`MigrateToolTest.java`](../src/test/java/org/swisspush/reststorage/migration/MigrateToolTest.java)
