# Redis Cluster Partitioning Migration Guide

This guide explains how to use the `MigrateTool` to migrate your REST Storage data from non-cluster mode to cluster mode when enabling Redis Cluster path-based partitioning.

## Overview

When you enable `redisClusterPartitioningEnabled`, the physical Redis key layout changes to support Redis Cluster's hash tag requirements. Keys are transformed from:
- Non-cluster: `:project:server:test`
- Cluster: `:{project}:server:test` (first segment wrapped in `{}`)

This change means existing data written in non-cluster mode will not be found by cluster-mode operations. The `MigrateTool` automates this key rewrite process.

**Runs automatically at startup:** when REST Storage is wired up via `RestStorageMod` (the normal way of
running this module) with `redisClusterPartitioningEnabled=true`, `MigrateTool` (with
`ClusterPartitionMigrationTask` registered) is started automatically on every boot, before the module
starts serving traffic - you do not need to wire it up yourself unless you are embedding these classes
directly in a custom setup (see "Quick Start" below for that case). If that automatic migration fails,
`RestStorageMod` now refuses to start rather than silently continuing with partitioning enabled, since
doing so could make pre-existing, not-yet-migrated data permanently invisible.

## Architecture

The migration tool provides:

1. **Task Interface** (`Task`): Implement this to define custom migration logic
2. **MigrateTool**: Orchestrates task execution with distributed locking
3. **ClusterPartitionMigrationTask**: Rewrites the whole REST Storage key space into its cluster-partitioned layout
4. **Distributed Lock**: Ensures only one instance migrates data at a time across cluster

For the full key-layout table, the completion flag, batching, and diagrams of the internal control
flow, see [docs/ClusterPartitionMigrationTask.MD](docs/ClusterPartitionMigrationTask.MD). For how the
distributed lock coordinates multiple REST Storage instances (with a 3-node walkthrough), see
[docs/MigrateTool.md](docs/MigrateTool.md).

### Important constraints

- **Run it against the pre-cluster Redis** (single-node or Sentinel), *before* switching the deployment
  to Redis Cluster. Keys are moved with server-side `RENAME`, which is atomic, binary-safe and
  TTL-preserving, but cannot move a key across hash slots. Running it against a live cluster that still
  holds untagged keys fails with `CROSSSLOT`.
- `RENAME` is used deliberately instead of `DUMP`/`RESTORE`: resource payloads are stored as
  ISO-8859-1 strings and may be gzip binary, so round-tripping them through the string-based `RedisAPI`
  would transcode them as UTF-8 and corrupt them.
- Key discovery uses `SCAN`, which only covers the node it is issued against.
- The task is safe to re-run: once it completes, a permanent completion flag makes subsequent runs a
  no-op instead of re-scanning the whole key space (see
  [docs/ClusterPartitionMigrationTask.MD#completion-flag](docs/ClusterPartitionMigrationTask.MD#completion-flag)).

## Quick Start

### Prerequisites
- Redis instance accessible (single-node, Sentinel, or pre-cluster)
- REST Storage configured with existing data
- Java project with REST Storage dependencies

### Basic Usage

```java
import org.swisspush.reststorage.migration.MigrateTool;
import org.swisspush.reststorage.migration.tasks.ClusterPartitionMigrationTask;
import io.vertx.core.Vertx;
import org.swisspush.reststorage.redis.RedisProvider;
import org.swisspush.reststorage.util.ModuleConfiguration;

// In your initialization code:
Vertx vertx = Vertx.vertx();
RedisProvider redisProvider = new DefaultRedisProvider(vertx, config);
ModuleConfiguration config = new ModuleConfiguration()
    .resourcesPrefix("rest-storage:resources")
    .collectionsPrefix("rest-storage:collections")
    .expirablePrefix("rest-storage:expirable")
    .lockPrefix("rest-storage:locks");

// Create and run migration
MigrateTool migrateTool = new MigrateTool(vertx, redisProvider, "instance-1");
migrateTool.addTask(new ClusterPartitionMigrationTask(redisProvider, config));

migrateTool.start()
    .onComplete(ar -> {
        if (ar.succeeded()) {
            log.info("Migration completed successfully");
            // Now enable redisClusterPartitioningEnabled = true
        } else {
            log.error("Migration failed", ar.cause());
        }
    });
```

## Migration Workflow

### Step 1: Prepare

1. Verify all REST Storage instances are using the same Redis configuration
2. Ensure you have a backup of your Redis data
3. Consider a maintenance window to avoid writes during migration

### Step 2: Stop Writes (Optional)

You can migrate while writes are happening, but for safety:
```bash
# Stop REST Storage instances
systemctl stop rest-storage

# Or pause traffic at the load balancer
```

### Step 3: Run Migration

Deploy and run the migration tool:
```java
// Can be integrated into REST Storage startup,
// or run as a separate deployment

MigrateTool migrateTool = new MigrateTool(vertx, redisProvider, getInstanceId());
migrateTool.addTask(new ClusterPartitionMigrationTask(...));

migrateTool.start().onComplete(result -> {
    if (result.succeeded()) {
        System.out.println("Migration succeeded, safe to enable cluster partitioning");
        System.exit(0);
    } else {
        System.err.println("Migration failed: " + result.cause());
        System.exit(1);
    }
});
```

### Step 4: Verify

```bash
# Check migrated data is accessible
curl http://localhost:8080/storage/resources/

# Verify key layout in Redis CLI
redis-cli
> KEYS rest-storage:resources:*
# Should show :{...}:... format for cluster mode keys
```

### Step 5: Enable Cluster Partitioning

Update your configuration:
```json
{
  "redisClusterPartitioningEnabled": true,
  "redisClusterNodes": ["node1:6379", "node2:6379", "node3:6379"]
}
```

Restart REST Storage instances.

## Implementing Custom Migration Tasks

Extend `Task` for custom migration logic:

```java
public class MyCustomMigrationTask implements Task {
    @Override
    public String getTaskKey() {
        return "my-custom-migration";
    }

    @Override
    public Future<Boolean> run() {
        // Your migration logic here
        // Return Future.succeededFuture(true) on success
        // Return Future.succeededFuture(false) on failure
        
        return Future.succeededFuture(true);
    }
}

// Add to migration tool
migrateTool.addTask(new MyCustomMigrationTask());
```

## Distributed Locking

The `MigrateTool` uses a Redis `SET NX PX` lock (refreshed every 2 seconds while held, 10 second TTL,
value a random per-acquisition ownership token) so that only one instance runs the tasks while every
other instance simply waits for the lock to be released - safe for multi-instance deployments, with
automatic failover if the running instance crashes, and safe even if a stalled instance's TTL expires
and another instance takes over mid-run (the ownership token prevents the stalled instance from later
refreshing or deleting the new owner's lock). See [docs/MigrateTool.md](docs/MigrateTool.md) for the
full sequence diagram, the ownership-token rationale, and a worked 3-node example.

## Troubleshooting

### Migration takes too long
- Consider running during low-traffic period
- Increase `BATCH_SIZE` in `ClusterPartitionMigrationTask` for faster scanning
- Monitor Redis CPU and memory usage

### Migration fails with "Lock timeout"
- Another instance is already migrating; wait or restart it
- Check Redis connectivity from all instances
- Verify Redis is not under memory pressure

### Keys not found after migration
- Verify migration completed successfully (no errors in logs)
- Check Redis key patterns with `redis-cli KEYS ...`
- Ensure cluster partitioning flag is enabled in config

### Rollback

If issues occur after migration:
1. Disable `redisClusterPartitioningEnabled`
2. Restart REST Storage instances
3. Old data remains in non-cluster key layout

## Performance Considerations

- **Data volume**: Large datasets (> 1GB) may take hours to migrate
- **Lock contention**: Only one instance runs tasks at a time
- **Redis load**: SCAN is non-blocking but can impact performance
- **Batch size**: Larger batches migrate faster but use more memory

## Security

The migration lock key is: `rest-storage:migration:lock`

- Stored in same Redis instance as application data
- No authentication added (uses existing Redis auth)
- TTL prevents permanent locks from stalled instances
- Consider ACLs in Redis 6+ to restrict access

## See Also

- [docs/ClusterPartitionMigrationTask.MD](docs/ClusterPartitionMigrationTask.MD) - key layout, batching, completion flag, control-flow diagrams
- [docs/MigrateTool.md](docs/MigrateTool.md) - distributed lock coordination, 3-node example
- [Redis Cluster Support](README.md#redis-cluster-support)
- [ModuleConfiguration](README.md#configuration)
- [MigrateTool JavaDoc](./src/main/java/org/swisspush/reststorage/migration/MigrateTool.java)
