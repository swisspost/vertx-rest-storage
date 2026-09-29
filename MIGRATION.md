# Redis Cluster Partitioning Migration Guide

This guide explains how to use the `MigrateTool` to migrate your REST Storage data from non-cluster mode to cluster mode when enabling Redis Cluster path-based partitioning.

## Overview

When you enable `redisClusterPartitioningEnabled`, the physical Redis key layout changes to support Redis Cluster's hash tag requirements. Keys are transformed from:
- Non-cluster: `:project:server:test`
- Cluster: `:{project}:server:test` (first segment wrapped in `{}`)

This change means existing data written in non-cluster mode will not be found by cluster-mode operations. The `MigrateTool` automates this key rewrite process.

## Architecture

The migration tool provides:

1. **Task Interface** (`Task`): Implement this to define custom migration logic
2. **MigrateTool**: Orchestrates task execution with distributed locking
3. **ClusterPartitionMigrationTask**: Rewrites the whole REST Storage key space into its cluster-partitioned layout
4. **Distributed Lock**: Ensures only one instance migrates data at a time across cluster

### What `ClusterPartitionMigrationTask` rewrites

| Key | Before | After |
|---|---|---|
| resource | `rest-storage:resources:project:a` | `rest-storage:resources:{project}:a` |
| collection | `rest-storage:collections:project:a` | `rest-storage:collections:{project}:a` |
| lock | `rest-storage:locks:project:a` | `rest-storage:locks:{project}:a` |
| delta | `delta:resources:...`, `delta:etags:...` | tagged the same way |
| expirable set | one global ZSET `rest-storage:expirable` | one ZSET per partition `rest-storage:expirable:{project}`, members rewritten |
| root collection | ZSET `rest-storage:collections` | *deleted*; its members seed the partition registry |
| partition registry | &mdash; | SET `rest-storage:locks-partitions` |

The partition registry is not optional: cluster-mode root `GET`/`DELETE` and `cleanup` are served from it, so
without it migrated data is invisible at root even though every resource key exists.

The task is idempotent &mdash; keys that already carry their hash tag are detected and skipped, so a
re-run is a no-op.

### Important constraints

- **Run it against the pre-cluster Redis** (single-node or Sentinel), *before* switching the deployment
  to Redis Cluster. Keys are moved with server-side `RENAME`, which is atomic, binary-safe and
  TTL-preserving, but cannot move a key across hash slots. Running it against a live cluster that still
  holds untagged keys fails with `CROSSSLOT`.
- `RENAME` is used deliberately instead of `DUMP`/`RESTORE`: resource payloads are stored as
  ISO-8859-1 strings and may be gzip binary, so round-tripping them through the string-based `RedisAPI`
  would transcode them as UTF-8 and corrupt them.
- Key discovery uses `SCAN`, which only covers the node it is issued against.

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

The `MigrateTool` uses Redis SET NX to implement distributed locking:

1. First instance to acquire lock runs all tasks
2. Other instances wait for lock to be released
3. Lock TTL is 10 seconds, refreshed every 2 seconds
4. If an instance crashes, lock auto-expires

This ensures:
- Tasks run only once across cluster
- Safe for multi-instance deployments
- Automatic failover if instance dies

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

- [Redis Cluster Support](../README.md#redis-cluster-support)
- [ModuleConfiguration](../README.md#configuration)
- [MigrateTool JavaDoc](./src/main/java/org/swisspush/reststorage/migration/MigrateTool.java)
