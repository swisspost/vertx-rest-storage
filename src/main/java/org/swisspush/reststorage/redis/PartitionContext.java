package org.swisspush.reststorage.redis;

/**
 * Holds the (possibly Redis Cluster hash-tagged) key and expirable-set name to use for one
 * storage operation, plus the raw partition tag (for partition registry bookkeeping).
 *
 * <p>This is the core building block of the path-based Redis Cluster partitioning feature
 * (see {@code redisClusterPartitioningEnabled} in {@code ModuleConfiguration}): wrapping the
 * first path segment of an encoded resource path in a Redis Cluster
 * <a href="https://redis.io/docs/latest/operations/cluster-tuning/#hash-tags">hash tag</a>
 * (e.g. {@code :project:server:test} becomes {@code :{project}:server:test}) makes all keys
 * derived from that path (resources, collections, locks, and its portion of the expirable set)
 * hash to the same Redis Cluster slot, so multi-key Lua scripts stay cluster-safe without any
 * change to the Lua scripts themselves.</p>
 *
 * <p>Instances are typically obtained via {@link #forPath(String, boolean, String)}. This class
 * is public so other modules (e.g. custom {@code Storage} implementations, tooling, or migration
 * scripts) can reuse the exact same partitioning/tagging logic used internally by
 * {@code RedisStorage}.</p>
 */
public final class PartitionContext {

    /** The path segment separator used by {@code RedisStorage.encodePath(String)}. */
    public static final String PATH_SEP = ":";

    private final String key;
    private final String expirableSetKey;
    private final String tag;

    public PartitionContext(String key, String expirableSetKey, String tag) {
        this.key = key;
        this.expirableSetKey = expirableSetKey;
        this.tag = tag;
    }

    /**
     * The (possibly hash-tagged) key to send as {@code KEYS[1]} to the storage Lua scripts.
     */
    public String getKey() {
        return key;
    }

    /**
     * The (possibly hash-tagged) name of the {@code expirableSet} to use for this operation.
     */
    public String getExpirableSetKey() {
        return expirableSetKey;
    }

    /**
     * The raw partition tag (e.g. {@code project}), or {@code null} when no partitioning
     * applies to this operation (partitioning disabled, or path has no partition-able segment).
     */
    public String getTag() {
        return tag;
    }

    /**
     * Derives the Redis Cluster partition tag from an already-encoded path (e.g. {@code :project:server:test})
     * by taking its first non-empty segment (e.g. {@code project}). Returns {@code null} when the path has
     * no segment to partition on (e.g. root).
     */
    public static String derivePartitionTag(String encodedPath) {
        String trimmed = encodedPath;
        while (trimmed.startsWith(PATH_SEP)) {
            trimmed = trimmed.substring(PATH_SEP.length());
        }
        if (trimmed.isEmpty()) {
            return null;
        }
        int idx = trimmed.indexOf(PATH_SEP);
        String tag = idx == -1 ? trimmed : trimmed.substring(0, idx);
        // Hash tag delimiters must not be part of the tag value itself
        tag = tag.replace("{", "").replace("}", "");
        return tag.isEmpty() ? null : tag;
    }

    /**
     * Builds the {@link PartitionContext} to use for one storage operation on the given encoded path.
     * When {@code partitioningEnabled} is {@code false}, the key and expirable-set name are returned
     * unchanged, preserving the original (non-cluster-safe) key layout for full backward compatibility.
     *
     * @param encodedPath        the already-encoded resource path (e.g. {@code :project:server:test})
     * @param partitioningEnabled whether Redis Cluster path-based partitioning is enabled
     * @param expirableSet       the (untagged, global) expirable-set key configured for this storage
     */
    public static PartitionContext forPath(String encodedPath, boolean partitioningEnabled, String expirableSet) {
        if (!partitioningEnabled) {
            return new PartitionContext(encodedPath, expirableSet, null);
        }
        String tag = derivePartitionTag(encodedPath);
        if (tag == null) {
            return new PartitionContext(encodedPath, expirableSet, null);
        }
        int leadingSeps = 0;
        while (encodedPath.startsWith(PATH_SEP, leadingSeps)) {
            leadingSeps += PATH_SEP.length();
        }
        // Locate the end of the raw first segment (as it appears in encodedPath) rather than relying
        // on tag.length(), since derivePartitionTag() may have stripped literal '{'/'}' characters from
        // it - using the stripped length here would compute the wrong substring offset and corrupt the path.
        int segmentEnd = encodedPath.indexOf(PATH_SEP, leadingSeps);
        if (segmentEnd == -1) {
            segmentEnd = encodedPath.length();
        }
        String taggedKey = encodedPath.substring(0, leadingSeps) + "{" + tag + "}"
                + encodedPath.substring(segmentEnd);
        String taggedExpirableSet = expirableSet + ":{" + tag + "}";
        return new PartitionContext(taggedKey, taggedExpirableSet, tag);
    }
}
