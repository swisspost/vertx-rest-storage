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

    // Redis Cluster hash tags use the *first* '{' and the first following '}' in the key to
    // determine the slot, so a raw path segment's own literal '{'/'}' characters cannot be embedded
    // verbatim inside our synthetic hash tag - and must not simply be discarded either (two distinct
    // segments differing only by such characters, e.g. "foo" and "fo{o}", would otherwise reduce to
    // the identical tag "foo" and collide onto the very same Redis key, silently overwriting data).
    // Instead they are escaped using a genuinely injective (JSON-Pointer-style) scheme: the escape
    // marker itself is escaped first, so no raw input - including input that happens to already
    // contain the escape marker character - can ever collide with another raw segment's escaped
    // output. A naive fixed one-for-one character substitution (e.g. always replacing '{' with some
    // placeholder character) would NOT have this guarantee, since a raw segment containing that
    // literal placeholder character would then collide with an unrelated segment whose brace got
    // replaced by it.
    private static final char ESCAPE_MARKER = '\u00A6'; // ¦

    private final String key;
    private final String expirableSetKey;
    private final String tag;
    private final boolean topLevel;

    public PartitionContext(String key, String expirableSetKey, String tag) {
        this(key, expirableSetKey, tag, false);
    }

    public PartitionContext(String key, String expirableSetKey, String tag, boolean topLevel) {
        this.key = key;
        this.expirableSetKey = expirableSetKey;
        this.tag = tag;
        this.topLevel = topLevel;
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
     * True when this operation's key is targeting exactly a partition's top-level segment - i.e. the
     * tagged key is {@code <leading separators>{tag}} with no further path suffix - as opposed to some
     * deeper resource nested under that partition. Only meaningful when {@link #getTag()} is non-null;
     * always {@code false} when partitioning is disabled or the path has no partition-able segment.
     *
     * <p>This is a structural check (based on where the raw first path segment actually ends in the
     * original encoded path), not a string heuristic on the resulting key - unlike e.g. checking
     * whether {@link #getKey()} ends with {@code '}'}, which would wrongly also match ordinary nested
     * resources whose own last path segment happens to literally end in {@code '}'}
     * (e.g. {@code /project/name}}).</p>
     */
    public boolean isTopLevel() {
        return topLevel;
    }

    /**
     * Derives the Redis Cluster partition tag from an already-encoded path (e.g. {@code :project:server:test})
     * by taking its first non-empty segment (e.g. {@code project}). Returns {@code null} when the path has
     * no segment to partition on (e.g. root).
     *
     * <p>Two distinct situations reach this method with a first segment that already looks like
     * {@code {something}} (with a non-empty {@code something}):
     * <ul>
     *   <li>The segment is the result of a <i>previous</i> tagging pass resurfacing - e.g. a key
     *       {@code ClusterPartitionMigrationTask} already renamed to {@code :{project}:...} coming back
     *       around in the same {@code SCAN} (Redis's at-least-once guarantee). This <b>must</b> be
     *       recognized and passed through unchanged (returning {@code project}, not a doubly-wrapped
     *       tag), or every re-visit would wrap the key again, corrupting it.</li>
     *   <li>The segment is genuinely raw, user-supplied path content that happens to itself be shaped
     *       like {@code {something}} (nothing upstream of this class escapes {@code {}/{@code }}} in
     *       resource paths). This is indistinguishable from the first case by construction, and is
     *       therefore treated the same way (the tag becomes {@code something}) - meaning a raw segment
     *       exactly of the form {@code {x}} and a raw segment {@code x} unavoidably collide onto the
     *       same partition tag/key. This is a narrow, accepted limitation; avoiding it entirely would
     *       require escaping literal {@code {}/{@code }} in every resource path up front (in
     *       {@code RedisStorage.encodePath}), which is a larger, separate change with its own
     *       backward-compatibility implications for already-stored (non-cluster) keys. Note this
     *       ambiguity only applies when {@code something} itself is non-empty - a literal segment
     *       {@code "{}"} is treated as ordinary raw content (escaped like any other), not as an empty
     *       tag/root, since {@code forPath} never itself produces an empty tag.</li>
     * </ul>
     * Any other literal {@code {}/{@code }} characters - i.e. ones that do <i>not</i> single-wrap the
     * whole segment - are escaped using {@link #escapeBraces(String)}, so e.g. {@code foo} and
     * {@code fo{o}} still reliably produce different tags, regardless of the raw segment's content.
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
        String segment = idx == -1 ? trimmed : trimmed.substring(0, idx);
        if (segment.isEmpty()) {
            return null;
        }
        if (isSingleWrappedInBraces(segment)) {
            // Already tagged (or raw content shaped exactly like a non-empty tag, see Javadoc) - reuse
            // the inner content verbatim so re-deriving the tag a second time is a true no-op.
            return segment.substring(1, segment.length() - 1);
        }
        // Hash tag delimiters must not appear literally inside the tag value itself, but must not be
        // discarded either - escape them instead (see field comments above).
        return escapeBraces(segment);
    }

    /**
     * Escapes {@code '{'}, {@code '}'} and any literal occurrence of {@link #ESCAPE_MARKER} itself
     * using a JSON-Pointer-style scheme ({@code ~0}/{@code ~1}/{@code ~2}-equivalent, here using
     * {@link #ESCAPE_MARKER} as the marker instead of {@code '~'}). Escaping the marker character
     * first (before using it to encode braces) is what makes this scheme genuinely injective - no two
     * distinct raw segments can ever produce the same escaped output, regardless of what characters
     * the raw segment happens to already contain. A naive fixed one-for-one character substitution
     * (replacing '{'/'}' directly with fixed placeholder characters, without also escaping literal
     * occurrences of those placeholders) would NOT have this guarantee.
     */
    private static String escapeBraces(String segment) {
        StringBuilder sb = new StringBuilder(segment.length());
        for (int i = 0; i < segment.length(); i++) {
            char c = segment.charAt(i);
            if (c == ESCAPE_MARKER) {
                sb.append(ESCAPE_MARKER).append('0');
            } else if (c == '{') {
                sb.append(ESCAPE_MARKER).append('1');
            } else if (c == '}') {
                sb.append(ESCAPE_MARKER).append('2');
            } else {
                sb.append(c);
            }
        }
        return sb.toString();
    }

    /**
     * Inverse of {@link #escapeBraces(String)}: recovers the original raw path segment from an escaped
     * partition tag (e.g. as stored in the partition registry), so it can be displayed as the correct
     * resource name (e.g. in a root collection listing) instead of the internal escaped form.
     *
     * <p>Package-private (rather than private) so {@code RedisStorage} can use it directly when turning
     * a registered partition tag back into the resource name to display, without duplicating the escape
     * scheme's decoding logic.</p>
     */
    static String unescapeBraces(String tag) {
        StringBuilder sb = new StringBuilder(tag.length());
        for (int i = 0; i < tag.length(); i++) {
            char c = tag.charAt(i);
            if (c == ESCAPE_MARKER && i + 1 < tag.length()) {
                char next = tag.charAt(i + 1);
                if (next == '0') {
                    sb.append(ESCAPE_MARKER);
                    i++;
                    continue;
                } else if (next == '1') {
                    sb.append('{');
                    i++;
                    continue;
                } else if (next == '2') {
                    sb.append('}');
                    i++;
                    continue;
                }
            }
            sb.append(c);
        }
        return sb.toString();
    }

    /**
     * True when {@code segment} starts with {@code '{'}, ends with {@code '}'}, has no other
     * {@code '{'}/{@code '}'} in between, and has non-empty inner content - i.e. it is already a
     * single, well-formed, non-empty Redis Cluster hash tag rather than raw content that merely
     * contains stray brace characters (or an empty {@code "{}"}, which {@link #forPath} never itself
     * produces and is therefore always genuinely raw content).
     */
    private static boolean isSingleWrappedInBraces(String segment) {
        if (segment.length() < 3 || segment.charAt(0) != '{' || segment.charAt(segment.length() - 1) != '}') {
            return false;
        }
        String inner = segment.substring(1, segment.length() - 1);
        return inner.indexOf('{') == -1 && inner.indexOf('}') == -1;
    }

    /**
     * Builds the {@link PartitionContext} to use for one storage operation on the given encoded path.
     * When {@code partitioningEnabled} is {@code false}, the key and expirable-set name are returned
     * unchanged, preserving the original (non-cluster-safe) key layout for full backward compatibility.
     *
     * <p><b>Root path stays untagged, on purpose:</b> {@code put.lua}/{@code del.lua} register every
     * top-level path segment (e.g. the {@code {project}} hash tag itself) as a member of the single,
     * global, untagged {@code collectionsPrefix} key (no path suffix at all) - that registration falls
     * out of their generic "split KEYS[1] on ':' and walk ancestors" logic, which always yields an empty
     * first path segment because {@code KEYS[1]} always starts with a leading separator. Giving the root
     * path its own reserved hash tag here (so {@code KEYS[1]} for an explicit {@code GET}/{@code DELETE}
     * on {@code /} becomes e.g. {@code :{root}}) would make root operations look up a key that is
     * different from - and never populated by - that shared registration, breaking root listing/delete
     * (confirmed by the existing root-path CRUD integration tests). Correctly closing this gap requires
     * reworking how the root listing is assembled (effectively a scatter/gather across every registered
     * partition tag, similar to {@link RedisStorage#cleanup}), not just this method; until that is done,
     * root keeps the pre-partitioning (untagged) layout, and remains subject to the Cluster-routing
     * caveat described on {@link #getTag()}.</p>
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
        // on tag.length(): although the current escaping is length-preserving (1 char -> 1 char), that
        // is an implementation detail of derivePartitionTag() this method must not depend on.
        int segmentEnd = encodedPath.indexOf(PATH_SEP, leadingSeps);
        if (segmentEnd == -1) {
            segmentEnd = encodedPath.length();
        }
        String taggedKey = encodedPath.substring(0, leadingSeps) + "{" + tag + "}"
                + encodedPath.substring(segmentEnd);
        String taggedExpirableSet = expirableSet + ":{" + tag + "}";
        boolean topLevel = segmentEnd == encodedPath.length();
        return new PartitionContext(taggedKey, taggedExpirableSet, tag, topLevel);
    }
}
