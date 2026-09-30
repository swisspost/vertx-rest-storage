package org.swisspush.reststorage.migration.tasks;

import io.vertx.core.Future;

/**
 * Interface for migration tasks to be executed by MigrateTool.
 * Implementations should define specific migration logic.
 */
public interface Task {
    /**
     * @return unique identifier for this task
     */
    String getTaskKey();

    /**
     * Executes the migration task.
     * @return a Future that completes with true if the task succeeded, false otherwise
     */
    Future<Boolean> run();

    /**
     * Reports whether this task has already completed successfully (e.g. by checking its own
     * permanent completion flag in Redis), independently of {@link #run()} ever having been invoked
     * on this instance.
     *
     * <p>Used by {@code MigrateTool} to verify a genuinely finished migration when a <em>waiting</em>
     * instance observes the distributed migration lock disappear: the lock going away is not, by
     * itself, proof of success - if the lock-holding instance crashed hard (e.g. was OOM-killed) mid
     * task, no {@code :FAILED} marker is ever written and the lock simply expires via its TTL, which
     * would otherwise be indistinguishable from a normal, successful release. Checking this lets the
     * waiting instance tell those two cases apart instead of silently proceeding against a
     * partially-migrated data set.</p>
     *
     * @return a Future that completes with true only if this task's own completion flag is set
     */
    Future<Boolean> isDone();
}
