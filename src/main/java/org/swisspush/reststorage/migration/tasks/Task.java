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
}
