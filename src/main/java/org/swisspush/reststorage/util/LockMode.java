package org.swisspush.reststorage.util;

/**
 * Enum for all implemented
 * Lock Modes.
 *
 * @author https://github.com/ljucam [Mario Ljuca]
 *
 */
public enum LockMode {
    SILENT("silent"),
    REJECT("reject");

    private final String lockMode;

    LockMode(String lockMode) {
        this.lockMode = lockMode;
    }

    public String text() {
        return lockMode;
    }
}
