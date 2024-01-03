package org.swisspush.reststorage;

import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.file.AsyncFile;
import io.vertx.core.file.impl.AsyncFileImpl;
import io.vertx.core.streams.ReadStream;
import org.slf4j.Logger;

import static org.slf4j.LoggerFactory.getLogger;


/**
 * Decorator with the purpose to log what's going on in a reading stream of an
 * {@link AsyncFile}.
 */
public class LoggingFileReadStream<T> implements ReadStream<T> {

    private static final Logger log = getLogger(LoggingFileReadStream.class);
    private final long expectedSize;
    private final String path;
    private final AsyncFile delegate;
    private long writtenBytes = 0;

    /**
     * @param expectedSize Actual file size which is expected to be streamed through that stream
     *                     in bytes.
     * @param path         Token printed alongside the logs so when reading logs, we can see which
     *                     log belongs to which file. A possible candidate is to use the file path
     *                     but it theoretically can be anything  which  helps  you  to  find  logs
     *                     related to your observed file.
     * @param delegate     The file (or stream) we wanna observe.
     */
    LoggingFileReadStream(long expectedSize, String path, AsyncFile delegate) {
        this.expectedSize = expectedSize;
        this.path = path;
        this.delegate = delegate;
    }

    @Override
    public ReadStream<T> exceptionHandler(Handler<Throwable> handler) {
        log.trace("exceptionHandler registered for reading '{}'", path);
        delegate.exceptionHandler( ex -> {
            log.debug("Got an exception at offset {} ({} bytes remaining) for '{}'",
                    writtenBytes, expectedSize - writtenBytes, path, ex);
            handler.handle(ex);
        });
        return this;
    }

    @Override
    public ReadStream<T> handler(Handler handler) {
        log.trace("handler registered");
        delegate.handler(buf -> {
            if (weShouldLogThatChunk(buf)) {
                log.debug("Read {} bytes at offset {} of total {} from '{}'",
                        buf.length(), writtenBytes, expectedSize, path);
            }
            writtenBytes += buf.length();
            handler.handle(buf);
        });
        return this;
    }

    @Override
    public ReadStream<T> pause() {
        log.debug("Pause reading at offset {} for '{}'", writtenBytes, path);
        delegate.pause();
        return this;
    }

    @Override
    public ReadStream<T> resume() {
        log.debug("Resume reading at offset {} for '{}'", writtenBytes, path);
        delegate.resume();
        return this;
    }

    @Override
    public ReadStream fetch(long amount) {
        log.debug("fetch amount {}", amount);
        return delegate.fetch(amount);
    }

    @Override
    public ReadStream<T> endHandler(Handler<Void> endHandler) {
        log.trace("endHandler registered.");
        delegate.endHandler(aVoid -> {
            log.debug("End handler called ({} bytes remaining) for '{}'", expectedSize - writtenBytes, path);
            endHandler.handle(aVoid);
        });
        return this;
    }

    /**
     * Determines if it is worth writing some details to the logs for that chunk.
     */
    private boolean weShouldLogThatChunk(Buffer buf) {

        if (log.isTraceEnabled()) {
            // Simply log everything.
            return true;
        }

        // Because trace is disabled, we only log near begin or end of  stream  to  not
        // flood the log too much. Especially  for  large  files  which  would  produce
        // hundreds of lines of output.

        if (writtenBytes <= AsyncFileImpl.DEFAULT_READ_BUFFER_SIZE) {
            // We'll log near the beginning.
            return true;
        }

        if ((expectedSize - writtenBytes - buf.length()) < AsyncFileImpl.DEFAULT_READ_BUFFER_SIZE) {
            // We'll log near the end.
            return true;
        }

        // Neither verbosity is configured, nor are we at a interesting position in the stream.
        return false;
    }
}
