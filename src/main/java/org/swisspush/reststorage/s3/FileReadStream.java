package org.swisspush.reststorage.s3;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.streams.ReadStream;
import io.vertx.core.streams.impl.InboundBuffer;
import org.slf4j.Logger;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;

import static io.vertx.core.file.impl.AsyncFileImpl.DEFAULT_READ_BUFFER_SIZE;
import static org.slf4j.LoggerFactory.getLogger;


public class FileReadStream<T> implements ReadStream<T>, Closeable {

    private static final Logger log = getLogger(FileReadStream.class);
    private final long expectedSize;
    private final String path;

    private final ReadableByteChannel ch;
    private final Vertx vertx;
    private final Context context;

    private boolean closed;
    private boolean readInProgress;

    private Handler<Buffer> dataHandler;
    private Handler<Void> endHandler;
    private Handler<Throwable> exceptionHandler;
    private final InboundBuffer<Buffer> queue;

    private final int readBufferSize = DEFAULT_READ_BUFFER_SIZE;
    private long writtenBytes = 0;

    /**
     * @param expectedSize Actual file size which is expected to be streamed through that stream
     *                     in bytes.
     * @param path         Token printed alongside the logs so when reading logs, we can see which
     *                     log belongs to which file. A possible candidate is to use the file path
     *                     but it theoretically can be anything  which  helps  you  to  find  logs
     *                     related to your observed file.
     * @param stream       The file (or stream) we wanna observe.
     */
    public FileReadStream(Vertx vertx, long expectedSize, String path, InputStream stream) {
        this.vertx = vertx;
        this.context = vertx.getOrCreateContext();
        this.expectedSize = expectedSize;
        this.path = path;
        this.ch = Channels.newChannel(stream);
        this.queue = new InboundBuffer<>(context, 0);
        queue.handler(buff -> {
            if (buff.length() > 0) {
                handleData(buff);
            } else {
                handleEnd();
            }
        });
        queue.drainHandler(v -> {
            doRead();
        });
    }

    public void close() {
        closeInternal(null);
    }

    public void close(Handler<AsyncResult<Void>> handler) {
        closeInternal(handler);
    }

    @Override
    public ReadStream<T> exceptionHandler(Handler<Throwable> exceptionHandler) {
        log.trace("exceptionHandler registered for reading '{}'", path);
        check();
        this.exceptionHandler = exceptionHandler;
        return this;
    }

    @Override
    public ReadStream<T> handler(Handler handler) {
        log.trace("handler registered");
        check();
        this.dataHandler = handler;
        if (this.dataHandler != null && !this.closed) {
            this.doRead();
        } else {
            queue.clear();
        }
        return this;
    }

    @Override
    public ReadStream<T> pause() {
        log.debug("Pause reading at offset {} for '{}'", writtenBytes, path);
        check();
        queue.pause();
        return this;
    }

    @Override
    public ReadStream<T> resume() {
        log.debug("Resume reading at offset {} for '{}'", writtenBytes, path);
        check();
        if (!closed) {
            queue.resume();
        }
        return this;
    }

    @Override
    public ReadStream<T> fetch(long amount) {
        log.debug("fetch amount {}", amount);
        queue.fetch(amount);
        return this;
    }

    @Override
    public ReadStream<T> endHandler(Handler<Void> endHandler) {
        log.trace("endHandler registered.");
        check();
        this.endHandler = endHandler;
        log.debug("End handler called ({} bytes remaining) for '{}'", expectedSize - writtenBytes, path);
        return this;
    }

    private void doRead() {
        check();
        doRead(ByteBuffer.allocate(readBufferSize));
    }

    private synchronized void doRead(ByteBuffer bb) {
        if (!readInProgress) {
            readInProgress = true;
            Buffer buff = Buffer.buffer(readBufferSize);
            doRead(buff, 0, bb, writtenBytes, ar -> {
                if (ar.succeeded()) {
                    readInProgress = false;
                    Buffer buffer = ar.result();
                    writtenBytes += buffer.length();
                    // Empty buffer represents end of file
                    if (queue.write(buffer) && buffer.length() > 0) {
                        doRead(bb);
                    }
                } else {
                    handleException(ar.cause());
                }
            });
        }
    }

    private void doRead(Buffer writeBuff, int offset, ByteBuffer buff, long position, Handler<AsyncResult<Buffer>> handler) {
        // ReadableByteChannel doesn't have a completion handler, so we wrap it into
        // an executeBlocking and use the future there
        vertx.executeBlocking(future -> {
            try {
                Integer bytesRead = ch.read(buff);
                future.complete(bytesRead);
            } catch (IOException e) {
                log.error("Failed to read data from buffer.", e);
                future.fail(e);
            }

        }, res -> {

            if (res.failed()) {
                log.error("Failed to read data from buffer.", res.cause());
                context.runOnContext((v) -> handler.handle(Future.failedFuture(res.cause())));
            } else {
                // Do the completed check
                Integer bytesRead = (Integer) res.result();
                if (bytesRead == -1) {
                    //End of file
                    context.runOnContext((v) -> {
                        buff.flip();
                        writeBuff.setBytes(offset, buff);
                        buff.compact();
                        handler.handle(Future.succeededFuture(writeBuff));
                    });
                } else if (buff.hasRemaining()) {
                    long pos = position;
                    pos += bytesRead;
                    // resubmit
                    doRead(writeBuff, offset, buff, pos, handler);
                } else {
                    // It's been fully written

                    context.runOnContext((v) -> {
                        buff.flip();
                        writeBuff.setBytes(offset, buff);
                        buff.compact();
                        handler.handle(Future.succeededFuture(writeBuff));
                    });
                }
            }
        });
    }

    private void handleData(Buffer buff) {
        Handler<Buffer> handler;
        synchronized (this) {
            handler = this.dataHandler;
        }
        if (handler != null) {
            checkContext();
            handler.handle(buff);
        }
    }

    private synchronized void handleEnd() {
        Handler<Void> endHandler;
        synchronized (this) {
            dataHandler = null;
            endHandler = this.endHandler;
        }
        if (endHandler != null) {
            checkContext();
            endHandler.handle(null);
        }
    }

    private void handleException(Throwable t) {
        if (exceptionHandler != null && t instanceof Exception) {
            exceptionHandler.handle(t);
        } else {
            log.error("Unhandled exception", t);
        }
    }

    private void check() {
        if (this.closed) {
            throw new IllegalStateException("Inputstream is closed");
        }
    }

    public boolean isClosed() {
        return this.closed;
    }

    private void checkContext() {
        if (!vertx.getOrCreateContext().equals(context)) {
            throw new IllegalStateException("AsyncInputStream must only be used in the context that created it, expected: " + this.context
                    + " actual " + vertx.getOrCreateContext());
        }
    }

    private synchronized void closeInternal(Handler<AsyncResult<Void>> handler) {
        check();
        closed = true;
        doClose(handler);
    }

    private void doClose(Handler<AsyncResult<Void>> handler) {
        try {
            ch.close();
            if (handler != null) {
                this.vertx.runOnContext(v -> handler.handle(Future.succeededFuture()));
            }
        } catch (IOException e) {
            if (handler != null) {
                this.vertx.runOnContext(v -> handler.handle(Future.failedFuture(e)));
            }
        }
    }
}
