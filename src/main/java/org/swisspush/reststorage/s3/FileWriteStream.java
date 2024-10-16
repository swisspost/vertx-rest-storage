package org.swisspush.reststorage.s3;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.streams.WriteStream;

import java.io.OutputStream;
import java.io.IOException;

public class FileWriteStream implements WriteStream<Buffer> {
    private final OutputStream outputStream;
    private Handler<Throwable> exceptionHandler;
    private Handler<Void> drainHandler;
    private boolean writeQueueFull = false;
    private int maxQueueSize = 1024;  // Default max queue size, can be modified

    public FileWriteStream(OutputStream outputStream) {
        this.outputStream = outputStream;
    }

    @Override
    public WriteStream<Buffer> exceptionHandler(Handler<Throwable> handler) {
        this.exceptionHandler = handler;
        return this;
    }

    @Override
    public void write(Buffer buffer, Handler<AsyncResult<Void>> handler) {
        try {
            outputStream.write(buffer.getBytes());
            outputStream.flush();
            handler.handle(Future.succeededFuture());
        } catch (IOException e) {
            if (exceptionHandler != null) {
                exceptionHandler.handle(e);
            }
            handler.handle(Future.failedFuture(e));
        }
    }

    @Override
    public Future<Void> write(Buffer buffer) {
        Promise<Void> promise = Promise.promise();
        try {
            outputStream.write(buffer.getBytes());
            outputStream.flush();
            promise.complete();
        } catch (IOException e) {
            if (exceptionHandler != null) {
                exceptionHandler.handle(e);
            }
            promise.fail(e);
        }
        return promise.future();
    }

    @Override
    public void end(Handler<AsyncResult<Void>> handler) {
        try {
            outputStream.flush();
            outputStream.close();
            handler.handle(Future.succeededFuture());
        } catch (IOException e) {
            if (exceptionHandler != null) {
                exceptionHandler.handle(e);
            }
            handler.handle(Future.failedFuture(e));
        }
    }

    @Override
    public Future<Void> end() {
        Promise<Void> promise = Promise.promise();
        try {
            outputStream.flush();
            outputStream.close();
            promise.complete();
        } catch (IOException e) {
            if (exceptionHandler != null) {
                exceptionHandler.handle(e);
            }
            promise.fail(e);
        }
        return promise.future();
    }

    @Override
    public WriteStream<Buffer> setWriteQueueMaxSize(int maxSize) {
        this.maxQueueSize = maxSize;
        return this;
    }

    @Override
    public boolean writeQueueFull() {
        // Implement custom logic to manage queue size if needed
        return writeQueueFull;
    }

    @Override
    public WriteStream<Buffer> drainHandler(Handler<Void> handler) {
        this.drainHandler = handler;
        return this;
    }
}
