package org.swisspush.reststorage.mocks;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.file.AsyncFile;


public class FailFastVertxAsyncFile implements AsyncFile {

    protected final String msg;

    public FailFastVertxAsyncFile() {
        this("Override this to provide your behaviour.");
    }

    public FailFastVertxAsyncFile(String msg) {
        this.msg = msg;
    }

    @Override
    public AsyncFile handler(Handler<Buffer> handler) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public AsyncFile pause() {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public AsyncFile resume() {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public AsyncFile endHandler(Handler<Void> handler) {
        throw new UnsupportedOperationException(msg);
    }


    @Override
    public AsyncFile setWriteQueueMaxSize(int i) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public boolean writeQueueFull() {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public AsyncFile drainHandler(Handler<Void> handler) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public AsyncFile exceptionHandler(Handler<Throwable> handler) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public Future<Void> write(Buffer data) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public void write(Buffer data, Handler<AsyncResult<Void>> handler) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public void end(Handler<AsyncResult<Void>> handler) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public AsyncFile fetch(long amount) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public Future<Void> close() {
        throw new UnsupportedOperationException(msg);
    }


    @Override
    public void close(Handler<AsyncResult<Void>> handler) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public void write(Buffer buffer, long position, Handler<AsyncResult<Void>> handler) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public Future<Void> write(Buffer buffer, long position) {
        throw new UnsupportedOperationException(msg);
    }


    @Override
    public AsyncFile read(Buffer buffer, int i, long l, int i1, Handler<AsyncResult<Buffer>> handler) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public Future<Buffer> read(Buffer buffer, int offset, long position, int length) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public Future<Void> flush() {
        throw new UnsupportedOperationException(msg);
    }


    @Override
    public AsyncFile flush(Handler<AsyncResult<Void>> handler) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public AsyncFile setReadPos(long l) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public AsyncFile setReadLength(long readLength) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public long getReadLength() {
        return 0;
    }

    @Override
    public AsyncFile setWritePos(long l) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public long getWritePos() {
        return 0;
    }

    @Override
    public AsyncFile setReadBufferSize(int i) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public long sizeBlocking() {
        return 0;
    }

    @Override
    public Future<Long> size() {
        throw new UnsupportedOperationException(msg);
    }
}
