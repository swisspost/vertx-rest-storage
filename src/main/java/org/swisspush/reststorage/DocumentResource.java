package org.swisspush.reststorage;

import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.streams.ReadStream;
import io.vertx.core.streams.WriteStream;

public class DocumentResource extends Resource {
    public long length;
    public String etag;
    public ReadStream<Buffer> readStream;
    public WriteStream<Buffer> writeStream;
    public Handler<Void> closeHandler; // Called by client to close the storage
    public Handler<Void> endHandler; // Called by storage to notify
}
