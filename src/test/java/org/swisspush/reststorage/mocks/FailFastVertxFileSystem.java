package org.swisspush.reststorage.mocks;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.file.*;

import java.util.List;


public class FailFastVertxFileSystem implements FileSystem {

    protected final String msg;

    public FailFastVertxFileSystem() {
        this("Mock method not implemented. Override to provide your expected behaviour.");
    }

    public FailFastVertxFileSystem(String msg) {
        this.msg = msg;
    }

    @Override
    public FileSystem copy(String s, String s1, Handler<AsyncResult<Void>> handler) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public Future<Void> copy(String from, String to) {
        return null;
    }

    @Override
    public FileSystem copy(String s, String s1, CopyOptions copyOptions, Handler<AsyncResult<Void>> handler) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public Future<Void> copy(String from, String to, CopyOptions options) {
        return null;
    }

    @Override
    public FileSystem copyBlocking(String s, String s1) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public FileSystem copyRecursive(String s, String s1, boolean b, Handler<AsyncResult<Void>> handler) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public Future<Void> copyRecursive(String from, String to, boolean recursive) {
        return null;
    }

    @Override
    public FileSystem copyRecursiveBlocking(String s, String s1, boolean b) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public FileSystem move(String s, String s1, Handler<AsyncResult<Void>> handler) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public Future<Void> move(String from, String to) {
        return null;
    }

    @Override
    public FileSystem move(String s, String s1, CopyOptions copyOptions, Handler<AsyncResult<Void>> handler) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public Future<Void> move(String from, String to, CopyOptions options) {
        return null;
    }

    @Override
    public FileSystem moveBlocking(String s, String s1) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public FileSystem truncate(String s, long l, Handler<AsyncResult<Void>> handler) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public Future<Void> truncate(String path, long len) {
        return null;
    }

    @Override
    public FileSystem truncateBlocking(String s, long l) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public FileSystem chmod(String s, String s1, Handler<AsyncResult<Void>> handler) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public Future<Void> chmod(String path, String perms) {
        return null;
    }

    @Override
    public FileSystem chmodBlocking(String s, String s1) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public FileSystem chmodRecursive(String s, String s1, String s2, Handler<AsyncResult<Void>> handler) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public Future<Void> chmodRecursive(String path, String perms, String dirPerms) {
        return null;
    }

    @Override
    public FileSystem chmodRecursiveBlocking(String s, String s1, String s2) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public FileSystem chown(String s, String s1, String s2, Handler<AsyncResult<Void>> handler) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public Future<Void> chown(String path, String user, String group) {
        return null;
    }

    @Override
    public FileSystem chownBlocking(String s, String s1, String s2) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public FileSystem props(String s, Handler<AsyncResult<FileProps>> handler) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public Future<FileProps> props(String path) {
        return null;
    }

    @Override
    public FileProps propsBlocking(String s) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public FileSystem lprops(String s, Handler<AsyncResult<FileProps>> handler) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public Future<FileProps> lprops(String path) {
        return null;
    }

    @Override
    public FileProps lpropsBlocking(String s) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public FileSystem link(String s, String s1, Handler<AsyncResult<Void>> handler) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public Future<Void> link(String link, String existing) {
        return null;
    }

    @Override
    public FileSystem linkBlocking(String s, String s1) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public FileSystem symlink(String s, String s1, Handler<AsyncResult<Void>> handler) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public Future<Void> symlink(String link, String existing) {
        return null;
    }

    @Override
    public FileSystem symlinkBlocking(String s, String s1) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public FileSystem unlink(String s, Handler<AsyncResult<Void>> handler) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public Future<Void> unlink(String link) {
        return null;
    }

    @Override
    public FileSystem unlinkBlocking(String s) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public FileSystem readSymlink(String s, Handler<AsyncResult<String>> handler) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public Future<String> readSymlink(String link) {
        return null;
    }

    @Override
    public String readSymlinkBlocking(String s) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public FileSystem delete(String s, Handler<AsyncResult<Void>> handler) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public Future<Void> delete(String path) {
        return null;
    }

    @Override
    public FileSystem deleteBlocking(String s) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public FileSystem deleteRecursive(String s, boolean b, Handler<AsyncResult<Void>> handler) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public Future<Void> deleteRecursive(String path, boolean recursive) {
        return null;
    }

    @Override
    public FileSystem deleteRecursiveBlocking(String s, boolean b) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public FileSystem mkdir(String s, Handler<AsyncResult<Void>> handler) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public Future<Void> mkdir(String path) {
        return null;
    }

    @Override
    public FileSystem mkdirBlocking(String s) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public FileSystem mkdir(String s, String s1, Handler<AsyncResult<Void>> handler) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public Future<Void> mkdir(String path, String perms) {
        return null;
    }

    @Override
    public FileSystem mkdirBlocking(String s, String s1) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public FileSystem mkdirs(String s, Handler<AsyncResult<Void>> handler) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public Future<Void> mkdirs(String path) {
        return null;
    }

    @Override
    public FileSystem mkdirsBlocking(String s) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public FileSystem mkdirs(String s, String s1, Handler<AsyncResult<Void>> handler) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public Future<Void> mkdirs(String path, String perms) {
        return null;
    }

    @Override
    public FileSystem mkdirsBlocking(String s, String s1) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public FileSystem readDir(String s, Handler<AsyncResult<List<String>>> handler) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public Future<List<String>> readDir(String path) {
        return null;
    }

    @Override
    public List<String> readDirBlocking(String s) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public FileSystem readDir(String s, String s1, Handler<AsyncResult<List<String>>> handler) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public Future<List<String>> readDir(String path, String filter) {
        return null;
    }

    @Override
    public List<String> readDirBlocking(String s, String s1) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public FileSystem readFile(String s, Handler<AsyncResult<Buffer>> handler) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public Future<Buffer> readFile(String path) {
        return null;
    }

    @Override
    public Buffer readFileBlocking(String s) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public FileSystem writeFile(String s, Buffer buffer, Handler<AsyncResult<Void>> handler) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public Future<Void> writeFile(String path, Buffer data) {
        return null;
    }

    @Override
    public FileSystem writeFileBlocking(String s, Buffer buffer) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public FileSystem open(String s, OpenOptions openOptions, Handler<AsyncResult<AsyncFile>> handler) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public Future<AsyncFile> open(String path, OpenOptions options) {
        return null;
    }

    @Override
    public AsyncFile openBlocking(String s, OpenOptions openOptions) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public FileSystem createFile(String s, Handler<AsyncResult<Void>> handler) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public Future<Void> createFile(String path) {
        return null;
    }

    @Override
    public FileSystem createFileBlocking(String s) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public FileSystem createFile(String s, String s1, Handler<AsyncResult<Void>> handler) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public Future<Void> createFile(String path, String perms) {
        return null;
    }

    @Override
    public FileSystem createFileBlocking(String s, String s1) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public FileSystem exists(String s, Handler<AsyncResult<Boolean>> handler) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public Future<Boolean> exists(String path) {
        return null;
    }

    @Override
    public boolean existsBlocking(String s) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public FileSystem fsProps(String s, Handler<AsyncResult<FileSystemProps>> handler) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public Future<FileSystemProps> fsProps(String path) {
        return null;
    }

    @Override
    public FileSystemProps fsPropsBlocking(String s) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public FileSystem createTempDirectory(String prefix, Handler<AsyncResult<String>> handler) {
        return null;
    }

    @Override
    public Future<String> createTempDirectory(String prefix) {
        return null;
    }

    @Override
    public String createTempDirectoryBlocking(String prefix) {
        return null;
    }

    @Override
    public FileSystem createTempDirectory(String prefix, String perms, Handler<AsyncResult<String>> handler) {
        return null;
    }

    @Override
    public Future<String> createTempDirectory(String prefix, String perms) {
        return null;
    }

    @Override
    public String createTempDirectoryBlocking(String prefix, String perms) {
        return null;
    }

    @Override
    public FileSystem createTempDirectory(String dir, String prefix, String perms, Handler<AsyncResult<String>> handler) {
        return null;
    }

    @Override
    public Future<String> createTempDirectory(String dir, String prefix, String perms) {
        return null;
    }

    @Override
    public String createTempDirectoryBlocking(String dir, String prefix, String perms) {
        return null;
    }

    @Override
    public FileSystem createTempFile(String prefix, String suffix, Handler<AsyncResult<String>> handler) {
        return null;
    }

    @Override
    public Future<String> createTempFile(String prefix, String suffix) {
        return null;
    }

    @Override
    public String createTempFileBlocking(String prefix, String suffix) {
        return null;
    }

    @Override
    public FileSystem createTempFile(String prefix, String suffix, String perms, Handler<AsyncResult<String>> handler) {
        return null;
    }

    @Override
    public Future<String> createTempFile(String prefix, String suffix, String perms) {
        return null;
    }

    @Override
    public String createTempFileBlocking(String prefix, String suffix, String perms) {
        return null;
    }

    @Override
    public FileSystem createTempFile(String dir, String prefix, String suffix, String perms, Handler<AsyncResult<String>> handler) {
        return null;
    }

    @Override
    public Future<String> createTempFile(String dir, String prefix, String suffix, String perms) {
        return null;
    }

    @Override
    public String createTempFileBlocking(String dir, String prefix, String suffix, String perms) {
        return null;
    }
}
