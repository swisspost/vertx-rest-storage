package org.swisspush.reststorage.mocks;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.file.*;

import java.util.List;


public class DelegatingVertxFileSystem implements FileSystem {

    private final FileSystem delegate;

    public DelegatingVertxFileSystem(FileSystem delegate) {
        this.delegate = delegate;
    }

    @Override
    public FileSystem copy(String s, String s1, Handler<AsyncResult<Void>> handler) {
        return delegate.copy(s, s1, handler);
    }

    @Override
    public Future<Void> copy(String s, String s1) {
        return delegate.copy(s, s1);
    }

    @Override
    public FileSystem copy(String s, String s1, CopyOptions copyOptions, Handler<AsyncResult<Void>> handler) {
        return delegate.copy(s, s1, copyOptions, handler);
    }

    @Override
    public Future<Void> copy(String s, String s1, CopyOptions options) {
        return delegate.copy(s, s1);
    }

    @Override
    public FileSystem copyBlocking(String s, String s1) {
        return delegate.copyBlocking(s, s1);
    }

    @Override
    public FileSystem copyRecursive(String s, String s1, boolean b, Handler<AsyncResult<Void>> handler) {
        return delegate.copyRecursive(s, s1, b, handler);
    }

    @Override
    public Future<Void> copyRecursive(String from, String to, boolean recursive) {
        return delegate.copyRecursive(from, to, recursive);
    }

    @Override
    public FileSystem copyRecursiveBlocking(String s, String s1, boolean b) {
        return delegate.copyRecursiveBlocking(s, s1, b);
    }

    @Override
    public FileSystem move(String s, String s1, Handler<AsyncResult<Void>> handler) {
        return delegate.move(s, s1, handler);
    }

    @Override
    public Future<Void> move(String from, String to) {
        return delegate.move(from, to);
    }

    @Override
    public FileSystem move(String s, String s1, CopyOptions copyOptions, Handler<AsyncResult<Void>> handler) {
        return delegate.move(s, s1, copyOptions, handler);
    }

    @Override
    public Future<Void> move(String from, String to, CopyOptions options) {
        return delegate.move(from, to);
    }

    @Override
    public FileSystem moveBlocking(String s, String s1) {
        return delegate.moveBlocking(s, s1);
    }

    @Override
    public FileSystem truncate(String s, long l, Handler<AsyncResult<Void>> handler) {
        return delegate.truncate(s, l, handler);
    }

    @Override
    public Future<Void> truncate(String path, long len) {
        return delegate.truncate(path, len);
    }

    @Override
    public FileSystem truncateBlocking(String s, long l) {
        return delegate.truncateBlocking(s, l);
    }

    @Override
    public FileSystem chmod(String s, String s1, Handler<AsyncResult<Void>> handler) {
        return delegate.chmod(s, s1, handler);
    }

    @Override
    public Future<Void> chmod(String path, String perms) {
        return delegate.chmod(path, perms);
    }

    @Override
    public FileSystem chmodBlocking(String s, String s1) {
        return delegate.chmodBlocking(s, s1);
    }

    @Override
    public FileSystem chmodRecursive(String s, String s1, String s2, Handler<AsyncResult<Void>> handler) {
        return delegate.chmodRecursive(s, s1, s2, handler);
    }

    @Override
    public Future<Void> chmodRecursive(String path, String perms, String dirPerms) {
        return delegate.chmodRecursive(path, perms, dirPerms);
    }

    @Override
    public FileSystem chmodRecursiveBlocking(String s, String s1, String s2) {
        return delegate.chmodRecursiveBlocking(s, s1, s2);
    }

    @Override
    public FileSystem chown(String s, String s1, String s2, Handler<AsyncResult<Void>> handler) {
        return delegate.chown(s, s1, s2, handler);
    }

    @Override
    public Future<Void> chown(String path, String user, String group) {
        return delegate.chown(path, user, group);
    }

    @Override
    public FileSystem chownBlocking(String s, String s1, String s2) {
        return delegate.chownBlocking(s, s1, s2);
    }

    @Override
    public FileSystem props(String s, Handler<AsyncResult<FileProps>> handler) {
        return delegate.props(s, handler);
    }

    @Override
    public Future<FileProps> props(String path) {
        return delegate.props(path);
    }

    @Override
    public FileProps propsBlocking(String s) {
        return delegate.propsBlocking(s);
    }

    @Override
    public FileSystem lprops(String s, Handler<AsyncResult<FileProps>> handler) {
        return delegate.lprops(s, handler);
    }

    @Override
    public Future<FileProps> lprops(String path) {
        return delegate.lprops(path);
    }

    @Override
    public FileProps lpropsBlocking(String s) {
        return delegate.lpropsBlocking(s);
    }

    @Override
    public FileSystem link(String s, String s1, Handler<AsyncResult<Void>> handler) {
        return delegate.link(s, s1, handler);
    }

    @Override
    public Future<Void> link(String link, String existing) {
        return delegate.link(link, existing);
    }

    @Override
    public FileSystem linkBlocking(String s, String s1) {
        return delegate.linkBlocking(s, s1);
    }

    @Override
    public FileSystem symlink(String s, String s1, Handler<AsyncResult<Void>> handler) {
        return delegate.symlink(s, s1, handler);
    }

    @Override
    public Future<Void> symlink(String link, String existing) {
        return delegate.symlink(link, existing);
    }

    @Override
    public FileSystem symlinkBlocking(String s, String s1) {
        return delegate.symlinkBlocking(s, s1);
    }

    @Override
    public FileSystem unlink(String s, Handler<AsyncResult<Void>> handler) {
        return delegate.unlink(s, handler);
    }

    @Override
    public Future<Void> unlink(String link) {
        return delegate.unlink(link);
    }

    @Override
    public FileSystem unlinkBlocking(String s) {
        return delegate.unlinkBlocking(s);
    }

    @Override
    public FileSystem readSymlink(String s, Handler<AsyncResult<String>> handler) {
        return delegate.readSymlink(s, handler);
    }

    @Override
    public Future<String> readSymlink(String link) {
        return delegate.readSymlink(link);
    }

    @Override
    public String readSymlinkBlocking(String s) {
        return delegate.readSymlinkBlocking(s);
    }

    @Override
    public FileSystem delete(String s, Handler<AsyncResult<Void>> handler) {
        return delegate.delete(s, handler);
    }

    @Override
    public Future<Void> delete(String path) {
        return delegate.delete(path);
    }

    @Override
    public FileSystem deleteBlocking(String s) {
        return delegate.deleteBlocking(s);
    }

    @Override
    public FileSystem deleteRecursive(String s, boolean b, Handler<AsyncResult<Void>> handler) {
        return delegate.deleteRecursive(s, b, handler);
    }

    @Override
    public Future<Void> deleteRecursive(String path, boolean recursive) {
        return null;
    }

    @Override
    public FileSystem deleteRecursiveBlocking(String s, boolean b) {
        return delegate.deleteRecursiveBlocking(s, b);
    }

    @Override
    public FileSystem mkdir(String s, Handler<AsyncResult<Void>> handler) {
        return delegate.mkdir(s, handler);
    }

    @Override
    public Future<Void> mkdir(String path) {
        return delegate.mkdir(path);
    }

    @Override
    public FileSystem mkdirBlocking(String s) {
        return delegate.mkdirBlocking(s);
    }

    @Override
    public FileSystem mkdir(String s, String s1, Handler<AsyncResult<Void>> handler) {
        return delegate.mkdir(s, s1, handler);
    }

    @Override
    public Future<Void> mkdir(String path, String perms) {
        return delegate.mkdir(path, perms);
    }

    @Override
    public FileSystem mkdirBlocking(String s, String s1) {
        return delegate.mkdirBlocking(s, s1);
    }

    @Override
    public FileSystem mkdirs(String s, Handler<AsyncResult<Void>> handler) {
        return delegate.mkdirs(s, handler);
    }

    @Override
    public Future<Void> mkdirs(String path) {
        return delegate.mkdirs(path);
    }

    @Override
    public FileSystem mkdirsBlocking(String s) {
        return delegate.mkdirsBlocking(s);
    }

    @Override
    public FileSystem mkdirs(String s, String s1, Handler<AsyncResult<Void>> handler) {
        return delegate.mkdirs(s, s1, handler);
    }

    @Override
    public Future<Void> mkdirs(String path, String perms) {
        return delegate.mkdirs(path, perms);
    }

    @Override
    public FileSystem mkdirsBlocking(String s, String s1) {
        return delegate.mkdirsBlocking(s, s1);
    }

    @Override
    public FileSystem readDir(String s, Handler<AsyncResult<List<String>>> handler) {
        return delegate.readDir(s, handler);
    }

    @Override
    public Future<List<String>> readDir(String path) {
        return delegate.readDir(path);
    }

    @Override
    public List<String> readDirBlocking(String s) {
        return delegate.readDirBlocking(s);
    }

    @Override
    public FileSystem readDir(String s, String s1, Handler<AsyncResult<List<String>>> handler) {
        return delegate.readDir(s, s1, handler);
    }

    @Override
    public Future<List<String>> readDir(String path, String filter) {
        return delegate.readDir(path, filter);
    }

    @Override
    public List<String> readDirBlocking(String s, String s1) {
        return delegate.readDirBlocking(s, s1);
    }

    @Override
    public FileSystem readFile(String s, Handler<AsyncResult<Buffer>> handler) {
        return delegate.readFile(s, handler);
    }

    @Override
    public Future<Buffer> readFile(String path) {
        return delegate.readFile(path);
    }

    @Override
    public Buffer readFileBlocking(String s) {
        return delegate.readFileBlocking(s);
    }

    @Override
    public FileSystem writeFile(String s, Buffer buffer, Handler<AsyncResult<Void>> handler) {
        return delegate.writeFile(s, buffer, handler);
    }

    @Override
    public Future<Void> writeFile(String path, Buffer data) {
        return delegate.writeFile(path, data);
    }

    @Override
    public FileSystem writeFileBlocking(String s, Buffer buffer) {
        return delegate.writeFileBlocking(s, buffer);
    }

    @Override
    public FileSystem open(String s, OpenOptions openOptions, Handler<AsyncResult<AsyncFile>> handler) {
        return delegate.open(s, openOptions, handler);
    }

    @Override
    public Future<AsyncFile> open(String path, OpenOptions options) {
        return delegate.open(path, options);
    }

    @Override
    public AsyncFile openBlocking(String s, OpenOptions openOptions) {
        return delegate.openBlocking(s, openOptions);
    }

    @Override
    public FileSystem createFile(String s, Handler<AsyncResult<Void>> handler) {
        return delegate.createFile(s, handler);
    }

    @Override
    public Future<Void> createFile(String path) {
        return delegate.createFile(path);
    }

    @Override
    public FileSystem createFileBlocking(String s) {
        return delegate.createFileBlocking(s);
    }

    @Override
    public FileSystem createFile(String s, String s1, Handler<AsyncResult<Void>> handler) {
        return delegate.createFile(s, s1, handler);
    }

    @Override
    public Future<Void> createFile(String path, String perms) {
        return delegate.createFile(path, perms);
    }

    @Override
    public FileSystem createFileBlocking(String s, String s1) {
        return delegate.createFileBlocking(s, s1);
    }

    @Override
    public FileSystem exists(String s, Handler<AsyncResult<Boolean>> handler) {
        return delegate.exists(s, handler);
    }

    @Override
    public Future<Boolean> exists(String path) {
        return delegate.exists(path);
    }

    @Override
    public boolean existsBlocking(String s) {
        return delegate.existsBlocking(s);
    }

    @Override
    public FileSystem fsProps(String s, Handler<AsyncResult<FileSystemProps>> handler) {
        return delegate.fsProps(s, handler);
    }

    @Override
    public Future<FileSystemProps> fsProps(String path) {
        return delegate.fsProps(path);
    }

    @Override
    public FileSystemProps fsPropsBlocking(String s) {
        return delegate.fsPropsBlocking(s);
    }

    @Override
    public FileSystem createTempDirectory(String prefix, Handler<AsyncResult<String>> handler) {
        return delegate.createTempDirectory(prefix, handler);
    }

    @Override
    public Future<String> createTempDirectory(String prefix) {
        return delegate.createTempDirectory(prefix);
    }

    @Override
    public String createTempDirectoryBlocking(String prefix) {
        return delegate.createTempDirectoryBlocking(prefix);
    }

    @Override
    public FileSystem createTempDirectory(String prefix, String perms, Handler<AsyncResult<String>> handler) {
        return delegate.createTempDirectory(prefix, perms, handler);
    }

    @Override
    public Future<String> createTempDirectory(String prefix, String perms) {
        return delegate.createTempDirectory(prefix, perms);
    }

    @Override
    public String createTempDirectoryBlocking(String prefix, String perms) {
        return delegate.createTempDirectoryBlocking(prefix, perms);
    }

    @Override
    public FileSystem createTempDirectory(String dir, String prefix, String perms, Handler<AsyncResult<String>> handler) {
        return delegate.createTempDirectory(dir, prefix, perms, handler);
    }

    @Override
    public Future<String> createTempDirectory(String dir, String prefix, String perms) {
        return delegate.createTempDirectory(dir, prefix, perms);
    }

    @Override
    public String createTempDirectoryBlocking(String dir, String prefix, String perms) {
        return delegate.createTempDirectoryBlocking(dir, prefix, perms);
    }

    @Override
    public FileSystem createTempFile(String prefix, String suffix, Handler<AsyncResult<String>> handler) {
        return delegate.createTempFile(prefix, suffix, handler);
    }

    @Override
    public Future<String> createTempFile(String prefix, String suffix) {
        return delegate.createTempFile(prefix, suffix);
    }

    @Override
    public String createTempFileBlocking(String prefix, String suffix) {
        return delegate.createTempFileBlocking(prefix, suffix);
    }

    @Override
    public FileSystem createTempFile(String prefix, String suffix, String perms, Handler<AsyncResult<String>> handler) {
        return delegate.createTempFile(prefix, suffix, perms, handler);
    }

    @Override
    public Future<String> createTempFile(String prefix, String suffix, String perms) {
        return delegate.createTempFile(prefix, suffix, perms);
    }

    @Override
    public String createTempFileBlocking(String prefix, String suffix, String perms) {
        return delegate.createTempFileBlocking(prefix, suffix, perms);
    }

    @Override
    public FileSystem createTempFile(String dir, String prefix, String suffix, String perms, Handler<AsyncResult<String>> handler) {
        return delegate.createTempFile(dir, prefix, suffix, perms, handler);
    }

    @Override
    public Future<String> createTempFile(String dir, String prefix, String suffix, String perms) {
        return delegate.createTempFile(dir, prefix, suffix, perms);
    }

    @Override
    public String createTempFileBlocking(String dir, String prefix, String suffix, String perms) {
        return delegate.createTempFileBlocking(dir, prefix, suffix, perms);
    }
}
