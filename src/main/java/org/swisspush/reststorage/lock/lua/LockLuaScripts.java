package org.swisspush.reststorage.lock.lua;


/**
 * @author https://github.com/mcweba [Marc-Andre Weber]
 */
public enum LockLuaScripts implements LuaScript {

    LOCK_RELEASE("lock_release.lua");

    private final String file;

    LockLuaScripts(String file) {
        this.file = file;
    }

    @Override
    public String getFilename() {
        return file;
    }
}
