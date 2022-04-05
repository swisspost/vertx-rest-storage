package org.swisspush.reststorage;

import io.vertx.core.*;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.DecodeException;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.streams.ReadStream;
import io.vertx.core.streams.WriteStream;
import io.vertx.redis.client.RedisAPI;
import io.vertx.redis.client.RedisOptions;
import io.vertx.redis.client.Response;
import io.vertx.redis.client.impl.RedisClient;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.text.StrSubstitutor;
import org.swisspush.reststorage.util.GZIPUtil;
import org.swisspush.reststorage.util.LockMode;
import org.swisspush.reststorage.util.ModuleConfiguration;
import org.swisspush.reststorage.util.ResourceNameUtil;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.text.DecimalFormat;
import java.util.*;
import java.util.stream.Stream;

public class RedisStorage implements Storage {

    private Logger log = LoggerFactory.getLogger(RedisStorage.class);

    // set to very high value = Wed Nov 16 5138 09:46:39
    private static final String MAX_EXPIRE_IN_MILLIS = "99999999999999";
    private final String EMPTY = "";
    private static final float MAX_PERCENTAGE = 100.0f;
    private static final float MIN_PERCENTAGE = 0.0f;
    private static final int CLEANUP_BULK_SIZE = 200;

    private String redisResourcesPrefix;
    private String redisCollectionsPrefix;
    private String redisDeltaResourcesPrefix;
    private String redisDeltaEtagsPrefix;
    private String expirableSet;
    private long cleanupResourcesAmount;
    private String redisLockPrefix;
    private Vertx vertx;

    private RedisAPI redisAPI;
    private Map<LuaScript, LuaScriptState> luaScripts = new HashMap<>();
    private DecimalFormat decimalFormat;

    private Optional<Float> currentMemoryUsageOptional = Optional.empty();

    public RedisStorage(Vertx vertx, ModuleConfiguration config) {
        this(vertx, config, new RedisClient(vertx,
                new RedisOptions().setConnectionString("redis://" +
                                config.getRedisHost() + ":" + config.getRedisPort())
                        .setPassword((config.getRedisAuth() == null ? "" : config.getRedisAuth()))
                        .setMaxPoolSize(config.getMaxRedisConnectionPoolSize())));
    }

    public RedisStorage(Vertx vertx, ModuleConfiguration config, RedisClient redisClient) {
        this(vertx, config, redisClient, RedisAPI.api(redisClient));
    }


    public RedisStorage(Vertx vertx, ModuleConfiguration config, RedisClient redisClient, RedisAPI redisAPI) {
        this.expirableSet = config.getExpirablePrefix();
        this.redisResourcesPrefix = config.getResourcesPrefix();
        this.redisCollectionsPrefix = config.getCollectionsPrefix();
        this.redisDeltaResourcesPrefix = config.getDeltaResourcesPrefix();
        this.redisDeltaEtagsPrefix = config.getDeltaEtagsPrefix();
        this.cleanupResourcesAmount = config.getResourceCleanupAmount();
        this.redisLockPrefix = config.getLockPrefix();

        this.vertx = vertx;
        this.redisAPI = redisAPI;
        this.decimalFormat = new DecimalFormat();
        this.decimalFormat.setMaximumFractionDigits(1);

        // load all the lua scripts
        LuaScriptState luaGetScriptState = new LuaScriptState(LuaScript.GET, false);
        luaGetScriptState.loadLuaScript(new RedisCommandDoNothing(), 0);
        luaScripts.put(LuaScript.GET, luaGetScriptState);

        LuaScriptState luaStorageExpandScriptState = new LuaScriptState(LuaScript.STORAGE_EXPAND, false);
        luaStorageExpandScriptState.loadLuaScript(new RedisCommandDoNothing(), 0);
        luaScripts.put(LuaScript.STORAGE_EXPAND, luaStorageExpandScriptState);

        LuaScriptState luaPutScriptState = new LuaScriptState(LuaScript.PUT, false);
        luaPutScriptState.loadLuaScript(new RedisCommandDoNothing(), 0);
        luaScripts.put(LuaScript.PUT, luaPutScriptState);

        LuaScriptState luaDeleteScriptState = new LuaScriptState(LuaScript.DELETE, false);
        luaDeleteScriptState.loadLuaScript(new RedisCommandDoNothing(), 0);
        luaScripts.put(LuaScript.DELETE, luaDeleteScriptState);

        LuaScriptState luaCleanupScriptState = new LuaScriptState(LuaScript.CLEANUP, false);
        luaCleanupScriptState.loadLuaScript(new RedisCommandDoNothing(), 0);
        luaScripts.put(LuaScript.CLEANUP, luaCleanupScriptState);

        if (config.isRejectStorageWriteOnLowMemory()) {
            calculateCurrentMemoryUsage().onComplete(optionalAsyncResult -> currentMemoryUsageOptional = optionalAsyncResult.result());
            startPeriodicMemoryUsageUpdate(config.getFreeMemoryCheckIntervalMs());
        }
    }

    private void startPeriodicMemoryUsageUpdate(long intervalMs) {
        vertx.setPeriodic(intervalMs, updateMemoryUsage -> calculateCurrentMemoryUsage().onComplete(optionalAsyncResult -> currentMemoryUsageOptional = optionalAsyncResult.result()));
    }

    public Future<Optional<Float>> calculateCurrentMemoryUsage() {
        Promise<Optional<Float>> promise = Promise.promise();
        redisAPI.info(Collections.singletonList("memory"), memoryInfo -> {
            if (memoryInfo.failed()) {
                log.error("Unable to get memory information from redis", memoryInfo.cause());
                promise.complete(Optional.empty());
                return;
            }

            long totalSystemMemory;
            try {
                Optional<String> totalSystemMemoryOpt = memoryInfo.result().toString()
                        .lines()
                        .filter(source -> source.startsWith("total_system_memory:"))
                        .findAny();
                if (totalSystemMemoryOpt.isEmpty()) {
                    log.warn("No 'total_system_memory' section received from redis. Unable to calculate the current memory usage");
                    promise.complete(Optional.empty());
                    return;
                }
                totalSystemMemory = Long.parseLong(totalSystemMemoryOpt.get().split(":")[1]);
                if (totalSystemMemory == 0L) {
                    log.warn("'total_system_memory' value 0 received from redis. Unable to calculate the current memory usage");
                    promise.complete(Optional.empty());
                    return;
                }

            } catch (NumberFormatException ex) {
                logPropertyWarning("total_system_memory", ex);
                promise.complete(Optional.empty());
                return;
            }

            long usedMemory;
            try {
                Optional<String> usedMemoryOpt = memoryInfo.result().toString()
                        .lines()
                        .filter(source -> source.startsWith("used_memory:"))
                        .findAny();
                if (usedMemoryOpt.isEmpty()) {
                    log.warn("No 'used_memory' section received from redis. Unable to calculate the current memory usage");
                    promise.complete(Optional.empty());
                    return;
                }
                usedMemory = Long.parseLong(usedMemoryOpt.get().split(":")[1]);
            } catch (NumberFormatException ex) {
                logPropertyWarning("used_memory", ex);
                promise.complete(Optional.empty());
                return;
            }

            float currentMemoryUsagePercentage = ((float) usedMemory / totalSystemMemory) * 100;
            if (currentMemoryUsagePercentage > MAX_PERCENTAGE) {
                currentMemoryUsagePercentage = MAX_PERCENTAGE;
            } else if (currentMemoryUsagePercentage < MIN_PERCENTAGE) {
                currentMemoryUsagePercentage = MIN_PERCENTAGE;
            }
            log.info("Current memory usage is {}%", decimalFormat.format(currentMemoryUsagePercentage));
            promise.complete(Optional.of(currentMemoryUsagePercentage));
        });
        return promise.future();
    }

    private void logPropertyWarning(String property, Exception ex) {
        log.warn("No or invalid '{}' value received from redis. Unable to calculate the current memory usage. " +
                "Exception: {}", property, ex.toString());
    }

    private enum LuaScript {
        GET("get.lua"), STORAGE_EXPAND("storageExpand.lua"), PUT("put.lua"), DELETE("del.lua"), CLEANUP("cleanup.lua");

        private String file;

        LuaScript(String file) {
            this.file = file;
        }

        public String getFile() {
            return file;
        }
    }

    /**
     * Holds the state of a lua script.
     */
    private class LuaScriptState {

        private LuaScript luaScriptType;
        /**
         * the script itself
         */
        private String script;
        /**
         * if the script logs to the redis log
         */
        private boolean logoutput = false;
        /**
         * the sha, over which the script can be accessed in redis
         */
        private String sha;

        private LuaScriptState(LuaScript luaScriptType, boolean logoutput) {
            this.luaScriptType = luaScriptType;
            this.logoutput = logoutput;
            this.composeLuaScript(luaScriptType);
            this.loadLuaScript(new RedisCommandDoNothing(), 0);
        }

        /**
         * Reads the script from the classpath and removes logging output if logoutput is false.
         * The script is stored in the class member script.
         *
         * @param luaScriptType
         */
        private void composeLuaScript(LuaScript luaScriptType) {
            log.info("read the lua script for script type: {} with logoutput: {}", luaScriptType, logoutput);

            // It is not possible to evalsha or eval inside lua scripts,
            // so we wrap the cleanupscript around the deletescript manually to avoid code duplication.
            // we have to comment the return, so that the cleanup script doesn't terminate
            if (LuaScript.CLEANUP.equals(luaScriptType)) {
                Map<String, String> values = new HashMap<>();
                values.put("delscript", readLuaScriptFromClasspath(LuaScript.DELETE).replaceAll("return", "--return"));
                StrSubstitutor sub = new StrSubstitutor(values, "--%(", ")");
                this.script = sub.replace(readLuaScriptFromClasspath(LuaScript.CLEANUP));
            } else {
                this.script = readLuaScriptFromClasspath(luaScriptType);
            }
            this.sha = DigestUtils.sha1Hex(this.script);
        }

        private String readLuaScriptFromClasspath(LuaScript luaScriptType) {
            BufferedReader in = new BufferedReader(new InputStreamReader(this.getClass().getClassLoader().getResourceAsStream(luaScriptType.getFile())));
            StringBuilder sb;
            try {
                sb = new StringBuilder();
                String line;
                while ((line = in.readLine()) != null) {
                    if (!logoutput && line.contains("redis.log(redis.LOG_NOTICE,")) {
                        continue;
                    }
                    sb.append(line).append("\n");
                }

            } catch (IOException e) {
                throw new RuntimeException(e);
            } finally {
                try {
                    in.close();
                } catch (IOException e) {
                    // Ignore
                }
            }
            return sb.toString();
        }

        /**
         * Rereads the lua script, eg. if the loglevel changed.
         */
        public void recomposeLuaScript() {
            this.composeLuaScript(luaScriptType);
        }

        /**
         * Load the get script into redis and store the sha in the class member sha.
         *
         * @param redisCommand     the redis command that should be executed, after the script is loaded.
         * @param executionCounter a counter to control recursion depth
         */
        public void loadLuaScript(final RedisCommand redisCommand, int executionCounter) {
            final int executionCounterIncr = ++executionCounter;

            // check first if the lua script already exists in the store
            redisAPI.script(Arrays.asList("exists", this.sha), resultArray -> {
                if (resultArray.failed()) {
                    log.error("Error checking whether lua script exists", resultArray.cause());
                    return;
                }
                Long exists = resultArray.result().get(0).toLong();
                // if script already
                if (Long.valueOf(1).equals(exists)) {
                    log.debug("RedisStorage script already exists in redis cache: " + luaScriptType);
                    redisCommand.exec(executionCounterIncr);
                } else {
                    log.info("load lua script for script type: {} logoutput: {}", luaScriptType, logoutput);
                    redisAPI.script(Arrays.asList("load", script), stringAsyncResult -> {
                        if (stringAsyncResult.failed()) {
                            log.error("Loading of lua script {} failed", luaScriptType);
                            return;
                        }
                        String newSha = stringAsyncResult.result().toString();
                        log.info("got sha from redis for lua script: {}: {}", luaScriptType, newSha);
                        if (!newSha.equals(sha)) {
                            log.warn("the sha calculated by myself: {} doesn't match with the sha from redis: {}. " +
                                    "We use the sha from redis", sha, newSha);
                        }
                        sha = newSha;
                        log.info("execute redis command for script type: {} with new sha: {}", luaScriptType, sha);
                        redisCommand.exec(executionCounterIncr);
                    });
                }
            });
        }

        public String getScript() {
            return script;
        }

        public void setScript(String script) {
            this.script = script;
        }

        public boolean getLogoutput() {
            return logoutput;
        }

        public void setLogoutput(boolean logoutput) {
            this.logoutput = logoutput;
        }

        public String getSha() {
            return sha;
        }

        public void setSha(String sha) {
            this.sha = sha;
        }
    }

    /**
     * The interface for a redis command.
     */
    private interface RedisCommand {
        void exec(int executionCounter);
    }

    /**
     * A dummy that can be passed if no RedisCommand should be executed.
     */
    private class RedisCommandDoNothing implements RedisCommand {

        @Override
        public void exec(int executionCounter) {
            // do nothing here
        }
    }

    /**
     * If the loglevel is trace and the logoutput in luaScriptState is false, then reload the script with logoutput and execute the RedisCommand.
     * If the loglevel is not trace and the logoutput in luaScriptState is true, then reload the script without logoutput and execute the RedisCommand.
     * If the loglevel is matching the luaScriptState, just execute the RedisCommand.
     *
     * @param luaScript    the type of lua script
     * @param redisCommand the redis command to execute
     */
    private void reloadScriptIfLoglevelChangedAndExecuteRedisCommand(LuaScript luaScript, RedisCommand redisCommand, int executionCounter) {
        boolean logoutput = log.isTraceEnabled();
        LuaScriptState luaScriptState = luaScripts.get(luaScript);
        // if the loglevel didn't change, execute the command and return
        if (logoutput == luaScriptState.getLogoutput()) {
            redisCommand.exec(executionCounter);
            return;
            // if the loglevel changed, set the new loglevel into the luaScriptState, recompose the script and provide the redisCommand as parameter to execute
        } else if (logoutput && !luaScriptState.getLogoutput()) {
            luaScriptState.setLogoutput(true);
            luaScriptState.recomposeLuaScript();

        } else if (!logoutput && luaScriptState.getLogoutput()) {
            luaScriptState.setLogoutput(false);
            luaScriptState.recomposeLuaScript();
        }
        luaScriptState.loadLuaScript(redisCommand, executionCounter);
    }

    public class ByteArrayReadStream implements ReadStream<Buffer> {

        ByteArrayInputStream content;
        int size;
        boolean paused;
        int position;
        Handler<Void> endHandler;
        Handler<Buffer> handler;

        public ByteArrayReadStream(byte[] byteArray) {
            size = byteArray.length;
            content = new ByteArrayInputStream(byteArray);
        }

        private void doRead() {
            vertx.runOnContext(v -> {
                if (!paused) {
                    if (position < size) {
                        int toRead = 8192;
                        if (position + toRead > size) {
                            toRead = size - position;
                        }
                        byte[] bytes = new byte[toRead];
                        content.read(bytes, 0, toRead);
                        handler.handle(Buffer.buffer(bytes));
                        position += toRead;
                        doRead();
                    } else {
                        endHandler.handle(null);
                    }
                }
            });
        }

        public ByteArrayReadStream resume() {
            paused = false;
            doRead();
            return this;
        }

        @Override
        public ReadStream<Buffer> fetch(long amount) {
            return null;
        }

        @Override
        public ByteArrayReadStream pause() {
            paused = true;
            return this;
        }

        @Override
        public ByteArrayReadStream exceptionHandler(Handler<Throwable> handler) {
            return this;
        }

        @Override
        public ReadStream<Buffer> handler(Handler<Buffer> handler) {
            this.handler = handler;
            doRead();
            return this;
        }

        @Override
        public ByteArrayReadStream endHandler(Handler<Void> endHandler) {
            this.endHandler = endHandler;
            return this;
        }
    }

    @Override
    public Optional<Float> getCurrentMemoryUsage() {
        return currentMemoryUsageOptional;
    }

    @Override
    public void get(String path, String etag, int offset, int limit, final Handler<Resource> handler) {
        final String key = encodePath(path);
        List<String> keys = Collections.singletonList(key);
        List<String> arguments = Arrays.asList(
                redisResourcesPrefix,
                redisCollectionsPrefix,
                expirableSet,
                String.valueOf(System.currentTimeMillis()),
                MAX_EXPIRE_IN_MILLIS,
                String.valueOf(offset),
                String.valueOf(limit),
                etag
        );
        reloadScriptIfLoglevelChangedAndExecuteRedisCommand(LuaScript.GET, new Get(keys, arguments, handler), 0);
    }

    /**
     * The Get Command Execution.
     * If the get script cannot be found under the sha in luaScriptState, reload the script.
     * To avoid infinite recursion, we limit the recursion.
     */
    private class Get implements RedisCommand {

        private List<String> keys;
        private List<String> arguments;
        private Handler<Resource> handler;

        public Get(List<String> keys, List<String> arguments, final Handler<Resource> handler) {
            this.keys = keys;
            this.arguments = arguments;
            this.handler = handler;
        }

        public void exec(final int executionCounter) {
            List<String> args = toPayload(luaScripts.get(LuaScript.GET).getSha(), keys.size(), keys, arguments);
            redisAPI.evalsha(args, event -> {
                if (event.succeeded()) {
                    Response values = event.result();
                    if (log.isTraceEnabled()) {
                        log.trace("RedisStorage get result: {}", values);
                    }
                    if ("notModified".equals(values.toString())) {
                        notModified(handler);
                    } else if ("notFound".equals(values.toString())) {
                        notFound(handler);
                    } else {
                        handleJsonArrayValues(values, handler, "0".equals(arguments.get(5)) && "-1".equals(arguments.get(6)));
                    }
                } else {
                    String message = event.cause().getMessage();
                    if (message != null && message.startsWith("NOSCRIPT")) {
                        log.warn("get script couldn't be found, reload it");
                        log.warn("amount the script got loaded: {}", executionCounter);
                        if (executionCounter > 10) {
                            log.error("amount the script got loaded is higher than 10, we abort");
                        } else {
                            luaScripts.get(LuaScript.GET).loadLuaScript(new Get(keys, arguments, handler), executionCounter);
                        }
                    } else {
                        log.error("GET request failed with message: {}", message);
                    }
                }
            });
        }
    }

    @Override
    public void storageExpand(String path, String etag, List<String> subResources, Handler<Resource> handler) {
        final String key = encodePath(path);
        List<String> keys = Collections.singletonList(key);
        List<String> arguments = Arrays.asList(
                redisResourcesPrefix,
                redisCollectionsPrefix,
                expirableSet,
                String.valueOf(System.currentTimeMillis()),
                MAX_EXPIRE_IN_MILLIS,
                StringUtils.join(subResources, ";"),
                String.valueOf(subResources.size())
        );
        reloadScriptIfLoglevelChangedAndExecuteRedisCommand(LuaScript.STORAGE_EXPAND, new StorageExpand(keys, arguments, handler, etag), 0);
    }

    /**
     * The StorageExpand Command Execution.
     * If the get script cannot be found under the sha in luaScriptState, reload the script.
     * To avoid infinite recursion, we limit the recursion.
     */
    private class StorageExpand implements RedisCommand {

        private List<String> keys;
        private List<String> arguments;
        private Handler<Resource> handler;
        private String etag;

        public StorageExpand(List<String> keys, List<String> arguments, final Handler<Resource> handler, String etag) {
            this.keys = keys;
            this.arguments = arguments;
            this.handler = handler;
            this.etag = etag;
        }

        public void exec(final int executionCounter) {
            List<String> args = toPayload(luaScripts.get(LuaScript.STORAGE_EXPAND).getSha(), keys.size(), keys, arguments);
            redisAPI.evalsha(args, event -> {
                if (event.succeeded()) {
                    String value = event.result().toString();
                    if (log.isTraceEnabled()) {
                        log.trace("RedisStorage get result: {}", value);
                    }
                    if ("compressionNotSupported".equalsIgnoreCase(value)) {
                        error(handler, "Collections having compressed resources are not supported in storage expand");
                        return;
                    }
                    if ("notFound".equalsIgnoreCase(value)) {
                        notFound(handler);
                        return;
                    }
                    JsonObject expandResult = new JsonObject();

                    JsonArray resultArr = new JsonArray(value);

                    for (Object resultEntry : resultArr) {
                        JsonArray entries = (JsonArray) resultEntry;
                        String subResourceName = ResourceNameUtil.resetReplacedColonsAndSemiColons(entries.getString(0));
                        String subResourceValue = entries.getString(1);
                        if (subResourceValue.startsWith("[") && subResourceValue.endsWith("]")) {
                            expandResult.put(subResourceName, extractSortedJsonArray(subResourceValue));
                        } else {
                            try {
                                expandResult.put(subResourceName, new JsonObject(subResourceValue));
                            } catch (DecodeException ex) {
                                invalid(handler, "Error decoding invalid json resource '" + subResourceName + "'");
                                return;
                            }
                        }
                    }

                    byte[] finalExpandedContent = decodeBinary(expandResult.encode());
                    String calcDigest = DigestUtils.sha1Hex(finalExpandedContent);

                    if (calcDigest.equals(etag)) {
                        notModified(handler);
                    } else {
                        DocumentResource r = new DocumentResource();
                        r.readStream = new ByteArrayReadStream(finalExpandedContent);
                        r.length = finalExpandedContent.length;
                        r.etag = calcDigest;
                        r.closeHandler = event1 -> {
                            // nothing to close
                        };
                        handler.handle(r);
                    }
                } else {
                    String message = event.cause().getMessage();
                    if (message != null && message.startsWith("NOSCRIPT")) {
                        log.warn("storageExpand script couldn't be found, reload it");
                        log.warn("amount the script got loaded: {}", executionCounter);
                        if (executionCounter > 10) {
                            log.error("amount the script got loaded is higher than 10, we abort");
                        } else {
                            luaScripts.get(LuaScript.STORAGE_EXPAND).loadLuaScript(new StorageExpand(keys, arguments, handler, etag), executionCounter);
                        }
                    } else {
                        log.error("StorageExpand request failed with message: {}", message);
                    }
                }
            });
        }
    }

    private JsonArray extractSortedJsonArray(String arrayString) {
        String arrayContent = arrayString.replaceAll("\\[", EMPTY).replaceAll("\\]", EMPTY).replaceAll("\"", EMPTY).replaceAll("\\\\", EMPTY);
        String[] splitted = StringUtils.split(arrayContent, ",");
        List<String> resources = new ArrayList<>();
        List<String> collections = new ArrayList<>();
        for (String split : splitted) {
            if (split.endsWith("/")) {
                collections.add(split);
            } else {
                resources.add(split);
            }
        }
        Collections.sort(collections);
        collections.addAll(resources);
        return new JsonArray(new ArrayList<Object>(collections));
    }

    private void handleJsonArrayValues(Response values, Handler<Resource> handler, boolean allowEmptyReturn) {
        String type = values.get(0).toString();
        if ("TYPE_RESOURCE".equals(type)) {
            String valueStr = values.get(1).toString();
            DocumentResource r = new DocumentResource();
            byte[] content = decodeBinary(valueStr);
            if (values.get(3) != null) {
                // data is compressed
                GZIPUtil.decompressResource(vertx, log, content, decompressedResult -> {
                    if (decompressedResult.succeeded()) {
                        r.readStream = new ByteArrayReadStream(decompressedResult.result());
                        r.length = decompressedResult.result().length;
                        r.etag = values.get(2).toString();
                        r.closeHandler = event -> {
                            // nothing to close
                        };
                        handler.handle(r);
                    } else {
                        error(handler, "Error during decompression of resource: " + decompressedResult.cause().getMessage());
                    }
                });
            } else {
                r.readStream = new ByteArrayReadStream(content);
                r.length = content.length;
                r.etag = values.get(2).toString();
                r.closeHandler = event -> {
                    // nothing to close
                };
                handler.handle(r);
            }
        } else if ("TYPE_COLLECTION".equals(type)) {
            CollectionResource r = new CollectionResource();
            Set<Resource> items = new HashSet<>();
            for (Response value : values) {
                String member = value.toString();
                if (!"TYPE_COLLECTION".equals(member)) {
                    if (member.endsWith(":")) {
                        member = member.replaceAll(":$", "");
                        CollectionResource c = new CollectionResource();
                        c.name = member;
                        items.add(c);
                    } else {
                        DocumentResource d = new DocumentResource();
                        d.name = member;
                        items.add(d);
                    }
                }
            }
            if (allowEmptyReturn && items.size() == 0) {
                notFound(handler);
            } else {
                r.items = new ArrayList<>(items);
                Collections.sort(r.items);
                handler.handle(r);
            }
        } else {
            notFound(handler);
        }
    }

    class ByteArrayWriteStream implements WriteStream<Buffer> {

        private ByteArrayOutputStream bos = new ByteArrayOutputStream();

        public byte[] getBytes() {
            return bos.toByteArray();
        }

        @Override
        public ByteArrayWriteStream setWriteQueueMaxSize(int maxSize) {
            return this;
        }

        @Override
        public boolean writeQueueFull() {
            return false;
        }

        @Override
        public ByteArrayWriteStream drainHandler(Handler<Void> handler) {
            return this;
        }

        @Override
        public ByteArrayWriteStream exceptionHandler(Handler<Throwable> handler) {
            return this;
        }

        @Override
        public Future<Void> write(Buffer data) {
            try {
                bos.write(data.getBytes());
                return Future.succeededFuture();
            } catch (IOException e) {
                return Future.failedFuture(e.getMessage());
            }

        }

        @Override
        public void write(Buffer data, Handler<AsyncResult<Void>> handler) {
            write(data).onComplete(handler);
        }

        @Override
        public Future<Void> end() {
            try {
                bos.close();
                return Future.succeededFuture();
            } catch (IOException e) {
                return Future.failedFuture(e.getMessage());
            }
        }

        @Override
        public void end(Handler<AsyncResult<Void>> handler) {
            end().onComplete(handler);
        }
    }

    private String initEtagValue(String providedEtag) {
        if (!isEmpty(providedEtag)) {
            return providedEtag;
        }
        return UUID.randomUUID().toString();
    }


    @Override
    public void put(String path, final String etag, final boolean merge, final long expire, final String lockOwner, final LockMode lockMode, final long lockExpire, final Handler<Resource> handler) {
        put(path, etag, merge, expire, lockOwner, lockMode, lockExpire, false, handler);
    }

    @Override
    public void put(String path, final String etag, final boolean merge, final long expire, final Handler<Resource> handler) {
        put(path, etag, merge, expire, "", LockMode.SILENT, 0, handler);
    }

    @Override
    public void put(String path, String etag, boolean merge, long expire, String lockOwner, LockMode lockMode, long lockExpire, boolean storeCompressed, Handler<Resource> handler) {
        final String key = encodePath(path);
        final DocumentResource d = new DocumentResource();
        final ByteArrayWriteStream stream = new ByteArrayWriteStream();

        final String etagValue = initEtagValue(etag);
        d.writeStream = stream;
        d.closeHandler = event -> {
            String expireInMillis = MAX_EXPIRE_IN_MILLIS;
            if (expire > -1) {
                expireInMillis = String.valueOf(System.currentTimeMillis() + (expire * 1000));
            }

            if (Long.valueOf(expireInMillis) > Long.valueOf(MAX_EXPIRE_IN_MILLIS)) {
                // #76 reset to the defined max value
                expireInMillis = MAX_EXPIRE_IN_MILLIS;
            }

            String lockExpireInMillis = String.valueOf(System.currentTimeMillis() + (lockExpire * 1000));

            List<String> keys = Collections.singletonList(key);

            if (storeCompressed) {
                String finalExpireInMillis = expireInMillis;
                GZIPUtil.compressResource(vertx, log, stream.getBytes(), compressResourceResult -> {
                    if (compressResourceResult.succeeded()) {
                        List<String> arg = Arrays.asList(
                                redisResourcesPrefix,
                                redisCollectionsPrefix,
                                expirableSet,
                                merge ? "true" : "false",
                                finalExpireInMillis,
                                MAX_EXPIRE_IN_MILLIS,
                                encodeBinary(compressResourceResult.result()),
                                etagValue,
                                redisLockPrefix,
                                lockOwner,
                                lockMode.text(),
                                lockExpireInMillis,
                                storeCompressed ? "1" : "0"
                        );
                        reloadScriptIfLoglevelChangedAndExecuteRedisCommand(LuaScript.PUT, new Put(d, keys, arg, handler), 0);
                    } else {
                        error(handler, "Error during compression of resource");
                    }
                });
            } else {
                List<String> arguments = Arrays.asList(
                        redisResourcesPrefix,
                        redisCollectionsPrefix,
                        expirableSet,
                        merge ? "true" : "false",
                        expireInMillis,
                        MAX_EXPIRE_IN_MILLIS,
                        encodeBinary(stream.getBytes()),
                        etagValue,
                        redisLockPrefix,
                        lockOwner,
                        lockMode.text(),
                        lockExpireInMillis,
                        storeCompressed ? "1" : "0"
                );
                reloadScriptIfLoglevelChangedAndExecuteRedisCommand(LuaScript.PUT, new Put(d, keys, arguments, handler), 0);
            }
        };
        handler.handle(d);
    }

    /**
     * The Put Command Execution.
     * If the get script cannot be found under the sha in luaScriptState, reload the script.
     * To avoid infinite recursion, we limit the recursion.
     */
    private class Put implements RedisCommand {

        private DocumentResource d;
        private List<String> keys;
        private List<String> arguments;
        private Handler<Resource> handler;

        public Put(DocumentResource d, List<String> keys, List<String> arguments, Handler<Resource> handler) {
            this.d = d;
            this.keys = keys;
            this.arguments = arguments;
            this.handler = handler;
        }

        public void exec(final int executionCounter) {
            List<String> args = toPayload(luaScripts.get(LuaScript.PUT).getSha(), keys.size(), keys, arguments);
            redisAPI.evalsha(args, event -> {
                if (event.succeeded()) {
                    String result = event.result().toString();
                    if (log.isTraceEnabled()) {
                        log.trace("RedisStorage successful put. Result: {}", result);
                    }
                    if (result != null && result.startsWith("existingCollection")) {
                        CollectionResource c = new CollectionResource();
                        handler.handle(c);
                    } else if (result != null && result.startsWith("existingResource")) {
                        DocumentResource d = new DocumentResource();
                        d.exists = false;
                        handler.handle(d);
                    } else if ("notModified".equals(result)) {
                        notModified(handler);
                    } else if (LockMode.REJECT.text().equals(result)) {
                        rejected(handler);
                    } else {
                        d.endHandler.handle(null);
                    }
                } else {
                    String message = event.cause().getMessage();
                    if (message != null && message.startsWith("NOSCRIPT")) {
                        log.warn("put script couldn't be found, reload it");
                        log.warn("amount the script got loaded: {}", executionCounter);
                        if (executionCounter > 10) {
                            log.error("amount the script got loaded is higher than 10, we abort");
                        } else {
                            luaScripts.get(LuaScript.PUT).loadLuaScript(new Put(d, keys, arguments, handler), executionCounter);
                        }
                    } else if (message != null && d.errorHandler != null) {
                        log.error("PUT request failed with message: {}", message);
                        d.errorHandler.handle(event.cause());
                    }
                }
            });
        }
    }

    @Override
    public void delete(String path, String lockOwner, LockMode lockMode, long lockExpire, boolean confirmCollectionDelete,
                       boolean deleteRecursive, final Handler<Resource> handler) {
        final String key = encodePath(path);
        List<String> keys = Collections.singletonList(key);

        String lockExpireInMillis = String.valueOf(System.currentTimeMillis() + (lockExpire * 1000));

        List<String> arguments = Arrays.asList(
                redisResourcesPrefix,
                redisCollectionsPrefix,
                redisDeltaResourcesPrefix,
                redisDeltaEtagsPrefix,
                expirableSet,
                String.valueOf(System.currentTimeMillis()),
                MAX_EXPIRE_IN_MILLIS,
                confirmCollectionDelete ? "true" : "false",
                deleteRecursive ? "true" : "false",
                redisLockPrefix,
                lockOwner,
                lockMode.text(),
                lockExpireInMillis
        );
        reloadScriptIfLoglevelChangedAndExecuteRedisCommand(LuaScript.DELETE, new Delete(keys, arguments, handler), 0);
    }

    /**
     * The Delete Command Execution.
     * If the get script cannot be found under the sha in luaScriptState, reload the script.
     * To avoid infinite recursion, we limit the recursion.
     */
    private class Delete implements RedisCommand {

        private List<String> keys;
        private List<String> arguments;
        private Handler<Resource> handler;

        public Delete(List<String> keys, List<String> arguments, final Handler<Resource> handler) {
            this.keys = keys;
            this.arguments = arguments;
            this.handler = handler;
        }

        public void exec(final int executionCounter) {
            List<String> args = toPayload(luaScripts.get(LuaScript.DELETE).getSha(), keys.size(), keys, arguments);
            redisAPI.evalsha(args, event -> {
                if (event.cause() != null && event.cause().getMessage().startsWith("NOSCRIPT")) {
                    log.warn("delete script couldn't be found, reload it");
                    log.warn("amount the script got loaded: {}", executionCounter);
                    if (executionCounter > 10) {
                        log.error("amount the script got loaded is higher than 10, we abort");
                    } else {
                        luaScripts.get(LuaScript.DELETE).loadLuaScript(new Delete(keys, arguments, handler), executionCounter);
                    }
                    return;
                }

                String result = null;
                if (event.result() != null) {
                    result = event.result().toString();
                }
                if (log.isTraceEnabled()) {
                    log.trace("RedisStorage delete result: {}", result);
                }
                if ("notEmpty".equals(result)) {
                    notEmpty(handler);
                    return;
                }
                if ("notFound".equals(result)) {
                    notFound(handler);
                    return;
                } else if (LockMode.REJECT.text().equals(result)) {
                    rejected(handler);
                    return;
                }
                Resource r = new Resource();
                handler.handle(r);
            });
        }
    }

    /**
     * Cleans up the outdated resources recursive.
     * If the script which is refered over the luaScriptState.sha, the execution is aborted and the script is reloaded.
     *
     * @param handler        the handler to execute
     * @param cleanedLastRun how many resources were cleaned in the last run
     * @param maxdel         max resources to clean
     * @param bulkSize       how many resources should be cleaned in one run
     */
    public void cleanupRecursive(final Handler<DocumentResource> handler, final long cleanedLastRun, final long maxdel, final int bulkSize) {
        List<String> arguments = Arrays.asList(
                redisResourcesPrefix,
                redisCollectionsPrefix,
                redisDeltaResourcesPrefix,
                redisDeltaEtagsPrefix,
                expirableSet,
                "0",
                MAX_EXPIRE_IN_MILLIS,
                "false",
                "true",
                String.valueOf(System.currentTimeMillis()),
                String.valueOf(bulkSize)
        );
        List<String> args = toPayload(luaScripts.get(LuaScript.CLEANUP).getSha(), 0, Collections.emptyList(), arguments);
        redisAPI.evalsha(args, event -> {
            if (log.isTraceEnabled()) {
                log.trace("RedisStorage cleanup resources succeeded: {}", event.succeeded());
            }

            if (event.failed() && event.cause() != null && event.cause().getMessage().startsWith("NOSCRIPT")) {
                log.warn("the cleanup script is not loaded. Load it and exit. The Cleanup will success the next time");
                luaScripts.get(LuaScript.CLEANUP).loadLuaScript(new RedisCommandDoNothing(), 0);
                return;
            }

            long cleanedThisRun = 0;
            if (event.succeeded() && event.result().toLong() != null) {
                cleanedThisRun = event.result().toLong();
            }
            if (log.isTraceEnabled()) {
                log.trace("RedisStorage cleanup resources cleanded this run: {}", cleanedThisRun);
            }
            final long cleaned = cleanedLastRun + cleanedThisRun;
            if (cleanedThisRun != 0 && cleaned < maxdel) {
                if (log.isTraceEnabled()) {
                    log.trace("RedisStorage cleanup resources call recursive next bulk");
                }
                cleanupRecursive(handler, cleaned, maxdel, bulkSize);
            } else {
                redisAPI.zcount(expirableSet, "0", String.valueOf(System.currentTimeMillis()), longAsyncResult -> {
                    Long result = longAsyncResult.result().toLong();
                    if (log.isTraceEnabled()) {
                        log.trace("RedisStorage cleanup resources zcount on expirable set: {}", result);
                    }
                    int resToCleanLeft = 0;
                    if (result != null && result.intValue() >= 0) {
                        resToCleanLeft = result.intValue();
                    }
                    JsonObject retObj = new JsonObject();
                    retObj.put("cleanedResources", cleaned);
                    retObj.put("expiredResourcesLeft", resToCleanLeft);
                    DocumentResource r = new DocumentResource();
                    byte[] content = decodeBinary(retObj.toString());
                    r.readStream = new ByteArrayReadStream(content);
                    r.length = content.length;
                    r.closeHandler = event1 -> {
                        // nothing to close
                    };
                    handler.handle(r);
                });
            }
        });
    }

    private String encodePath(String path) {
        if (path.equals("/")) {
            path = "";
        }
        return ResourceNameUtil.replaceColonsAndSemiColons(path).replaceAll("/", ":");
    }

    private String encodeBinary(byte[] bytes) {
        return new String(bytes, StandardCharsets.ISO_8859_1);
    }

    private byte[] decodeBinary(String s) {
        return s.getBytes(StandardCharsets.ISO_8859_1);
    }

    private void notFound(Handler<Resource> handler) {
        Resource r = new Resource();
        r.exists = false;
        handler.handle(r);
    }

    private void notEmpty(Handler<Resource> handler) {
        Resource r = new Resource();
        r.error = true;
        r.errorMessage = "directory not empty. Use recursive=true parameter to delete";
        handler.handle(r);
    }

    private void notModified(Handler<Resource> handler) {
        Resource r = new Resource();
        r.modified = false;
        handler.handle(r);
    }

    private void rejected(Handler<Resource> handler) {
        Resource r = new Resource();
        r.rejected = true;
        handler.handle(r);
    }

    private void invalid(Handler<Resource> handler, String invalidMessage) {
        Resource r = new Resource();
        r.invalid = true;
        r.invalidMessage = invalidMessage;
        handler.handle(r);
    }

    private void error(Handler<Resource> handler, String errorMessage) {
        Resource r = new Resource();
        r.error = true;
        r.errorMessage = errorMessage;
        handler.handle(r);
    }

    @Override
    public void cleanup(Handler<DocumentResource> handler, String cleanupResourcesAmountStr) {
        long cleanupResourcesAmountUsed = cleanupResourcesAmount;
        if (log.isTraceEnabled()) {
            log.trace("RedisStorage cleanup resources,  cleanupResourcesAmount: {}", cleanupResourcesAmountUsed);
        }
        try {
            cleanupResourcesAmountUsed = Long.parseLong(cleanupResourcesAmountStr);
        } catch (Exception e) {
            // do nothing
        }
        cleanupRecursive(handler, 0, cleanupResourcesAmountUsed, CLEANUP_BULK_SIZE);
    }

    private boolean isEmpty(CharSequence cs) {
        return cs == null || cs.length() == 0;
    }

    /**
     * from https://github.com/vert-x3/vertx-redis-client/blob/3.9/src/main/java/io/vertx/redis/impl/RedisClientImpl.java#L94
     *
     * @param parameters
     * @return
     */
    private static List<String> toPayload(Object... parameters) {
        List<String> result = new ArrayList<>(parameters.length);

        for (Object param : parameters) {
            // unwrap
            if (param instanceof JsonArray) {
                param = ((JsonArray) param).getList();
            }
            // unwrap
            if (param instanceof JsonObject) {
                param = ((JsonObject) param).getMap();
            }

            if (param instanceof Collection) {
                ((Collection) param).stream().filter(Objects::nonNull).forEach(o -> result.add(o.toString()));
            } else if (param instanceof Map) {
                for (Map.Entry<?, ?> pair : ((Map<?, ?>) param).entrySet()) {
                    result.add(pair.getKey().toString());
                    result.add(pair.getValue().toString());
                }
            } else if (param instanceof Stream) {
                ((Stream) param).forEach(e -> {
                    if (e instanceof Object[]) {
                        Collections.addAll(result, (String[]) e);
                    } else {
                        result.add(e.toString());
                    }
                });
            } else if (param != null) {
                result.add(param.toString());
            }
        }
        return result;
    }

}