package org.swisspush.reststorage.util;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.vertx.core.json.JsonObject;
import io.vertx.redis.client.RedisClientType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Utility class to configure the RestStorageModule.
 *
 * @author https://github.com/mcweba [Marc-Andre Weber]
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ModuleConfiguration {

    private static final Logger log = LoggerFactory.getLogger(ModuleConfiguration.class);

    public enum StorageType {
        filesystem, redis
    }

    private String root = ".";
    private StorageType storageType = StorageType.filesystem;
    private int port = 8989;
    private boolean httpRequestHandlerEnabled = true;
    private boolean httpRequestHandlerAuthenticationEnabled = false;
    private String httpRequestHandlerUsername;
    private String httpRequestHandlerPassword;
    private String prefix = "";
    private String storageAddress = "resource-storage";
    private Map<String, String> editorConfig = null;
    private List<String> redisHosts = Collections.singletonList("localhost");
    private List<Integer> redisPorts = Collections.singletonList(6379);
    private boolean redisEnableTls;
    private RedisClientType redisClientType = RedisClientType.STANDALONE;

    /**
     * @deprecated Instance authentication is considered as legacy. With Redis from 6.x on the ACL authentication method should be used.
     */
    @Deprecated(since = "3.0.17")
    private String redisAuth = null;
    private int redisReconnectAttempts = 0;
    private int redisReconnectDelaySec = 30;
    private int redisPoolRecycleTimeoutMs = 180_000;
    private String redisPassword = null;
    private String redisUser = null;
    private String expirablePrefix = "rest-storage:expirable";
    private String resourcesPrefix = "rest-storage:resources";
    private String collectionsPrefix = "rest-storage:collections";
    private String deltaResourcesPrefix = "delta:resources";
    private String deltaEtagsPrefix = "delta:etags";
    private Integer resourceCleanupIntervalSec = null;
    private long resourceCleanupAmount = 100_000L;
    private String lockPrefix = "rest-storage:locks";
    private boolean confirmCollectionDelete = false;
    private boolean rejectStorageWriteOnLowMemory = false;
    private long freeMemoryCheckIntervalMs = 60_000L;
    private boolean return200onDeleteNonExisting = false;
    private int maxRedisConnectionPoolSize = 24;
    private int maxQueueWaiting = 24;
    private int maxRedisWaitingHandlers = 2048;


    public ModuleConfiguration root(String root) {
        this.root = root;
        return this;
    }

    public ModuleConfiguration storageType(StorageType storageType) {
        this.storageType = storageType;
        return this;
    }

    public ModuleConfiguration storageTypeFromString(String storageType) throws IllegalArgumentException {
        this.storageType = StorageType.valueOf(storageType); // let it throw IAEx in case of unknown string value
        return this;
    }

    public ModuleConfiguration port(int port) {
        this.port = port;
        return this;
    }

    public ModuleConfiguration httpRequestHandlerEnabled(boolean httpRequestHandlerEnabled) {
        this.httpRequestHandlerEnabled = httpRequestHandlerEnabled;
        return this;
    }

    public ModuleConfiguration httpRequestHandlerAuthenticationEnabled(boolean httpRequestHandlerAuthenticationEnabled) {
        this.httpRequestHandlerAuthenticationEnabled = httpRequestHandlerAuthenticationEnabled;
        return this;
    }

    public ModuleConfiguration httpRequestHandlerUsername(String httpRequestHandlerUsername) {
        this.httpRequestHandlerUsername = httpRequestHandlerUsername;
        return this;
    }

    public ModuleConfiguration httpRequestHandlerPassword(String httpRequestHandlerPassword) {
        this.httpRequestHandlerPassword = httpRequestHandlerPassword;
        return this;
    }

    public ModuleConfiguration prefix(String prefix) {
        this.prefix = prefix;
        return this;
    }

    public ModuleConfiguration storageAddress(String storageAddress) {
        this.storageAddress = storageAddress;
        return this;
    }

    public ModuleConfiguration editorConfig(Map<String, String> editorConfig) {
        this.editorConfig = editorConfig;
        return this;
    }

    public ModuleConfiguration redisHost(String redisHost) {
        this.redisHosts = Collections.singletonList(redisHost);
        return this;
    }

    public ModuleConfiguration redisPort(int redisPort) {
        this.redisPorts = Collections.singletonList(redisPort);
        return this;
    }

    public ModuleConfiguration redisHosts(List<String> redisHosts) {
        this.redisHosts = redisHosts;
        return this;
    }

    public ModuleConfiguration redisPorts(List<Integer> redisPorts) {
        this.redisPorts = redisPorts;
        return this;
    }

    public ModuleConfiguration redisEnableTls(boolean redisEnableTls) {
        this.redisEnableTls = redisEnableTls;
        return this;
    }

    public ModuleConfiguration redisClientType(RedisClientType redisClientType) {
        this.redisClientType = redisClientType;
        return this;
    }

    public ModuleConfiguration redisReconnectAttempts(int redisReconnectAttempts) {
        this.redisReconnectAttempts = redisReconnectAttempts;
        return this;
    }

    public ModuleConfiguration redisReconnectDelaySec(int redisReconnectDelaySec) {
        this.redisReconnectDelaySec = redisReconnectDelaySec;
        return this;
    }

    public ModuleConfiguration redisPoolRecycleTimeoutMs(int redisPoolRecycleTimeoutMs) {
        this.redisPoolRecycleTimeoutMs = redisPoolRecycleTimeoutMs;
        return this;
    }

    @Deprecated(since = "3.0.17")
    public ModuleConfiguration redisAuth(String redisAuth) {
        this.redisAuth = redisAuth;
        return this;
    }

    public ModuleConfiguration redisPassword(String redisPassword) {
        this.redisPassword = redisPassword;
        return this;
    }

    public ModuleConfiguration redisUser(String redisUser) {
        this.redisUser = redisUser;
        return this;
    }

    public ModuleConfiguration expirablePrefix(String expirablePrefix) {
        this.expirablePrefix = expirablePrefix;
        return this;
    }

    public ModuleConfiguration resourcesPrefix(String resourcesPrefix) {
        this.resourcesPrefix = resourcesPrefix;
        return this;
    }

    public ModuleConfiguration collectionsPrefix(String collectionsPrefix) {
        this.collectionsPrefix = collectionsPrefix;
        return this;
    }

    public ModuleConfiguration deltaResourcesPrefix(String deltaResourcesPrefix) {
        this.deltaResourcesPrefix = deltaResourcesPrefix;
        return this;
    }

    public ModuleConfiguration deltaEtagsPrefix(String deltaEtagsPrefix) {
        this.deltaEtagsPrefix = deltaEtagsPrefix;
        return this;
    }

    public ModuleConfiguration resourceCleanupAmount(long resourceCleanupAmount) {
        this.resourceCleanupAmount = resourceCleanupAmount;
        return this;
    }

    public ModuleConfiguration resourceCleanupIntervalSec(Integer resourceCleanupIntervalSec) {
        if(resourceCleanupIntervalSec == null || resourceCleanupIntervalSec < 1){
            log.warn("Resource cleanup interval value is either null or negative. Interval cleanup will not be activated");
            this.resourceCleanupIntervalSec = null;
        }else {
            this.resourceCleanupIntervalSec = resourceCleanupIntervalSec;
        }
        return this;
    }

    public ModuleConfiguration lockPrefix(String lockPrefix) {
        this.lockPrefix = lockPrefix;
        return this;
    }

    public ModuleConfiguration confirmCollectionDelete(boolean confirmCollectionDelete) {
        this.confirmCollectionDelete = confirmCollectionDelete;
        return this;
    }

    public ModuleConfiguration rejectStorageWriteOnLowMemory(boolean rejectStorageWriteOnLowMemory) {
        this.rejectStorageWriteOnLowMemory = rejectStorageWriteOnLowMemory;
        return this;
    }

    public ModuleConfiguration freeMemoryCheckIntervalMs(long freeMemoryCheckIntervalMs) {
        this.freeMemoryCheckIntervalMs = freeMemoryCheckIntervalMs;
        return this;
    }

    public ModuleConfiguration maxRedisConnectionPoolSize(int maxRedisConnectionPoolSize) {
        this.maxRedisConnectionPoolSize = maxRedisConnectionPoolSize;
        return this;
    }

    public ModuleConfiguration maxRedisWaitQueueSize(int maxRedisWaitQueueSize) {
        this.maxQueueWaiting = maxRedisWaitQueueSize;
        return this;
    }

    public ModuleConfiguration maxRedisWaitingHandlers(int maxRedisWaitingHandlers) {
        this.maxRedisWaitingHandlers = maxRedisWaitingHandlers;
        return this;
    }

    public ModuleConfiguration return200onDeleteNonExisting(boolean deleteNonExistingReturn200) {
        this.return200onDeleteNonExisting = deleteNonExistingReturn200;
        return this;
    }

    public String getRoot() {
        return root;
    }

    public StorageType getStorageType() {
        return storageType;
    }

    public int getPort() {
        return port;
    }

    public boolean isHttpRequestHandlerEnabled() {
        return httpRequestHandlerEnabled;
    }

    public boolean isHttpRequestHandlerAuthenticationEnabled() {
        return httpRequestHandlerAuthenticationEnabled;
    }

    public String getHttpRequestHandlerUsername() {
        return httpRequestHandlerUsername;
    }

    public String getHttpRequestHandlerPassword() {
        return httpRequestHandlerPassword;
    }

    public String getPrefix() {
        return prefix;
    }

    public String getStorageAddress() {
        return storageAddress;
    }

    public Map<String, String> getEditorConfig() {
        return editorConfig;
    }

    public String getRedisHost() {
        return redisHosts.get(0);
    }

    public List<String> getRedisHosts() {
        return redisHosts;
    }

    public int getRedisPort() {
        return redisPorts.get(0);
    }
    public List<Integer> getRedisPorts() {
        return redisPorts;
    }

    public int getRedisReconnectAttempts() {
        return redisReconnectAttempts;
    }

    public int getRedisReconnectDelaySec() {
        if (redisReconnectDelaySec < 1) {
            log.debug("Ignoring value {}s for redisReconnectDelay (too small) and use 1 instead", redisReconnectDelaySec);
            return 1;
        }
        return redisReconnectDelaySec;
    }

    public int getRedisPoolRecycleTimeoutMs() {
        return redisPoolRecycleTimeoutMs;
    }

    public boolean isRedisEnableTls() {
        return redisEnableTls;
    }

    public boolean isRedisClustered() {
        return redisClustered;
    }

    public String getRedisAuth() {
        return redisAuth;
    }

    public String getRedisPassword() {
        return redisPassword;
    }

    public String getRedisUser() {
        return redisUser;
    }
    public RedisClientType getRedisClientType() {
        return redisClientType;
    }
    public String getExpirablePrefix() {
        return expirablePrefix;
    }

    public String getResourcesPrefix() {
        return resourcesPrefix;
    }

    public String getCollectionsPrefix() {
        return collectionsPrefix;
    }

    public String getDeltaResourcesPrefix() {
        return deltaResourcesPrefix;
    }

    public String getDeltaEtagsPrefix() {
        return deltaEtagsPrefix;
    }

    public Integer getResourceCleanupIntervalSec() {
        return resourceCleanupIntervalSec;
    }

    public long getResourceCleanupAmount() {
        return resourceCleanupAmount;
    }

    public String getLockPrefix() {
        return lockPrefix;
    }

    public boolean isConfirmCollectionDelete() {
        return confirmCollectionDelete;
    }

    public boolean isRejectStorageWriteOnLowMemory() {
        return rejectStorageWriteOnLowMemory;
    }

    public long getFreeMemoryCheckIntervalMs() {
        return freeMemoryCheckIntervalMs;
    }

    public boolean isReturn200onDeleteNonExisting() {
        return return200onDeleteNonExisting;
    }

    public int getMaxRedisConnectionPoolSize() {
        return maxRedisConnectionPoolSize;
    }

    public int getMaxQueueWaiting() {
        return maxQueueWaiting;
    }

    public int getMaxRedisWaitingHandlers() {
        return maxRedisWaitingHandlers;
    }

    public JsonObject asJsonObject() {
        return JsonObject.mapFrom(this);
    }

    public static ModuleConfiguration fromJsonObject(JsonObject json) {
        return json.mapTo(ModuleConfiguration.class);
    }

    @Override
    public String toString() {
        return asJsonObject().toString();
    }

}
