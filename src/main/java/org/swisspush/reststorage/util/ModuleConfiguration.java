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
        filesystem, redis, s3
    }

    private static final String DEFAULT_HOSTNAME_VERIFICATION_ALGORITHM = "";

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
    private boolean ssl = false;
    private boolean trustAll = false;
    private String hostnameVerificationAlgorithm = DEFAULT_HOSTNAME_VERIFICATION_ALGORITHM;
    private RedisClientType redisClientType = RedisClientType.STANDALONE;

    /**
     * @deprecated Instance authentication is considered as legacy. With Redis from 6.x on the ACL authentication method should be used.
     */
    @Deprecated(since = "3.0.17")
    private String redisAuth = null;
    private String redisPublishMetrcisAddress = null;
    private String redisPublishMetrcisPrefix = "storage";
    private int redisPublishMetrcisRefreshPeriodSec = 10;
    private int redisReconnectAttempts = 0;
    private int redisReconnectDelaySec = 30;
    private int redisPoolRecycleTimeoutMs = 180_000;
    private int redisReadyCheckIntervalMs = -1;
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
    private int maxStorageExpandSubresources = 1000;

    private String s3BucketName = null;
    private String awsS3Region = null;
    private String s3AccessKeyId = null;
    private String s3SecretAccessKey = null;
    private boolean s3UseTlsConnection = true;
    private boolean createBucketIfNotPresentYet = false;
    private boolean localS3 = false;
    private String localS3Endpoint = null;
    private int localS3Port = 0;

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

    public ModuleConfiguration setSsl(boolean ssl) {
        this.ssl = ssl;
        return this;
    }

    public ModuleConfiguration setTrustAll(boolean trustAll) {
        this.trustAll = trustAll;
        return this;
    }

    public ModuleConfiguration setHostnameVerificationAlgorithm(String hostnameVerificationAlgorithm) {
        if(hostnameVerificationAlgorithm == null) {
            log.warn("hostnameVerificationAlgorithm can not be null!");
            this.hostnameVerificationAlgorithm = DEFAULT_HOSTNAME_VERIFICATION_ALGORITHM;
        } else {
            this.hostnameVerificationAlgorithm = hostnameVerificationAlgorithm;
        }
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

    public ModuleConfiguration redisReadyCheckIntervalMs(int redisReadyCheckIntervalMs) {
        this.redisReadyCheckIntervalMs = redisReadyCheckIntervalMs;
        return this;
    }

    @Deprecated(since = "3.0.17")
    public ModuleConfiguration redisAuth(String redisAuth) {
        this.redisAuth = redisAuth;
        return this;
    }

    public ModuleConfiguration redisPublishMetrcisAddress(String redisPublishMetrcisAddress) {
        this.redisPublishMetrcisAddress = redisPublishMetrcisAddress;
        return this;
    }

    public ModuleConfiguration redisPublishMetrcisPrefix(String redisPublishMetrcisPrefix) {
        this.redisPublishMetrcisPrefix = redisPublishMetrcisPrefix;
        return this;
    }

    public ModuleConfiguration redisPublishMetrcisRefreshPeriodSec(int redisPublishMetrcisRefreshPeriodSec) {
        this.redisPublishMetrcisRefreshPeriodSec = redisPublishMetrcisRefreshPeriodSec;
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

    public ModuleConfiguration maxStorageExpandSubresources(int maxStorageExpandSubresources) {
        this.maxStorageExpandSubresources = maxStorageExpandSubresources;
        return this;
    }

    public ModuleConfiguration awsS3Region(String awsS3Region) {
        this.awsS3Region = awsS3Region;
        return this;
    }

    public ModuleConfiguration s3BucketName(String awsS3BucketName) {
        this.s3BucketName = awsS3BucketName;
        return this;
    }

    public ModuleConfiguration s3AccessKeyId(String awsS3AccessKeyId) {
        this.s3AccessKeyId = awsS3AccessKeyId;
        return this;
    }

    public ModuleConfiguration s3SecretAccessKey(String awsS3SecretAccessKey) {
        this.s3SecretAccessKey = awsS3SecretAccessKey;
        return this;
    }

    public ModuleConfiguration s3UseTlsConnection(boolean s3UseTlsConnection) {
        this.s3UseTlsConnection = s3UseTlsConnection;
        return this;
    }

    public ModuleConfiguration localS3Endpoint(String s3Endpoint) {
        this.localS3Endpoint = s3Endpoint;
        return this;
    }

    public ModuleConfiguration localS3Port(int s3Port) {
        this.localS3Port = s3Port;
        return this;
    }

    public ModuleConfiguration createBucketIfNotPresentYet(boolean createBucketIfNotExist) {
        this.createBucketIfNotPresentYet = createBucketIfNotExist;
        return this;
    }

    public ModuleConfiguration localS3(boolean localS3) {
        this.localS3 = localS3;
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

    public int getRedisReadyCheckIntervalMs() {
        return redisReadyCheckIntervalMs;
    }

    public boolean isRedisEnableTls() {
        return redisEnableTls;
    }

    public boolean isSsl() {
        return ssl;
    }

    public boolean isTrustAll() {
        return trustAll;
    }

    public String getHostnameVerificationAlgorithm() {
        return hostnameVerificationAlgorithm;
    }

    public String getRedisAuth() {
        return redisAuth;
    }

    public String getRedisPublishMetrcisAddress() {
        return redisPublishMetrcisAddress;
    }

    public String getRedisPublishMetrcisPrefix() {
        return redisPublishMetrcisPrefix;
    }

    public int getRedisPublishMetrcisRefreshPeriodSec() {
        if (redisPublishMetrcisRefreshPeriodSec < 1) {
            log.debug("Ignoring value {}s for redisPublishMetrcisRefreshPersiodSec (too small) and use 1 instead", redisPublishMetrcisRefreshPeriodSec);
            return 1;
        }
        return redisPublishMetrcisRefreshPeriodSec;
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

    public int getMaxStorageExpandSubresources() {
        return maxStorageExpandSubresources;
    }

    public String getAwsS3Region() {
        return awsS3Region;
    }

    public String getS3BucketName() {
        return s3BucketName;
    }

    public String getS3AccessKeyId() {
        return s3AccessKeyId;
    }

    public String getS3SecretAccessKey() {
        return s3SecretAccessKey;
    }

    public boolean getS3UseTlsConnection() {
        return s3UseTlsConnection;
    }

    public String getLocalS3Endpoint() {
        return localS3Endpoint;
    }

    public int getLocalS3Port() {
        return localS3Port;
    }

    public boolean getCreateBucketIfNotPresentYet() {
        return createBucketIfNotPresentYet;
    }

    public boolean isLocalS3() {
        return localS3;
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
