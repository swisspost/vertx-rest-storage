# vertx-rest-storage

[![Java CI with Maven](https://github.com/swisspost/vertx-rest-storage/actions/workflows/maven.yml/badge.svg)](https://github.com/swisspost/vertx-rest-storage/actions/workflows/maven.yml)
[![codecov](https://codecov.io/gh/swisspost/vertx-rest-storage/branch/master/graph/badge.svg?token=ufyTWmGbC6)](https://codecov.io/gh/swisspost/vertx-rest-storage)
[![GitHub contributors](https://img.shields.io/github/contributors/swisspost/vertx-rest-storage)](https://github.com/swisspost/vertx-rest-storage/graphs/contributors)

[![GitHub release](https://img.shields.io/github/v/release/swisspost/vertx-rest-storage)](https://github.com/swisspost/vertx-rest-storage/releases/latest)
[![Maven Central](https://img.shields.io/maven-central/v/org.swisspush/rest-storage.svg)]()

Persistence for REST resources in the filesystem, Aws S3 storage or a redis database. 

Stores resources in a hierarchical way according to their URI. It actually implements a generic CRUD REST service.

It uses usual mime mapping to determine content type, so you can also use it as a web server. Without extension, JSON is assumed.

The following methods are supported on leaves (documents):
* GET: Returns the content of the resource.
* PUT: Stores the request body in the resource.
* DELETE: Deletes the resource.

The following methods are supported on intermediate nodes (collections):
* GET: Returns the list of collection members. Serves JSON and HTML representations.
* POST (StorageExpand): Returns the expanded content of the sub resources of the (collection) resource. The depth is limited to 1 level. See description below
* DELETE: Delete the collection and all its members.

Runs either as a module or can be integrated into an existing application by instantiating the RestStorageHandler class directly.

## Run it
1. clone the repository
2. Install and start Redis
  * Debian/Ubuntu: `apt-get install redis-server`
  * Fedora/RedHat/CentOS: `yum install redis`
  * OS X: `brew install redis`
  * [Windows](https://github.com/MSOpenTech/redis/releases/download/win-2.8.2400/Redis-x64-2.8.2400.zip)
  * [Other](http://redis.io/download)
3. run `mvn install -Dmaven.test.skip=true`
4. run the fatjar with `java -jar build/libs/rest-storage-x.x.x-all.jar
5. you get a rest-storage, that stores to the filesystem in the directory where you started it. If you want to use the rest-storage with redis, you have to pass the configuration over a json file with `-conf conf.json`

## Features
### GET
Invoking GET request on a leave (document) returns the content of the resource.
> GET /storage/resources/resource_1

Invoking GET request on a collection returns a list of collection members.
> GET /storage/resources/

#### Parameters

| Parameter | Description                                                                                                     |
|:----------|:----------------------------------------------------------------------------------------------------------------|
| limit     | defines the amount of returned resources                                                                        |
| offset    | defines the amount of resources to skip. Can be used in combination with limit to provide paging functionality |

##### Examples
Given a collection of ten items (res1-res10) under the path /server/tests/offset/resources/

| Request                                                   | Returned items                                |
|:----------------------------------------------------------|:----------------------------------------------|
| **GET** /server/tests/offset/resources/?limit=10          | all                                           |
| **GET** /server/tests/offset/resources/?limit=99          | all                                           |
| **GET** /server/tests/offset/resources/?limit=5           | res1,res10,res2,res3,res4                     |
| **GET** /server/tests/offset/resources/?offset=2          | res2,res3,res4,res5,res6,res7,res8,res9       |
| **GET** /server/tests/offset/resources/?offset=11         | no items (empty array)                        |
| **GET** /server/tests/offset/resources/?offset=2&limit=-1 | res2,res3,res4,res5,res6,res7,res8,res9       |
| **GET** /server/tests/offset/resources/?offset=0&limit=3  | res1,res10,res2                               |
| **GET** /server/tests/offset/resources/?offset=1&limit=10 | res10,res2,res3,res4,res5,res6,res7,res8,res9 |

The returned json response look like this:

```json
{
  "resources": [
    "res1",
    "res10",
    "res2",
    "res3",
    "res4"
  ]
}
```

### DELETE
Invoking DELETE request on a leave (document) deletes the resource.
> DELETE /storage/resources/resource_1

Invoking DELETE request on a collection deletes the collection and all its children.
> DELETE /storage/resources/

#### Parameters

| Parameter | Description                                                                                                                                       |
|:----------|:--------------------------------------------------------------------------------------------------------------------------------------------------|
| recursive | When configuration property _confirmCollectionDelete_ is set to _true_, the url parameter _recursive=true_ has to be added to delete collections. |

### StorageExpand

The StorageExpand feature expands the hierarchical resources and returns them as a single concatenated json resource.

Having the following resources in the storage:

```sh
key: data:test:collection:resource1     value: {"myProp1": "myVal1"}
key: data:test:collection:resource2     value: {"myProp2": "myVal2"}
key: data:test:collection:resource3     value: {"myProp3": "myVal3"}
```
would lead to this result

```json
{
    "collection" : {
        "resource1" : {
            "myProp1": "myVal1"
        },
        "resource2" : {
            "myProp2": "myVal2"
        },
        "resource3" : {
            "myProp3": "myVal3"
        }
    }
}
```
    
##### Usage

To use the StorageExpand feature you have to make a POST request to the desired collection to expand having the url parameter **storageExpand=true**. Also, you wil have
to send the names of the sub resources in the body of the request. Using the example above, the request would look like this:

**POST /yourStorageURL/collection?storageExpand=true** with the body:
```json
{
    "subResources" : ["resource1", "resource2", "resource3"]
}
```
The amount of sub resources that can be provided is defined in the configuration by the property _maxStorageExpandSubresources_.

To override this for a single request, add the following request header with an appropriate value:
> x-max-expand-resources: 1500


### Reject PUT requests on low memory (redis only)
The redis storage provides a feature to reject PUT requests when the memory gets low. The information about the used memory is provided by the
redis _INFO_ command.

`Attention:` The stats received by the _INFO_ command depend on the redis version. The required stats are _used_memory_ and _total_system_memory_. Without these stats, the feature is disabled!

#### Configuration
To enable the feature, set the _rejectStorageWriteOnLowMemory_ property (ModuleConfiguration) to _true_. Additionally, the _freeMemoryCheckIntervalMs_ property can be
changed to modify the interval for current memory usage calculation.

#### Usage
To define the _importance level_ of PUT requests, add the following header:
> x-importance-level: 75

The value defines the percentage of used memory which is not allowed to be exceeded to accept the PUT request. In the example above, the Request will only be acepted when
the currently used memory is lower than 75%. When the currently used memory is higher than 75%, the request will be rejected with a status code _507 Insufficient Storage_.

The higher the _x-importance-level_ value, the more important the request. When no _x-importance-level_ header is provided, the request is handled with the highest importance.

### Lock Mechanism
The lock mechanism allows you to lock a resource for a specified time. This way only the owner of the lock is able to write or delete the given resource.
To lock a resource, you have to add the following headers to your PUT / DELETE request.

| Headers             | Type   | Default value | Description                                                                                                           |
|:--------------------|:-------|:--------------|:----------------------------------------------------------------------------------------------------------------------|
| x-lock              | String |               | The owner of the lock.                                                                                                |
| x-lock-mode         | silent | **silent**    | Any PUT or DELETE performed on this resource without the valid owner will have no effect and get *200 OK* back.       |
|                     | reject |               | Any PUT or DELETE performed on this resource without the valid owner will have no effect and get *409 Conflict* back. |
| x-lock-expire-after | long   | **300**       | Defines the lock lifetime. The default value is set to *300* seconds.                                                 |
| x-expire-after      | long   |               | Defines the lifetime of a resource                                                                                    |

`Warning:` The lock will always be removed if you perform a DELETE on a collection containing a locked resource. There is no check for locks in collections.

### Store data compressed
In order to optimize the memory usage when using the redis storage, it's possible to store resources compressed using
the gzip compression algorithm.

To store a resource compressed, add the following header to the PUT request:
> x-stored-compressed: true

When making a GET request to a compressed resource, the resource will be uncompressed before returning. No additional header is required!

**Restrictions**

The data compression feature is not compatible with all vertx-rest-storage features. The following listing contains the restrictions of this feature: 
* Data compression is available in redis storage only
* Data compression cannot be used with _merge=true_ url parameter concurrently. Such PUT requests will be rejected.
* Compressed resources cannot be used in _storageExpand_ requests. _storageExpand_ requests to a collection containing a compressed resource will be rejected.
* If a resource is already stored in a different compression state (state = not compressed, compressed) as the compression of sent resource, the stored resource will be overwritten in every case. Like this we prevent unexpected behaviour considering the etag mechanism. 

## Configuration

The following configuration values are available:

| Property                                | Type   | Default value            | Description                                                                                                                           | 
|:----------------------------------------|:-------|:-------------------------|:--------------------------------------------------------------------------------------------------------------------------------------|
| root                                    | common | .                        | The prefix for the directory or redis key                                                                                             |
| storageType                             | common | filesystem               | The storage implementation to use. Choose between filesystem or redis                                                                 |
| port                                    | common | 8989                     | The port the mod listens to when HTTP API is enabled.                                                                                 |
| httpRequestHandlerEnabled               | common | true                     | When set to _false_, the storage is accessible throught the event bus only.                                                           |
| httpRequestHandlerAuthenticationEnabled | common | false                    | Enable / disable authentication for the HTTP API                                                                                      |
| httpRequestHandlerUsername              | common |                          | The username for the HTTP API authentication                                                                                          |
| httpRequestHandlerPassword              | common |                          | The password for the HTTP API authentication                                                                                          |
| prefix                                  | common | /                        | The part of the URL path before this handler (aka "context path" in JEE terminology)                                                  |
| storageAddress                          | common | resource-storage         | The eventbus address the mod listens to.                                                                                              |
| editorConfig                            | common |                          | Additional configuration values for the editor                                                                                        |
| confirmCollectionDelete                 | common | false                    | When set to _true_, an additional _recursive=true_ url parameter has to be set to delete collections                                  |
| maxStorageExpandSubresources            | common | 1000                     | The amount of sub resources to expand. When limit exceeded, _413 Payload Too Large_ is returned                                       |
| redisHost                               | redis  | localhost                | The host where redis is running on                                                                                                    |
| redisPort                               | redis  | 6379                     | The port where redis is running on                                                                                                    |
| redisReconnectAttempts                  | redis  | 0                        | The amount of reconnect attempts when connection to redis is lost. Use _-1_ for continuous reconnects or _0_ for no reconnects at all |
| redisReconnectDelaySec                  | redis  | 30                       | The delay (in seconds) between each reconnect attempt                                                                                 |
| redisPoolRecycleTimeoutMs               | redis  | 180000                   | The timeout [ms] when the connection pool is recycled. Use **-1** when having reconnect feature enabled.                              |
| expirablePrefix                         | redis  | rest-storage:expirable   | The prefix for expirable data redis keys                                                                                              |
| resourcesPrefix                         | redis  | rest-storage:resources   | The prefix for resources redis keys                                                                                                   |
| collectionsPrefix                       | redis  | rest-storage:collections | The prefix for collections redis keys                                                                                                 |
| deltaResourcesPrefix                    | redis  | delta:resources          | The prefix for delta resources redis keys                                                                                             |
| deltaEtagsPrefix                        | redis  | delta:etags              | The prefix for delta etags redis keys                                                                                                 |
| lockPrefix                              | redis  | rest-storage:locks       | The prefix for lock redis keys                                                                                                        |
| resourceCleanupAmount                   | redis  | 100000                   | The maximum amount of resources to clean in a single cleanup run                                                                      |
| resourceCleanupIntervalSec              | redis  |                          | The interval (in seconds) how often to perform the storage cleanup. When set to _null_ no periodic storage cleanup is performed       |
| rejectStorageWriteOnLowMemory           | redis  | false                    | When set to _true_, PUT requests with the x-importance-level header can be rejected when memory gets low                              |
| freeMemoryCheckIntervalMs               | redis  | 60000                    | The interval in milliseconds to calculate the actual memory usage                                                                     |
| redisReadyCheckIntervalMs               | redis  | -1                       | The interval in milliseconds to calculate the "ready state" of redis. When value < 1, no "ready state" will be calculated             |
| awsS3Region                             | aws-s3 |                          | The region of S3 server                                                                                                               |
| awsS3BucketName                         | aws-s3 |                          | The S3 bucket name                                                                                                                    |
| awsS3AccessKeyId                        | aws-s3 |                          | The AWS access key Id                                                                                                                 |
| awsS3SecretAccessKey                    | aws-s3 |                          | The AWS secret access key                                                                                                             |

### Configuration util

The configurations have to be passed as JsonObject to the module. For a simplified configuration the _ModuleConfigurationBuilder_ can be used.

Example:

```java
ModuleConfiguration config = with()
		.redisHost("anotherhost")
		.redisPort(1234)
		.editorConfig(new JsonObject().put("myKey", "myValue"))
		.build();

JsonObject json = config.asJsonObject();
```

Properties not overridden will not be changed. Thus remaining default.

To use default values only, the _ModuleConfiguration_ constructor without parameters can be used:

```java
JsonObject json  = new ModuleConfiguration().asJsonObject();
```

## Storage types
Currently, there are three storage types supported. File system storage, S3 File storage and redis storage.

### File System Storage
The data is stored hierarchically on the file system. This is the default storage type when not overridden in the configuration.

### S3 File storage
The data is stored in a S3 instance.
Before use, need sign up in AWS service at https://docs.aws.amazon.com/SetUp/latest/UserGuide/setup-AWSsignup.html
you also need create bucket from S3 web console first

### Redis Storage
The data is stored in a redis database.
Caution: The redis storage implementation does not currently support streaming. Avoid transferring too big payloads since they will be entirely copied in memory.

## Dependencies
- Starting from 2.6.x rest-storage requires **Java 11**.

- This module uses Vert.x v3.3.3 (or later), so **Java 8** is required.
