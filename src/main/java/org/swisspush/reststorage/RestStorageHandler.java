package org.swisspush.reststorage;

import io.netty.util.internal.StringUtil;
import io.vertx.core.Handler;
import io.vertx.core.MultiMap;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.streams.Pump;
import io.vertx.ext.auth.authentication.AuthenticationProvider;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BasicAuthHandler;
import org.slf4j.Logger;
import org.swisspush.reststorage.util.*;

import java.text.DecimalFormat;
import java.util.*;

import static org.swisspush.reststorage.util.HttpRequestHeader.*;
import static org.swisspush.reststorage.util.HttpRequestParam.getString;
import static org.swisspush.reststorage.util.HttpRequestParam.*;

public class RestStorageHandler implements Handler<HttpServerRequest> {

    private final Logger log;
    private final Router router;
    private final Storage storage;

    private final MimeTypeResolver mimeTypeResolver = new MimeTypeResolver("application/json; charset=utf-8");

    private final Map<String, String> editors = new LinkedHashMap<>();

    private final String newMarker = "?new=true";
    private final String prefixFixed;
    private final String prefix;
    private final boolean confirmCollectionDelete;
    private final boolean rejectStorageWriteOnLowMemory;
    private final boolean return200onDeleteNonExisting;
    private final DecimalFormat decimalFormat;

    public RestStorageHandler(Vertx vertx, final Logger log, final Storage storage, final ModuleConfiguration config) {
        this.router = Router.router(vertx);
        this.log = log;
        this.storage = storage;
        this.prefix = config.getPrefix();
        this.confirmCollectionDelete = config.isConfirmCollectionDelete();
        this.rejectStorageWriteOnLowMemory = config.isRejectStorageWriteOnLowMemory();
        this.return200onDeleteNonExisting = config.isReturn200onDeleteNonExisting();

        this.decimalFormat = new DecimalFormat();
        this.decimalFormat.setMaximumFractionDigits(1);

        prefixFixed = prefix.equals("/") ? "" : prefix;

        if (config.getEditorConfig() != null) {
            editors.putAll(config.getEditorConfig());
        }

        Result<Boolean, String> result = checkHttpAuthenticationConfiguration(config);
        if(result.isErr()) {
            router.route().handler(ctx -> respondWith(ctx.response(), StatusCode.INTERNAL_SERVER_ERROR, result.getErr()));
        } else if (result.getOk()) {
            AuthenticationProvider authProvider = new ModuleConfigurationAuthentication(config);
                router.route().handler(BasicAuthHandler.create(authProvider));
            log.info("Authentication enabled for HTTP API");
        }

        router.postWithRegex(".*_cleanup").handler(this::cleanup);

        router.postWithRegex(prefixFixed + ".*").handler(this::storageExpand);

        router.getWithRegex(prefixFixed + ".*").handler(this::getResource);

        router.putWithRegex(prefixFixed + ".*").handler(this::putResource);

        router.deleteWithRegex(prefixFixed + ".*").handler(this::deleteResource);

        router.getWithRegex(".*").handler(this::getResourceNotFound);

        router.routeWithRegex(".*").handler(this::respondMethodNotAllowed);
    }

    @Override
    public void handle(HttpServerRequest request) {
        router.handle(request);
    }

    ////////////////////////////
    // Begin Router handling  //
    ////////////////////////////

    private void respondMethodNotAllowed(RoutingContext ctx) {
        respondWithNotAllowed(ctx.request());
    }

    private void cleanup(RoutingContext ctx) {
        if (log.isTraceEnabled()) {
            log.trace("RestStorageHandler cleanup");
        }
        storage.cleanup(documentResource -> {
            if (log.isTraceEnabled()) {
                log.trace("RestStorageHandler cleanup");
            }
            ctx.response().headers().add(CONTENT_LENGTH.getName(), "" + documentResource.length);
            ctx.response().headers().add(CONTENT_TYPE.getName(), "application/json; charset=utf-8");
            ctx.response().setStatusCode(StatusCode.OK.getStatusCode());
            final Pump pump = Pump.pump(documentResource.readStream, ctx.response());
            documentResource.readStream.endHandler(nothing -> {
                documentResource.closeHandler.handle(null);
                ctx.response().end();
            });
            pump.start();
        }, ctx.request().params().get("cleanupResourcesAmount"));
    }

    private void getResourceNotFound(RoutingContext ctx) {
        if (log.isTraceEnabled()) {
            log.trace("RestStorageHandler resource not found: {}", ctx.request().uri());
        }
        ctx.response().setStatusCode(StatusCode.NOT_FOUND.getStatusCode());
        ctx.response().setStatusMessage(StatusCode.NOT_FOUND.getStatusMessage());
        ctx.response().end(StatusCode.NOT_FOUND.toString());
    }

    private void getResource(RoutingContext ctx) {
        final String path = cleanPath(ctx.request().path().substring(prefixFixed.length()));
        final String etag = ctx.request().headers().get(IF_NONE_MATCH_HEADER.getName());
        if (log.isTraceEnabled()) {
            log.trace("RestStorageHandler got GET Request path: {} etag: {}", path, etag);
        }
        MultiMap params = ctx.request().params();
        String offsetFromUrl = getString(params, OFFSET_PARAMETER);
        String limitFromUrl = getString(params, LIMIT_PARAMETER);
        OffsetLimit offsetLimit = UrlParser.offsetLimit(offsetFromUrl, limitFromUrl);
        storage.get(path, etag, offsetLimit.offset, offsetLimit.limit, new Handler<>() {
            public void handle(Resource resource) {
                if (log.isTraceEnabled()) {
                    log.trace("RestStorageHandler resource exists: {}", resource.exists);
                }

                if (resource.error) {
                    ctx.response().setStatusCode(StatusCode.INTERNAL_SERVER_ERROR.getStatusCode());
                    ctx.response().setStatusMessage(StatusCode.INTERNAL_SERVER_ERROR.getStatusMessage());
                    String message = StatusCode.INTERNAL_SERVER_ERROR.getStatusMessage();
                    if (resource.errorMessage != null) {
                        message = resource.errorMessage;
                    }
                    ctx.response().end(message);
                    return;
                }

                if (!resource.modified) {
                    ctx.response().setStatusCode(StatusCode.NOT_MODIFIED.getStatusCode());
                    ctx.response().setStatusMessage(StatusCode.NOT_MODIFIED.getStatusMessage());
                    ctx.response().headers().set(ETAG_HEADER.getName(), etag);
                    ctx.response().headers().add(CONTENT_LENGTH.getName(), "0");
                    ctx.response().end();
                    return;
                }

                if (resource.exists) {
                    String accept = ctx.request().headers().get("Accept");
                    boolean html = (accept != null && accept.contains("text/html"));
                    if (resource instanceof CollectionResource) {
                        if (log.isTraceEnabled()) {
                            log.trace("RestStorageHandler resource is collection: {}", ctx.request().uri());
                        }
                        CollectionResource collection = (CollectionResource) resource;
                        String collectionName = collectionName(path);
                        if (html && !ctx.request().uri().endsWith("/")) {
                            if (log.isTraceEnabled()) {
                                log.trace("RestStorageHandler accept contains text/html and ends with /");
                            }
                            ctx.response().setStatusCode(StatusCode.FOUND.getStatusCode());
                            ctx.response().setStatusMessage(StatusCode.FOUND.getStatusMessage());
                            ctx.response().headers().add("Location", ctx.request().uri() + "/");
                            ctx.response().end();
                        } else if (html) {
                            if (log.isTraceEnabled()) {
                                log.trace("RestStorageHandler accept contains text/html");
                            }
                            if (!(ctx.request().query() != null && ctx.request().query().contains("follow=off")) &&
                                    collection.items.size() == 1 &&
                                    collection.items.get(0) instanceof CollectionResource) {
                                if (log.isTraceEnabled()) {
                                    log.trace("RestStorageHandler query contains follow=off");
                                }
                                ctx.response().setStatusCode(StatusCode.FOUND.getStatusCode());
                                ctx.response().setStatusMessage(StatusCode.FOUND.getStatusMessage());
                                ctx.response().headers().add("Location", (ctx.request().uri()) + collection.items.get(0).name);
                                ctx.response().end();
                                return;
                            }

                            StringBuilder body = new StringBuilder();
                            String editor = null;
                            if (editors.size() > 0) {
                                editor = editors.values().iterator().next();
                            }
                            body.append("<!DOCTYPE html>\n");
                            body.append("<html><head><meta charset='utf-8'/><title>").append(collectionName).append("</title>");
                            body.append("<link href='//netdna.bootstrapcdn.com/twitter-bootstrap/2.3.1/css/bootstrap-combined.min.css' rel='stylesheet'></head>");
                            body.append("<body><div style='font-size: 2em; height:48px; border-bottom: 1px solid lightgray; color: darkgray'><div style='padding:12px;'>").append(htmlPath(prefix + path)).append("</div>");
                            if (editor != null) {
                                String editorString = editor.replace("$path", path + (path.equals("/") ? "" : "/") + "$new");
                                editorString = editorString.replaceFirst("\\?", newMarker);
                                body.append("<div style='position: fixed; top: 8px; right: 20px;'>" +
                                        "<input id='name' type='text' placeholder='New Resource\u2026' onkeydown='if (event.keyCode == 13) { if(document.getElementById(\"name\").value) {window.location=\"" + editorString + "\".replace(\"$new\",document.getElementById(\"name\").value);}}'></input></div>");
                            }
                            body.append("</div><ul style='padding: 12px; font-size: 1.2em;' class='unstyled'><li><a href=\"../?follow=off\">..</a></li>");
                            List<String> sortedNames = sortedNames(collection);
                            ResourceNameUtil.resetReplacedColonsAndSemiColonsInList(sortedNames);
                            for (String name : sortedNames) {
                                body.append("<li><a href=\"" + name + "\">" + name + "</a>");
                                body.append("</li>");
                            }
                            body.append("</ul></body></html>");
                            ctx.response().headers().add(CONTENT_LENGTH.getName(), "" + body.length());
                            ctx.response().headers().add(CONTENT_TYPE.getName(), "text/html; charset=utf-8");
                            ctx.response().end(body.toString());
                        } else {
                            JsonArray array = new JsonArray();
                            List<String> sortedNames = sortedNames(collection);
                            ResourceNameUtil.resetReplacedColonsAndSemiColonsInList(sortedNames);
                            sortedNames.forEach(array::add);
                            if (log.isTraceEnabled()) {
                                log.trace("RestStorageHandler return collection: {}", sortedNames);
                            }
                            String body = new JsonObject().put(collectionName, array).encode();
                            ctx.response().headers().add(CONTENT_LENGTH.getName(), "" + body.length());
                            ctx.response().headers().add(CONTENT_TYPE.getName(), "application/json; charset=utf-8");
                            ctx.response().end(body);
                        }
                    }
                    if (resource instanceof DocumentResource) {
                        if (log.isTraceEnabled()) {
                            log.trace("RestStorageHandler resource is a DocumentResource: {}", ctx.request().uri());
                        }
                        if (ctx.request().uri().endsWith("/")) {
                            if (log.isTraceEnabled()) {
                                log.trace("RestStorageHandler DocumentResource ends with /");
                            }
                            ctx.response().setStatusCode(StatusCode.FOUND.getStatusCode());
                            ctx.response().setStatusMessage(StatusCode.FOUND.getStatusMessage());
                            ctx.response().headers().add("Location", ctx.request().uri().substring(0, ctx.request().uri().length() - 1));
                            ctx.response().end();
                        } else {
                            if (log.isTraceEnabled()) {
                                log.trace("RestStorageHandler DocumentResource does not end with /");
                            }
                            String mimeType = mimeTypeResolver.resolveMimeType(path);
                            if (ctx.request().headers().names().contains("Accept") && ctx.request().headers().get("Accept").contains("text/html")) {
                                String editor = editors.get(mimeType.split(";")[0]);
                                if (editor != null) {
                                    ctx.response().setStatusCode(StatusCode.FOUND.getStatusCode());
                                    ctx.response().setStatusMessage(StatusCode.FOUND.getStatusMessage());
                                    String editorString = editor.replaceAll("\\$path", path);
                                    ctx.response().headers().add("Location", editorString);
                                    ctx.response().end();
                                    return;
                                }
                            }

                            final DocumentResource documentResource = (DocumentResource) resource;
                            if (documentResource.etag != null && !documentResource.etag.isEmpty()) {
                                ctx.response().headers().add(ETAG_HEADER.getName(), documentResource.etag);
                            }
                            ctx.response().headers().add(CONTENT_LENGTH.getName(), "" + documentResource.length);
                            ctx.response().headers().add(CONTENT_TYPE.getName(), mimeType);
                            final Pump pump = Pump.pump(documentResource.readStream, ctx.response());
                            documentResource.readStream.endHandler(nothing -> {
                                documentResource.closeHandler.handle(null);
                                ctx.response().end();
                            });
                            pump.start();
                            // TODO: exception handlers
                        }
                    }
                } else {
                    if (log.isTraceEnabled()) {
                        log.trace("RestStorageHandler Could not find resource: {}", ctx.request().uri());
                    }
                    ctx.response().setStatusCode(StatusCode.NOT_FOUND.getStatusCode());
                    ctx.response().setStatusMessage(StatusCode.NOT_FOUND.getStatusMessage());
                    ctx.response().end(StatusCode.NOT_FOUND.toString());
                }
            }

            private List<String> sortedNames(CollectionResource collection) {
                List<String> collections = new ArrayList<>();
                List<String> documents = new ArrayList<>();
                for (Resource r : collection.items) {
                    String name = r.name;
                    if (r instanceof CollectionResource) {
                        collections.add(name + "/");
                    } else {
                        documents.add(name);
                    }
                }
                collections.addAll(documents);
                return collections;
            }
        });
    }

    private void putResource(RoutingContext ctx) {
        ctx.request().pause();
        final String path = cleanPath(ctx.request().path().substring(prefixFixed.length()));

        MultiMap headers = ctx.request().headers();

        Integer importanceLevel;
        if (containsHeader(headers, IMPORTANCE_LEVEL_HEADER)) {
            importanceLevel = getInteger(headers, IMPORTANCE_LEVEL_HEADER);
            if (importanceLevel == null) {
                ctx.request().resume();
                ctx.response().setStatusCode(StatusCode.BAD_REQUEST.getStatusCode());
                ctx.response().setStatusMessage(StatusCode.BAD_REQUEST.getStatusMessage());
                ctx.response().end("Invalid " + IMPORTANCE_LEVEL_HEADER.getName() + " header: " + headers.get(IMPORTANCE_LEVEL_HEADER.getName()));
                log.error("Rejecting PUT request to {} because {} header, has an invalid value: {}",
                        ctx.request().uri(), IMPORTANCE_LEVEL_HEADER.getName(), headers.get(IMPORTANCE_LEVEL_HEADER.getName()));
                return;
            }

            if (rejectStorageWriteOnLowMemory) {
                Optional<Float> currentMemoryUsage = storage.getCurrentMemoryUsage();
                if (currentMemoryUsage.isPresent()) {
                    if (currentMemoryUsage.get() > importanceLevel) {
                        ctx.request().resume();
                        ctx.response().setStatusCode(StatusCode.INSUFFICIENT_STORAGE.getStatusCode());
                        ctx.response().setStatusMessage(StatusCode.INSUFFICIENT_STORAGE.getStatusMessage());
                        ctx.response().end(StatusCode.INSUFFICIENT_STORAGE.getStatusMessage());
                        log.info("Rejecting PUT request to {} because current memory usage of {}% is higher than " +
                                        "provided importance level of {}%", ctx.request().uri(),
                                decimalFormat.format(currentMemoryUsage.get()), importanceLevel);
                        return;
                    }
                } else {
                    log.warn("Rejecting storage writes on low memory feature disabled, because current memory usage not available");
                }
            } else {
                log.warn("Received request with {} header, but rejecting storage writes on low memory feature " +
                        "is disabled", IMPORTANCE_LEVEL_HEADER.getName());
            }
        } else if (rejectStorageWriteOnLowMemory) {
            log.debug("Received PUT request to {} without {} header. Going to handle this request with highest importance",
                    ctx.request().uri(), IMPORTANCE_LEVEL_HEADER.getName());
        }

        Long expire = -1L; // default infinit
        if (containsHeader(headers, EXPIRE_AFTER_HEADER)) {
            expire = getLong(headers, EXPIRE_AFTER_HEADER);
            if (expire == null) {
                ctx.request().resume();
                ctx.response().setStatusCode(StatusCode.BAD_REQUEST.getStatusCode());
                ctx.response().setStatusMessage("Invalid " + EXPIRE_AFTER_HEADER.getName() + " header: " + headers.get(EXPIRE_AFTER_HEADER.getName()));
                ctx.response().end(ctx.response().getStatusMessage());
                log.error("{} header, invalid value: {}", EXPIRE_AFTER_HEADER.getName(), ctx.response().getStatusMessage());
                return;
            }
        }

        if (log.isTraceEnabled()) {
            log.trace("RestStorageHandler put resource: {} with expire: {}", ctx.request().uri(), expire);
        }

        String lock = "";
        Long lockExpire = 300L; // default 300s
        LockMode lockMode = LockMode.SILENT; // default

        if (containsHeader(headers, LOCK_HEADER)) {
            lock = headers.get(LOCK_HEADER.getName());

            if (containsHeader(headers, LOCK_MODE_HEADER)) {
                try {
                    lockMode = LockMode.valueOf(headers.get(LOCK_MODE_HEADER.getName()).toUpperCase());
                } catch (IllegalArgumentException e) {
                    ctx.request().resume();
                    ctx.response().setStatusCode(StatusCode.BAD_REQUEST.getStatusCode());
                    ctx.response().setStatusMessage("Invalid " + LOCK_MODE_HEADER.getName() + " header: " + headers.get(LOCK_MODE_HEADER.getName()));
                    ctx.response().end(ctx.response().getStatusMessage());
                    log.error("{} header, invalid value: {}", LOCK_MODE_HEADER.getName(), ctx.response().getStatusMessage());
                    return;
                }
            }

            if (containsHeader(headers, LOCK_EXPIRE_AFTER_HEADER)) {
                lockExpire = getLong(headers, LOCK_EXPIRE_AFTER_HEADER);
                if (lockExpire == null) {
                    ctx.request().resume();
                    ctx.response().setStatusCode(StatusCode.BAD_REQUEST.getStatusCode());
                    ctx.response().setStatusMessage("Invalid " + LOCK_EXPIRE_AFTER_HEADER.getName() + " header: " +
                            headers.get(LOCK_EXPIRE_AFTER_HEADER.getName()));
                    ctx.response().end(ctx.response().getStatusMessage());
                    log.error("{} header, invalid value: {}", LOCK_EXPIRE_AFTER_HEADER.getName(), ctx.response().getStatusMessage());
                    return;
                }
            }
        }

        boolean merge = (ctx.request().query() != null && ctx.request().query().contains("merge=true")
                && mimeTypeResolver.resolveMimeType(path).contains("application/json"));

        final String etag = headers.get(IF_NONE_MATCH_HEADER.getName());

        boolean storeCompressed = Boolean.parseBoolean(headers.get(COMPRESS_HEADER.getName()));

        if (merge && storeCompressed) {
            ctx.request().resume();
            ctx.response().setStatusCode(StatusCode.BAD_REQUEST.getStatusCode());
            ctx.response().setStatusMessage("Invalid parameter/header combination: merge parameter and " +
                    COMPRESS_HEADER.getName() + " header cannot be used concurrently");
            ctx.response().end(ctx.response().getStatusMessage());
            return;
        }

        storage.put(path, etag, merge, expire, lock, lockMode, lockExpire, storeCompressed, resource -> {
            final HttpServerResponse response = ctx.response();
            ctx.request().resume();

            if (resource.error) {
                final String message = (resource.errorMessage != null)
                        ? resource.errorMessage
                        : StatusCode.INTERNAL_SERVER_ERROR.getStatusMessage();
                respondWith(response, StatusCode.INTERNAL_SERVER_ERROR, message);
            } else if (resource.rejected) {
                // TODO: Describe how 'rejected' maps to 'CONFLICT'.
                respondWith(response, StatusCode.CONFLICT, null);
            } else if (!resource.modified) {
                // TODO: Describe how 'not modified' relates to those headers.
                response.headers()
                        .set(ETAG_HEADER.getName(), etag)
                        .add(CONTENT_LENGTH.getName(), "0")
                ;
                respondWith(response, StatusCode.NOT_MODIFIED, null);
            } else if (resource instanceof CollectionResource) {
                // Its not allowed to override an existing collection by a resource. A
                // collection only can be GET or DELETE.
                response.headers()
                        .add("Allow", "GET, DELETE")
                ;
                respondWith(response, StatusCode.METHOD_NOT_ALLOWED, null);
            } else if (resource instanceof DocumentResource) {
                if (!resource.exists) {
                    // We'll arrive here when we try to put "/one/two/three" but "/one/two" already
                    // exists as a resource.
                    // See: "https://github.com/swisspush/vertx-rest-storage/blob/v2.5.7/src/main/java/org/swisspush/reststorage/RedisStorage.java#L837".
                    // May there are also other cases this can happen. But I don't know about them.
                    response.headers()
                            .add("Allow", "GET, DELETE")
                    ;
                    respondWith(response, StatusCode.METHOD_NOT_ALLOWED, null);
                } else {
                    // All checks successful. We'll now store contents of the resource.
                    putResource_storeContentsOfDocumentResource(ctx, (DocumentResource) resource);
                }
            } else {
                // Cannot happen (Or at least theoretically it shouldn't). But in case someone
                // manages creating such a request somehow, we at least should properly
                // finalize our request.
                final HttpServerRequest request = ctx.request();
                log.error("Unexpected case during 'PUT {}'", request.path());
                respondWith(response, StatusCode.INTERNAL_SERVER_ERROR, "Unexpected case during PUT");
            }
        });
    }

    /**
     * <p>Helper method which completes response in case a {@link DocumentResource}
     * got PUT to storage.</p>
     *
     * <p>This method doesn't perform correctness checks for passed arguments. The
     * Caller is responsible to only call this method, if the resource is ready to
     * be stored.</p>
     */
    private void putResource_storeContentsOfDocumentResource(RoutingContext ctx, DocumentResource resource) {
        final HttpServerResponse response = ctx.response();

        // Caller is responsible to do any 'error', 'exists', 'rejected' checks on the
        // resource. Therefore we simply go forward and store its content.
        final HttpServerRequest request = ctx.request();
        resource.addErrorHandler(error -> respondWith(response, StatusCode.INTERNAL_SERVER_ERROR, error.getMessage()));
        // Complete response when resource written.
        resource.endHandler = event -> response.end();
        // Close resource when payload fully read.
        request.endHandler(v -> resource.closeHandler.handle(null));
        request.exceptionHandler(exc -> {
            // Report error
            // TODO: Evaluate which properties to set. Public interface documentation of
            //       DocumentResource is de-facto non-existent. Therefore I've no idea
            //       which properties to set how in this case here.
            resource.error = true;
            resource.errorMessage = exc.getMessage();
            // Notify error handler.
            final Handler<Throwable> resourceErrorHandler = resource.errorHandler;
            if (resourceErrorHandler != null) {
                resourceErrorHandler.handle(exc);
            }
        });
        final Pump pump = Pump.pump(request, resource.writeStream);
        pump.start();
    }

    private void deleteResource(RoutingContext ctx) {
        final String path = cleanPath(ctx.request().path().substring(prefixFixed.length()));
        if (log.isTraceEnabled()) {
            log.trace("RestStorageHandler delete resource: {}", ctx.request().uri());
        }

        String lock = "";
        Long lockExpire = 300L; // default 300s
        LockMode lockMode = LockMode.SILENT; // default

        MultiMap headers = ctx.request().headers();
        MultiMap params = ctx.request().params();

        if (containsHeader(headers, LOCK_HEADER)) {
            lock = headers.get(LOCK_HEADER.getName());

            if (containsHeader(headers, LOCK_MODE_HEADER)) {
                try {
                    lockMode = LockMode.valueOf(headers.get(LOCK_MODE_HEADER.getName()).toUpperCase());
                } catch (IllegalArgumentException e) {
                    ctx.request().resume();
                    ctx.response().setStatusCode(StatusCode.BAD_REQUEST.getStatusCode());
                    ctx.response().setStatusMessage("Invalid " + LOCK_MODE_HEADER.getName() + " header: " + headers.get(LOCK_MODE_HEADER.getName()));
                    ctx.response().end(ctx.response().getStatusMessage());
                    log.error("{} header, invalid value: {}", LOCK_MODE_HEADER.getName(), ctx.response().getStatusMessage());
                    return;
                }
            }

            if (containsHeader(headers, LOCK_EXPIRE_AFTER_HEADER)) {
                lockExpire = getLong(headers, LOCK_EXPIRE_AFTER_HEADER);
                if (lockExpire == null) {
                    ctx.request().resume();
                    ctx.response().setStatusCode(StatusCode.BAD_REQUEST.getStatusCode());
                    ctx.response().setStatusMessage("Invalid " + LOCK_EXPIRE_AFTER_HEADER.getName() + " header: " +
                            headers.get(LOCK_EXPIRE_AFTER_HEADER.getName()));
                    ctx.response().end(ctx.response().getStatusMessage());
                    log.error("{} header, invalid value: {}", LOCK_EXPIRE_AFTER_HEADER.getName(), ctx.response().getStatusMessage());
                    return;
                }
            }
        }

        storage.delete(path, lock, lockMode, lockExpire, confirmCollectionDelete, getBoolean(params, RECURSIVE_PARAMETER),
                resource -> {
                    if (resource.rejected) {
                        ctx.response().setStatusCode(StatusCode.CONFLICT.getStatusCode());
                        ctx.response().setStatusMessage(StatusCode.CONFLICT.getStatusMessage());
                        ctx.response().end();
                    } else if (resource.error) {
                        ctx.response().setStatusCode(StatusCode.BAD_REQUEST.getStatusCode());
                        ctx.response().setStatusMessage(StatusCode.BAD_REQUEST.getStatusMessage());
                        String message = StatusCode.BAD_REQUEST.getStatusMessage();
                        if (resource.errorMessage != null) {
                            message = message + ": " + resource.errorMessage;
                        }
                        ctx.response().end(message);
                    } else if (!resource.exists) {
                        if (return200onDeleteNonExisting) {
                            ctx.response().end(); // just say "200 OK" - ignore that the resource-to-be-deleted was not present
                        } else {
                            ctx.request().response().setStatusCode(StatusCode.NOT_FOUND.getStatusCode());
                            ctx.request().response().setStatusMessage(StatusCode.NOT_FOUND.getStatusMessage());
                            ctx.request().response().end(StatusCode.NOT_FOUND.toString());
                        }
                    } else {
                        ctx.request().response().end();
                    }
                });
    }

    private void storageExpand(RoutingContext ctx) {
        if (!containsParam(ctx.request().params(), STORAGE_EXPAND_PARAMETER)) {
            respondWithNotAllowed(ctx.request());
        } else {
            ctx.request().bodyHandler(event -> {
                List<String> subResourceNames = new ArrayList<>();
                try {
                    JsonObject body = new JsonObject(event.toString());
                    JsonArray subResourcesArray = body.getJsonArray("subResources");
                    if (subResourcesArray == null) {
                        respondWithBadRequest(ctx.request(), "Bad Request: Expected array field 'subResources' with names of resources");
                        return;
                    }

                    for (int i = 0; i < subResourcesArray.size(); i++) {
                        subResourceNames.add(subResourcesArray.getString(i));
                    }
                    ResourceNameUtil.replaceColonsAndSemiColonsInList(subResourceNames);
                } catch (RuntimeException ex) {
                    respondWithBadRequest(ctx.request(), "Bad Request: Unable to parse body of storageExpand POST request");
                    return;
                }

                final String path = cleanPath(ctx.request().path().substring(prefixFixed.length()));
                final String etag = ctx.request().headers().get(IF_NONE_MATCH_HEADER.getName());
                storage.storageExpand(path, etag, subResourceNames, resource -> {

                    if (resource.error) {
                        ctx.response().setStatusCode(StatusCode.CONFLICT.getStatusCode());
                        ctx.response().setStatusMessage(StatusCode.CONFLICT.getStatusMessage());
                        String message = StatusCode.CONFLICT.getStatusMessage();
                        if (resource.errorMessage != null) {
                            message = resource.errorMessage;
                        }
                        ctx.response().end(message);
                        return;
                    }

                    if (resource.invalid) {
                        ctx.response().setStatusCode(StatusCode.INTERNAL_SERVER_ERROR.getStatusCode());
                        ctx.response().setStatusMessage(StatusCode.INTERNAL_SERVER_ERROR.getStatusMessage());

                        String message = StatusCode.INTERNAL_SERVER_ERROR.getStatusMessage();
                        if (resource.invalidMessage != null) {
                            message = resource.invalidMessage;
                        }
                        ctx.response().end(new JsonObject().put("error", message).encode());
                        return;
                    }

                    if (!resource.modified) {
                        ctx.response().setStatusCode(StatusCode.NOT_MODIFIED.getStatusCode());
                        ctx.response().setStatusMessage(StatusCode.NOT_MODIFIED.getStatusMessage());
                        ctx.response().headers().set(ETAG_HEADER.getName(), etag);
                        ctx.response().headers().add(CONTENT_LENGTH.getName(), "0");
                        ctx.response().end();
                        return;
                    }

                    if (resource.exists) {
                        if (log.isTraceEnabled()) {
                            log.trace("RestStorageHandler resource is a DocumentResource: {}", ctx.request().uri());
                        }

                        String mimeType = mimeTypeResolver.resolveMimeType(path);
                        final DocumentResource documentResource = (DocumentResource) resource;
                        if (documentResource.etag != null && !documentResource.etag.isEmpty()) {
                            ctx.response().headers().add(ETAG_HEADER.getName(), documentResource.etag);
                        }
                        ctx.response().headers().add(CONTENT_LENGTH.getName(), "" + documentResource.length);
                        ctx.response().headers().add(CONTENT_TYPE.getName(), mimeType);
                        final Pump pump = Pump.pump(documentResource.readStream, ctx.response());
                        documentResource.readStream.endHandler(nothing -> {
                            documentResource.closeHandler.handle(null);
                            ctx.response().end();
                        });
                        pump.start();
                        // TODO: exception handlers

                    } else {
                        if (log.isTraceEnabled()) {
                            log.trace("RestStorageHandler Could not find resource: {}", ctx.request().uri());
                        }
                        ctx.response().setStatusCode(StatusCode.NOT_FOUND.getStatusCode());
                        ctx.response().setStatusMessage(StatusCode.NOT_FOUND.getStatusMessage());
                        ctx.response().end(StatusCode.NOT_FOUND.toString());
                    }
                });
            });
        }
    }

    ////////////////////////////
    // End Router handling    //
    ////////////////////////////

    private Result<Boolean, String> checkHttpAuthenticationConfiguration(ModuleConfiguration modConfig) {
        if(modConfig.isHttpRequestHandlerAuthenticationEnabled()) {
            if(StringUtil.isNullOrEmpty(modConfig.getHttpRequestHandlerUsername()) ||
                    StringUtil.isNullOrEmpty(modConfig.getHttpRequestHandlerPassword())) {
                String msg = "HTTP API authentication is enabled but credentials are missing";
                log.warn(msg);
                return Result.err(msg);
            }
            return Result.ok(true);
        }
        return Result.ok(false);
    }

    private String cleanPath(String value) {
        value = value.replaceAll("\\.\\.", "").replaceAll("\\/\\/", "/");
        while (value.endsWith("/")) {
            value = value.substring(0, value.length() - 1);
        }
        if (value.isEmpty()) {
            return "/";
        }
        return value;
    }

    public static class OffsetLimit {
        public OffsetLimit(int offset, int limit) {
            this.offset = offset;
            this.limit = limit;
        }

        public int offset;
        public int limit;
    }

    private void respondWithNotAllowed(HttpServerRequest request) {
        respondWith(request.response(), StatusCode.METHOD_NOT_ALLOWED, null);
    }

    private void respondWithBadRequest(HttpServerRequest request, String responseMessage) {
        respondWith(request.response(), StatusCode.BAD_REQUEST, responseMessage);
    }

    private void respondWith(HttpServerResponse response, StatusCode statusCode, String responseBody) {
        response.setStatusCode(statusCode.getStatusCode());
        response.setStatusMessage(statusCode.getStatusMessage());
        if (responseBody != null) {
            response.end(responseBody);
        } else {
            response.end();
        }
    }

    private String collectionName(String path) {
        if (path.equals("/") || path.equals("")) {
            return "root";
        } else {
            return path.substring(path.lastIndexOf("/") + 1);
        }
    }

    private String htmlPath(String path) {
        if (path.equals("/")) {
            return "/";
        }
        StringBuilder sb = new StringBuilder();
        StringBuilder p = new StringBuilder();
        String[] parts = path.split("/");
        for (int i = 0; i < parts.length; i++) {
            String part = parts[i];
            p.append(part);
            p.append("/");
            if (i < parts.length - 1) {
                sb.append(" <a href=\"");
                sb.append(p);
                sb.append("?follow=off\">");
                sb.append(part);
                sb.append("</a> > ");
            } else {
                sb.append(" ");
                sb.append(part);
            }
        }
        return sb.toString();
    }
}
