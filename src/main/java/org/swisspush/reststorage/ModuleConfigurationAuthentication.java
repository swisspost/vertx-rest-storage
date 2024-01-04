package org.swisspush.reststorage;

import io.netty.util.internal.StringUtil;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.authentication.AuthenticationProvider;
import io.vertx.ext.auth.authentication.Credentials;
import io.vertx.ext.auth.authentication.UsernamePasswordCredentials;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.swisspush.reststorage.util.ModuleConfiguration;

import java.util.Objects;

import static org.slf4j.LoggerFactory.getLogger;

/**
 * Custom implementation of a {@link AuthenticationProvider} using credentials from {@link ModuleConfiguration}
 *
 * @author https://github.com/mcweba [Marc-Andre Weber]
 */
public class ModuleConfigurationAuthentication implements AuthenticationProvider {

    private final static Logger logger = getLogger(ModuleConfigurationAuthentication.class);

    private static final String INVALID_CREDENTIALS = "invalid credentials";

    private static class User {
        final String name;
        final String password;

        private User(String name, String password) {
            this.name = Objects.requireNonNull(name);
            this.password = Objects.requireNonNull(password);
        }
    }

    private final User user;

    public ModuleConfigurationAuthentication(ModuleConfiguration configuration) {
        Objects.requireNonNull(configuration);

        String username = configuration.getHttpRequestHandlerUsername();
        String password = configuration.getHttpRequestHandlerPassword();

        if (StringUtil.isNullOrEmpty(username) || StringUtil.isNullOrEmpty(password)) {
            logger.warn("Credentials are missing/empty");
            this.user = null;
        } else {
            this.user = new User(username, password);
        }
    }

    @Override
    public void authenticate(JsonObject authInfo, Handler<AsyncResult<io.vertx.ext.auth.User>> resultHandler) {
        authenticate(new UsernamePasswordCredentials(authInfo), resultHandler);
    }

    @Override
    public void authenticate(Credentials credentials, Handler<AsyncResult<io.vertx.ext.auth.User>> resultHandler) {
        try {
            UsernamePasswordCredentials authInfo = (UsernamePasswordCredentials) credentials;
            authInfo.checkValid(null);

            if(user == null) {
                resultHandler.handle(Future.failedFuture(INVALID_CREDENTIALS));
            } else {
                if (Objects.equals(user.name, authInfo.getUsername())
                        && Objects.equals(user.password, authInfo.getPassword())) {
                    resultHandler.handle(Future.succeededFuture(io.vertx.ext.auth.User.fromName(user.name)));
                } else {
                    resultHandler.handle(Future.failedFuture(INVALID_CREDENTIALS));
                }
            }
        } catch (RuntimeException e) {
            resultHandler.handle(Future.failedFuture(e));
        }
    }
}
