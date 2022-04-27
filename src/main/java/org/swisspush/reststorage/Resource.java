package org.swisspush.reststorage;

import io.vertx.core.Handler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;


public class Resource implements Comparable<Resource>{
    public String name;
    public boolean exists = true;
    public boolean modified = true;
    public boolean invalid = false;
    public boolean rejected = false;
    public boolean error = false;
    public String invalidMessage;
    public String errorMessage;

    public Handler<Throwable> errorHandler;

    @Override
    public int compareTo(Resource o) {
        return this.name.compareTo(o.name);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((name == null) ? 0 : name.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        Resource other = (Resource) obj;
        if (name == null) {
            if (other.name != null)
                return false;
        } else if (!name.equals(other.name))
            return false;
        return true;
    }

    public void addErrorHandler(Handler<Throwable> handler) {
        if (errorHandler instanceof EventEmitter) {
            ((EventEmitter) errorHandler).addHandler(handler);
        } else {
            errorHandler = new EventEmitter<>() {{
                if (errorHandler != null) addHandler(errorHandler);
                addHandler(handler);
            }};
        }
    }

}


/**
 * <p>Simple event dispatcher.</p>
 *
 * @param <T>
 *      Event type.
 */
class EventEmitter<T> implements Handler<T> {

    private static final Logger log = LoggerFactory.getLogger(EventEmitter.class);

    /**
     * <p>Delegated propagated event to each caller. Keep in mind: Every handler
     * will receive same event instance!</p>
     */
    @Override
    public void handle(T event) {
        for (Handler<T> handler : handlers) {
            try {
                handler.handle(event);
            } catch (Exception e) {
                log.error("Exception thrown in event handler.", e);
            }
        }
    }

    /**
     * @param handler
     *      Handler to receive events with.
     */
    public void addHandler(Handler<T> handler) {
        //if( handler == null ){ throw new IllegalArgumentException("Arg 'handler' MUST NOT be null."); }
        handlers.add(handler);
    }

    private final List<Handler<T>> handlers = new ArrayList<>();

}
