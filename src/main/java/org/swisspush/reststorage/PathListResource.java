package org.swisspush.reststorage;

import java.util.List;

public class PathListResource extends Resource {
    public List<String> paths;
    /**
     * Opaque cursor to resume a paginated {@code list} call. A value of {@code 0}
     * indicates that there are no more results to fetch.
     */
    public long nextCursor;
}
