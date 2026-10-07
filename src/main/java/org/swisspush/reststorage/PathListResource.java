package org.swisspush.reststorage;

import java.util.List;

public class PathListResource extends Resource {
    /**
     * Paths returned in this page. Redis listings may contain duplicates within or across pages;
     * clients must deduplicate across the entire iteration when processing each path once.
     */
    public List<String> paths;
    /**
     * Opaque cursor to resume a paginated {@code list} call. A value of {@code 0}
     * indicates that there are no more results to fetch.
     */
    public long nextCursor;
}
