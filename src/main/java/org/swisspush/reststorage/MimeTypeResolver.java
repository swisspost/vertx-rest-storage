package org.swisspush.reststorage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.slf4j.LoggerFactory.getLogger;

public class MimeTypeResolver {

    private static final Logger log = getLogger(MimeTypeResolver.class);
    private Map<String, String> mimeTypes = new HashMap<>();
    
    private final String defaultMimeType;
    
    public MimeTypeResolver(String defaultMimeType) {
        this.defaultMimeType = defaultMimeType;
        Properties props = new Properties();
        InputStream in = this.getClass().getClassLoader().getResourceAsStream("mime-types.properties");
        try {            
            props.load(in);
        } catch (IOException e) {            
            throw new RuntimeException(e);
        } finally {
            try {
                in.close();
            } catch (IOException ex) {
                log.debug("close() failed", ex);
            }
        }
        
        for( Map.Entry<Object, Object> entry : props.entrySet()) {
            mimeTypes.put(((String)entry.getKey()).toLowerCase(), (String)entry.getValue());
        }        
    }
    
    public String resolveMimeType(String path) {
        int lastSlash = path.lastIndexOf("/");
        String part = path;
        if(lastSlash >= 0 && !path.endsWith("/")) {
            part = part.substring(lastSlash+1);
        }
        int dot = part.lastIndexOf(".");
        if(dot == -1 || part.endsWith(".")) {
            return defaultMimeType;
        } else {
            String extension = part.substring(dot+1);
            String type = mimeTypes.get(extension.toLowerCase());
            if(type==null) {
                type = "text/plain";
            }
            return type;
        }
    }
}
