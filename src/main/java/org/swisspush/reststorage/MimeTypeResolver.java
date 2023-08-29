package org.swisspush.reststorage;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class MimeTypeResolver {

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
            } catch (IOException e) {
                // Ignore
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
