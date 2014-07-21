package collene;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Holds on to file meta information (currently just the length), flushing it when told.
 */
public class RowMeta {
    public static final long ROW_LENGTH = 0;
    
    private final IO io;
    private final Map<String, Long> cache = new HashMap<String, Long>();
    private final Set<String> dirty = new HashSet<String>();
    
    public RowMeta(IO io) {
        this.io = io;
    }
    
    public long getLength(String key) throws IOException {
        if (cache.containsKey(key)) {
            return cache.get(key);
        } else {
            byte[] buf = io.get(key, ROW_LENGTH);
            assert buf.length == 8;
            long length = Utils.bytesToLong(buf);
            cache.put(key, length);
            return length;
        }
    }
    
    public void setLength(String key, long length, boolean commit) throws IOException {
        cache.put(key, length);
        if (commit) {
            byte[] buf = Utils.longToBytes(length);
            assert buf.length == 8;
            io.put(key, ROW_LENGTH, buf);
        } else {
            dirty.add(key);
        }
    }
    
    public void flush(boolean clear) throws IOException {
        for (String key : dirty) {
            io.put(key, ROW_LENGTH, Utils.longToBytes(cache.get(key)));
        }
        if (clear) {
            cache.clear();
            dirty.clear();
        }
    }
}
