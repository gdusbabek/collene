package collene;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class RowReader {
    private final String key;
    private final IO io;
    private final RowMeta meta;
    
    public RowReader(String key, IO io, IO metaIo) {
        this.key = key;
        this.io = new CachingCompositeIO(io);
        this.meta = new RowMeta(key, metaIo);
    }
    
    public byte getByte(long pointer) throws IOException {
        long col = columnFor(pointer);
        int offset = offsetFor(pointer);
        byte[] buf = io.get(key, col);
        if (buf == null) {
            return 0;
        } else {
            return buf[offset];
        }
    }
    
    private long columnFor(long pointer) {
        return pointer / io.getColSize();
    }
    
    private int offsetFor(long pointer) {
        return (int)(pointer % io.getColSize());
    }
    
    public byte[] getBytes(long pointer, int len) throws IOException {
        Map<Long, byte[]> colCache = new HashMap<Long, byte[]>();
        
        byte[] buf = new byte[len];
        byte[] colValue;
        for (int i = 0; i < len; i++) {
            
            long column = columnFor(pointer + i);
            if (colCache.containsKey(column)) {
                colValue = colCache.get(column);
            } else {
                colValue = io.get(key, column);
                if (colValue != null) {
                    colCache.put(column, colValue);
                }
            }
            
            if (colValue == null) {
                buf[i] = 0;
            } else {
                buf[i] = colValue[offsetFor(pointer + i)];
            }
        }
        return buf;
    }
    
    public RowMeta meta() {
        return meta;
    }
}
