package collene;

import java.io.IOError;
import java.io.IOException;

public class RowWriter {
    private final String key;
    private final CachingCompositeIO io;
    private final RowMeta meta;
    
    public RowWriter(String key, IO io, IO metaIo) {
        this.key = key;
        this.io = new CachingCompositeIO(io);
        this.meta = new RowMeta(key, metaIo);
    }
    
    public void flush() throws IOException {
        // since files are never modified, we can remove them from the cache.
        io.flush(true);
    }
    
    public void append(long pointer, byte[] buf, int bufOffset, int length) throws IOException {
        //System.out.println(String.format("PUT %s@%d %d bytes: %s", key, pointer, length, bytesToString(buf, bufOffset, length)));
        
        byte[] colValue = null;
        long lastCol = -1;
        boolean mustFinish = false;
        
        for (int i = 0; i < length; i++) {
            
            long col = (pointer + i) / io.getColSize();
            int colOffset = (int)((pointer + i) % io.getColSize());
            
            if (col != lastCol) {
                // we've moved columns.
                // maybe save the last one.
                if (colValue != null) {
                    io.put(key, lastCol, colValue);
                    mustFinish = false;
                }
                
                // read in the new column.
                colValue = io.get(key, col);
                lastCol = col;
            }
            
            if (colValue == null) {
                colValue = new byte[io.getColSize()];
            }
            
            colValue[colOffset] = buf[bufOffset + i];
            mustFinish = true;
        }
        
        if (mustFinish) {
            io.put(key, (int)((pointer + length - 1) / io.getColSize()), colValue);
        }
        
        meta.setLength(pointer + length);
    }
    
    private static String bytesToString(byte[] buf, int offset, int len) {
        StringBuilder sb = new StringBuilder();
        int b;
        for (int i = 0; i < len; i++) {
            b = buf[offset + i];
            if (b < 0) {
                b = 0x000000ff & b;
            }
            if (b < 0x0f) {
                sb = sb.append("0").append(Integer.toHexString(b));
            } else {
                sb = sb.append(Integer.toHexString(b));
            }
        }
        return sb.toString();
    }
    
    public String toString() {
        try {
            return String.format("%s %d bytes", key, meta.length());
        } catch (IOException ex) {
            throw new IOError(ex);
        }
    }
}
