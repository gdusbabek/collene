package collene;

import java.io.IOException;

public class RowReader {
    private final String key;
    private final IO io;
    private final RowMeta meta;
    
    public RowReader(String key, IO io, IO metaIo) {
        this.key = key;
        this.io = io;
        this.meta = new RowMeta(key, metaIo);
    }
    
    public byte getByte(long pointer) throws IOException {
        long col = pointer / io.getColSize();
        int offset = (int)(pointer % io.getColSize());
        byte[] buf = io.get(key, col);
        if (buf == null) {
            return 0;
        } else {
            return buf[offset];
        }
    }
    
    // todo: make smarter.
    public byte[] getBytes(long pointer, int len) throws IOException {
        byte[] buf = new byte[len];
        for (int i = 0; i < len; i++) {
            buf[i] = getByte(pointer + i);
        }
        return buf;
    }
    
    public RowMeta meta() {
        return meta;
    }
}
