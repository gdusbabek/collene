package collene;

import java.io.IOException;

public class RowMeta {
    public static final long ROW_LENGTH = 0;
    
    private final IO io;
    private final String key;
    
    public RowMeta(String key, IO io) {
        this.key = key;
        this.io = io;
    }
    
    public long length() throws IOException {
        byte[] buf = io.get(key, ROW_LENGTH);
        assert buf.length == 8;
        return Utils.bytesToLong(buf);
    }
    
    public void setLength(long length) throws IOException {
        byte[] buf = Utils.longToBytes(length);
        assert buf.length == 8;
        io.put(key, ROW_LENGTH, buf);
    }
}
