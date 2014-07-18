package collene;

import java.io.IOException;

public interface IO {
    
    public void put(String key, long col, byte[] value) throws IOException;
    public byte[] get(String key, long col) throws IOException;
    public int getColSize();
    
    public String[] allKeys() throws IOException;
    public void delete(String key) throws IOException;
    public boolean hasKey(String key) throws IOException;
}
