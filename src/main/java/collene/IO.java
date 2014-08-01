package collene;

import java.io.IOException;

public interface IO {
    
    public void put(String key, long col, byte[] value) throws IOException;
    public byte[] get(String key, long col) throws IOException;
    public int getColSize();
    
    public void delete(String key) throws IOException;
    public boolean hasKey(String key) throws IOException;
    
    // todo: I'd like to get rid of this method. implementing it in cassandra requires an index row or additional CF.
    public String[] allKeys() throws IOException;
}
