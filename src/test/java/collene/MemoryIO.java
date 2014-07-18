package collene;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

public class MemoryIO implements IO {
    private final int colSize;
    
    public MemoryIO(int colSize) {
        this.colSize = colSize;
    }
    
    private final Table<String, Long, byte[]> data = HashBasedTable.create();
    
    @Override
    public void put(String key, long col, byte[] value) throws IOException {
        data.put(key, col, value);    
    }

    @Override
    public byte[] get(String key, long col) throws IOException {
        return data.get(key, col); // what about nulls?
    }

    @Override
    public int getColSize() {
        return colSize;
    }

    @Override
    public String[] allKeys() throws IOException {
        Set<String> keys = data.rowKeySet();
        return keys.toArray(new String[keys.size()]);
    }

    @Override
    public void delete(String key) throws IOException {
        // todo: is there a more efficient way to do this?
        Map<Long, byte[]> row = data.row(key);
        Collection<Long> cols = new ArrayList<Long>(row.keySet());
        for (long col : cols) {
            data.remove(key, col);
        }
    }

    @Override
    public boolean hasKey(String key) throws IOException {
        return data.rowKeySet().contains(key);
    }
}
