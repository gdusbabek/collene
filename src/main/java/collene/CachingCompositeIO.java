package collene;

import com.google.common.base.Supplier;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.MultimapBuilder;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Multiset;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import com.google.common.collect.Table;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

public class CachingCompositeIO implements IO {
    private final IO io;
    
    private final Table<String, Long, byte[]> cache = HashBasedTable.create();
    private final SetMultimap<String, Long> needsFlush = Multimaps.newSetMultimap(
            Maps.<String, Collection<Long>>newHashMap(),
            new Supplier<Set<Long>>() {
                @Override
                public Set<Long> get() {
                    return Sets.newHashSet();
                }
            }
    );
    
    public CachingCompositeIO(IO io) {
        this.io = io;
    }

    @Override
    public void put(String key, long col, byte[] value) throws IOException {
        needsFlush.put(key, col);
        cache.put(key, col, value);
    }

    @Override
    public byte[] get(String key, long col) throws IOException {
        byte[] value = cache.get(key, col);
        if (value == null) {
            value = io.get(key, col);
            if (value != null) {
                cache.put(key, col, value);
            }
        }
        return value;
    }

    @Override
    public int getColSize() {
        return io.getColSize();
    }

    @Override
    public String[] allKeys() throws IOException {
        return io.allKeys();
    }

    @Override
    public void delete(String key) throws IOException {
        // purge from the cache.
        Map<Long, byte[]> row = cache.row(key);
        Collection<Long> cols = new ArrayList<Long>(row.keySet());
        for (long col : cols) {
            cache.remove(key, col);
        }
        needsFlush.removeAll(key);
        
        io.delete(key);
    }

    @Override
    public boolean hasKey(String key) throws IOException {
        if (cache.contains(key, 0))
            return true;
        else {
            get(key, 0);
            return cache.contains(key, 0);
        }
    }
    
    public void flush(boolean emptyCache) throws IOException {
        for (String key : needsFlush.keySet()) {
            for (long col : needsFlush.get(key)) {
                io.put(key, col, cache.get(key, col));
            }
        }
        needsFlush.clear();
        if (emptyCache) {
            cache.clear();
        }
    }
}
