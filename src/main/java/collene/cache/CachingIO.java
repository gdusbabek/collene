/*
 * Copyright 2014 Gary Dusbabek
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package collene.cache;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicInteger;

import collene.IO;
import com.google.common.base.Supplier;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimaps;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import com.google.common.collect.Table;

public class CachingIO implements IO {
    private static final AtomicInteger NAMER = new AtomicInteger(0);
    
    private final IO io;
    private final boolean autoFlush;
    
    private final EvictionStrategy evictionStrategy;
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
    private Timer evictTimer = new Timer(String.format("CachingIO-eviction-%d", NAMER.getAndIncrement()), false);
    
    public CachingIO(IO io) {
        this(io, false);
    }
    
    public CachingIO(IO io, boolean autoFlush) {
        this(io, autoFlush, EvictionStrategies.NEVER);
    }
    
    public CachingIO(IO io, boolean autoFlush, EvictionStrategy evictionStrategy) {
        this.io = io;
        this.autoFlush = autoFlush;
        this.evictionStrategy = evictionStrategy;
        evictTimer.schedule(new TimerTask() {
            @Override
            public void run() {
                try {
                    runEvictions();
                } catch (Exception ex) {
                    // log that.
                }
            }
        },
        10000, 10000);
    }

    @Override
    public void put(String key, long col, byte[] value) throws IOException {
        needsFlush.put(key, col);
        cache.put(key, col, value);
        evictionStrategy.notePut(key, col);
        if (autoFlush) {
            this.flush(false);
        }
    }

    @Override
    public byte[] get(String key, long col) throws IOException {
        byte[] value = cache.get(key, col);
        if (value == null) {
            value = io.get(key, col);
            if (value != null) {
                cache.put(key, col, value);
                evictionStrategy.noteGet(key, col);
            }
        } else {
            evictionStrategy.noteGet(key, col);
        }
        return value;
    }

    @Override
    public int getColSize() {
        return io.getColSize();
    }

    @Override
    public Iterable<byte[]> allValues(String key) throws IOException {
        // no caching here because the column names are filtered.
        return io.allValues(key);
    }

    @Override
    public void delete(String key) throws IOException {
        // purge from the cache.
        Map<Long, byte[]> row = cache.row(key);
        Collection<Long> cols = new ArrayList<Long>(row.keySet());
        for (long col : cols) {
            cache.remove(key, col);
            evictionStrategy.remove(key, col);
        }
        needsFlush.removeAll(key);
        
        io.delete(key);
    }

    @Override
    public void delete(String key, long col) throws IOException {
        cache.remove(key, col);
        evictionStrategy.remove(key, col);
        if (cache.row(key).size() == 0) {
            needsFlush.removeAll(key);
            io.delete(key);
        } else {
            io.delete(key, col);
        }
    }

    @Override
    public boolean hasKey(String key) throws IOException {
        if (cache.contains(key, 0L))
            return true;
        else {
            get(key, 0L);
            return cache.contains(key, 0L);
        }
    }
    
    public void flush(boolean emptyCache) throws IOException {
        synchronized (cache) {
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
    
    public void forceEvictions() {
        try {
            runEvictions();
        } catch (Exception ex) {
            // todo: log this!
        }
    }
    
    private void runEvictions() throws Exception {
        // don't bother if there is no eviction policy.
        if (evictionStrategy == null) {
            return;
        }
        
        synchronized (evictionStrategy) {
            
            // keep track of what to remove here.
            final SetMultimap<String, Long> willRemove = Multimaps.newSetMultimap(
                    Maps.<String, Collection<Long>>newHashMap(),
                    new Supplier<Set<Long>>() {
                        @Override
                        public Set<Long> get() {
                            return Sets.newHashSet();
                        }
                    }
            );
            
            // iterate over the table, evaluating each member.
            // do not evaluate members that need to be flushed (written).
            for (String key : cache.rowKeySet()) {
                Set<Long> colsToAvoid = needsFlush.get(key);
                for (Long col : cache.row(key).keySet()) {
                    if (colsToAvoid.contains(col)) {
                        continue;
                    }
                    if (evictionStrategy.shouldEvict(key, col)) {
                        willRemove.put(key, col);
                    }
                }
            }
            
            // actually remove them now.
            synchronized (cache) {
                for (String key : willRemove.keySet()) {
                    for (long col : willRemove.get(key)) {
                        cache.remove(key, col);
                        evictionStrategy.remove(key, col);
                    }
                }
            }
        }
    }
}
