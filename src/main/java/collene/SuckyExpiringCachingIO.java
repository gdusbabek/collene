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

package collene;

import java.io.IOError;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import com.google.common.base.Supplier;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimaps;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;

/** 
 * just to show it can be done. Performance is terrible when there are a lot of nothings in the database, which 
 * is the case early on. I'm keeping this around for a while, until I come up with something better, but I'm not
 * going to use it.
 * 
 * Can be used in place of CachingCompositeIO
 */
public class SuckyExpiringCachingIO implements IO {
    private static final byte[] NULL_SENTINAL = new byte[]{0};
    private final IO io;
    private final boolean autoFlush;
    
    private final String cacheSpec;
    private final Map<String, LoadingCache<Long, byte[]>> mcache = new HashMap<>();
    
    private final SetMultimap<String, Long> needsFlush = Multimaps.newSetMultimap(
            Maps.<String, Collection<Long>>newHashMap(),
            new Supplier<Set<Long>>() {
                @Override
                public Set<Long> get() {
                    return Sets.newHashSet();
                }
            }
    );
    
    public SuckyExpiringCachingIO(IO io) {
        this(io, false);
    }
    
    public SuckyExpiringCachingIO(IO io, boolean autoFlush) {
        this.io = io;
        this.autoFlush = autoFlush;
        this.cacheSpec = String.format("concurrencyLevel=2,maximumSize=%d,expireAfterAccess=%s", 100L, "1m");
    }
    
    // key must be interned.
    private void ensureCache(final String key) {
        if (mcache.get(key) == null) {
            String interned = key.intern();
            synchronized (interned) {
                if (mcache.get(key) != null) {
                    return;
                }
                
                LoadingCache<Long, byte[]> rowCache = CacheBuilder.from(cacheSpec).build(new CacheLoader<Long, byte[]>() {
                    @Override
                    public byte[] load(final Long col) throws Exception {
                        byte[] value = io.get(key, col);
                        if (value == null) {
                            value = NULL_SENTINAL;
                        }
                        return value;
                    }
                });
                mcache.put(key, rowCache);
            }
        }
    }

    @Override
    public void put(String key, long col, byte[] value) throws IOException {
        needsFlush.put(key, col);
        ensureCache(key);
        mcache.get(key).put(col, value);
        if (autoFlush) {
            this.flush(false);
        }
    }

    @Override
    public byte[] get(String key, long col) throws IOException {
        try {
            ensureCache(key);
            byte[] value = mcache.get(key).get(col);
            if (value == NULL_SENTINAL) {
                mcache.get(key).invalidate(col);
                return null;
            } else {
                return value;
            }
        } catch (ExecutionException ex) {
            throw new IOError(ex);
        }
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
        ensureCache(key);
        mcache.remove(key);
        needsFlush.removeAll(key);
        io.delete(key);
    }

    @Override
    public void delete(String key, long col) throws IOException {
        ensureCache(key);
        mcache.get(key).invalidate(col);
        if (mcache.get(key).size() == 0) {
            needsFlush.removeAll(key);
            io.delete(key);
        } else {
            io.delete(key, col);
        }
    }

    @Override
    public boolean hasKey(String key) throws IOException {
        if (mcache.containsKey(key)) {
            return true;
        } else {
            boolean existsUnderneath = io.hasKey(key);
            if (existsUnderneath) {
                // keep something local so checks do not go to the lower store next time.
                ensureCache(key);
            }
            return existsUnderneath;
        }
    }
    
    public void flush(boolean emptyCache) throws IOException {
        try {
            for (String key : needsFlush.keySet()) {
                for (long col : needsFlush.get(key)) {
                    io.put(key, col, mcache.get(key).get(col));
                }
            }
            needsFlush.clear();
            if (emptyCache) {
                mcache.clear();
            }
        } catch (ExecutionException ex) {
            throw new IOException(ex);
        }
    }
}
