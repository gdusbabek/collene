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

import com.google.common.base.Charsets;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.CharsetDecoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Holds on to file meta information (currently just the length), flushing it when told.
 */
public class RowMeta {
    private static final long ROW_LENGTH_COL = 0;
    private static final String ROW_PREFIX = "__COLLENE_META_ROW_PREFIX__";
    
    // special row key used to store all keys. todo: obvious consistency problems. I think we mostly get around this in
    // lucene by knowing that a particular Directory instnace only operates on a subset of the keys.
    private static final String KEY_LIST_KEY = "__COLLENE_KEY_LIST_KEY__";

    private static final ThreadLocal<CharsetDecoder> decoders = new ThreadLocal<CharsetDecoder>() {
        @Override
        protected CharsetDecoder initialValue() {
            return Charsets.UTF_8.newDecoder();
        }
    };
    
    private final IO io;
    private final Map<String, Long> cache = new HashMap<String, Long>();
    private final Set<String> dirty = new HashSet<String>();
    private final String fileNamesListKey;
    
    public RowMeta(IO io) {
        this.io = io;
        fileNamesListKey = prefix(KEY_LIST_KEY);
    }
    
    public long getLength(String key) throws IOException {
        if (cache.containsKey(key)) {
            return cache.get(key);
        } else {
            byte[] buf = io.get(prefix(key), ROW_LENGTH_COL);
            if (buf == null) {
                throw new NullPointerException("Null bytes for key " + key);
            }
            assert buf.length == 8;
            long length = Utils.bytesToLong(buf);
            cache.put(key, length);
            return length;
        }
    }
    
    public void setLength(String key, long length, boolean commit) throws IOException {
        cache.put(key, length);
        if (commit) {
            byte[] buf = Utils.longToBytes(length);
            assert buf.length == 8;
            io.put(prefix(key), ROW_LENGTH_COL, buf);
        } else {
            synchronized (dirty) {
                dirty.add(key);
            }
        }
    }
    
    public void flush(boolean clear) throws IOException {
        Set<String> tempDirty = new HashSet<>(dirty);
        for (String key : tempDirty) {
            Long v = cache.get(key);
            String prefixedKey = prefix(key);
            if (v != null) {
                io.put(prefix(key), ROW_LENGTH_COL, Utils.longToBytes(v));
                io.put(fileNamesListKey, prefixedKey.hashCode(), prefixedKey.getBytes(Charsets.UTF_8));
            }
        }
        if (clear) {
            cache.clear();
            synchronized (dirty) {
                dirty.removeAll(tempDirty);
            }
        }
    }
    
    public void delete(String key) throws IOException {
        String prefixedKey = prefix(key);
        io.delete(prefixedKey);
        io.delete(fileNamesListKey, prefixedKey.hashCode());
        
    }
    
    private static String prefix(String key) {
        return String.format("%s/%s", ROW_PREFIX, key);
    }
    
    private String unprefix(String prefixedKey) {
        return prefixedKey.split("/", -1)[1];
    }
    
    public String[] allKeys() throws IOException {
        List<String> keys = new ArrayList<String>();
        for (byte[] bb : io.allValues(fileNamesListKey)) {
            // todo: need to benchmark the various approaches here in a concurrent environment.
            //keys.add(unprefix(new String(bb, Charsets.UTF_8)));
            keys.add(unprefix(decoders.get().decode(ByteBuffer.wrap(bb)).toString()));
        }
        return keys.toArray(new String[keys.size()]);
    }
}
