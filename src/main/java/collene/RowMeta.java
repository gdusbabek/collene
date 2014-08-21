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
 * 
 * How this works.
 * 1. every file gets its own row. offset 0 holds the length incoded as a 8-byte long.
 * 2. every file gets a column in the row keyed by KEY_LIST_KEY. This makes it easy to get a list of all the files.
 *    (I realize the performance implications of this). It's one reason you may wish to use a SplitRowIO for your
 *    RowMeta instance.
 */
public class RowMeta {
    private static final long ROW_LENGTH_COL = 0;
    
    // every row gets prefixed with this string.
    public static final String ROW_PREFIX = "__COLLENE_META_ROW_PREFIX__";
    
    // special row key used to store all keys. todo: obvious consistency problems. I think we mostly get around this in
    // lucene by knowing that a particular Directory instnace only operates on a subset of the keys.
    public static final String KEY_LIST_KEY = "__COLLENE_KEY_LIST_KEY__";

    private static final ThreadLocal<CharsetDecoder> decoders = new ThreadLocal<CharsetDecoder>() {
        @Override
        protected CharsetDecoder initialValue() {
            return Charsets.UTF_8.newDecoder();
        }
    };
    
    private final IO io;
    
    // avoid lookups by using a cache of file lengths.
    private final Map<String, Long> cache = new HashMap<String, Long>();
    
    // keep track of "dirty" metadata (mainly when the length of a file is set).
    private final Set<String> dirty = new HashSet<String>();
    
    // final key used to keep the long list of file names.
    private final String fileNamesListKey;
    
    /** create */
    public RowMeta(IO io) {
        this.io = io;
        fileNamesListKey = prefix(KEY_LIST_KEY);
    }
    
    /** @return the length of a particular file */
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
    
    /** set the length of a file. set commit if you want that data immediately flushed to the backing store */
    public void setLength(String key, long length, boolean commit) throws IOException {
        cache.put(key, length);
        if (commit) {
            byte[] buf = Utils.longToBytes(length);
            assert buf.length == 8;
            String prefixKey = prefix(key);
            // store the length.
            io.put(prefixKey, ROW_LENGTH_COL, buf);
            // ensure we have a record so we know this file exists.
            io.put(fileNamesListKey, prefixKey.hashCode(), prefixKey.getBytes(Charsets.UTF_8));
        } else {
            synchronized (dirty) {
                dirty.add(key);
            }
        }
    }
    
    /** commit all the dirty information to the backing store */
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
    
    /** remove all meta information for a particular file */
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
    
    /** @return the file names from the long row of file names */
    public String[] allKeys() throws IOException {
        // this could very well have been done with a "select *" type of query (IO has that), but I think this might
        // perform better.
        List<String> keys = new ArrayList<String>();
        for (byte[] bb : io.allValues(fileNamesListKey)) {
            // todo: need to benchmark the various approaches here in a concurrent environment.
            //keys.add(unprefix(new String(bb, Charsets.UTF_8)));
            keys.add(unprefix(decoders.get().decode(ByteBuffer.wrap(bb)).toString()));
        }
        return keys.toArray(new String[keys.size()]);
    }
}
