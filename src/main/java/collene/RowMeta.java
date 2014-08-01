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

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Holds on to file meta information (currently just the length), flushing it when told.
 */
public class RowMeta {
    private static final long ROW_LENGTH_COL = 0;
    private static final String ROW_PREFIX = "__COLLENE_META_ROW_PREFIX__";
    
    
    private final IO io;
    private final Map<String, Long> cache = new HashMap<String, Long>();
    private final Set<String> dirty = new HashSet<String>();
    
    public RowMeta(IO io) {
        this.io = io;
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
            dirty.add(key);
        }
    }
    
    public void flush(boolean clear) throws IOException {
        for (String key : dirty) {
            io.put(prefix(key), ROW_LENGTH_COL, Utils.longToBytes(cache.get(key)));
        }
        if (clear) {
            cache.clear();
            dirty.clear();
        }
    }
    
    public void delete(String key) throws IOException {
        io.delete(prefix(key));
    }
    
    private static String prefix(String key) {
        return String.format("%s.%s", ROW_PREFIX, key);
    }
}
