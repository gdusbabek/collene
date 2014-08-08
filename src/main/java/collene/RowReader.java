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

import collene.cache.CachingIO;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class RowReader {
    private final String key;
    private final IO io;
    private final RowMeta meta;
    
    public RowReader(String key, IO io, RowMeta meta) {
        this.key = key;
        this.io = new CachingIO(io);
        this.meta = meta;
    }
    
    public byte getByte(long pointer) throws IOException {
        long col = columnFor(pointer);
        int offset = offsetFor(pointer);
        byte[] buf = io.get(key, col);
        if (buf == null) {
            return 0;
        } else {
            return buf[offset];
        }
    }
    
    private long columnFor(long pointer) {
        return pointer / io.getColSize();
    }
    
    private int offsetFor(long pointer) {
        return (int)(pointer % io.getColSize());
    }
    
    public byte[] getBytes(long pointer, int len) throws IOException {
        Map<Long, byte[]> colCache = new HashMap<Long, byte[]>();
        
        byte[] buf = new byte[len];
        byte[] colValue;
        for (int i = 0; i < len; i++) {
            
            long column = columnFor(pointer + i);
            if (colCache.containsKey(column)) {
                colValue = colCache.get(column);
            } else {
                colValue = io.get(key, column);
                if (colValue != null) {
                    colCache.put(column, colValue);
                }
            }
            
            if (colValue == null) {
                buf[i] = 0;
            } else {
                buf[i] = colValue[offsetFor(pointer + i)];
            }
        }
        return buf;
    }
    
    public RowMeta meta() {
        return meta;
    }
}
