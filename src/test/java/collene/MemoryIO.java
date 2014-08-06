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
    public Iterable<byte[]> allValues(String key) throws IOException {
        return data.row(key).values();
    }

    @Override
    public int getColSize() {
        return colSize;
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
    public void delete(String key, long col) throws IOException {
        data.remove(key, col);
    }

    @Override
    public boolean hasKey(String key) throws IOException {
        return data.rowKeySet().contains(key);
    }
}
