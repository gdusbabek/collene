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
import java.util.HashSet;
import java.util.Set;

public class SplitRowIO implements IO {
    private final IO io;
    private final int splits;
    private final String delimiter;
    
    public SplitRowIO(int splits, String delimiter, IO io) {
        this.io = io;
        this.splits = splits;
        this.delimiter = delimiter;
    }
    
    @Override
    public void put(String key, long col, byte[] value) throws IOException {
        io.put(dbKey(key, col), col, value);
    }

    @Override
    public byte[] get(String key, long col) throws IOException {
        return io.get(dbKey(key, col), col);
    }

    @Override
    public int getColSize() {
        return io.getColSize();
    }

    @Override
    public void delete(String key) throws IOException {
        for (long mod = 0; mod < splits; mod++) {
            io.delete(dbKey(key, mod));
        }
    }

    @Override
    public boolean hasKey(String key) throws IOException {
        for (long mod = 0; mod < splits; mod++) {
            if (io.hasKey(dbKey(key, mod))) {
                return true;
            }
        }
        return false;
    }

    @Override
    public String[] allKeys() throws IOException {
        Set<String> allKeys = new HashSet<String>();
        for (String augmentedKey : io.allKeys()) {
            String[] parts = augmentedKey.split(delimiter, -1);
            allKeys.add(parts[0]);
        }
        return allKeys.toArray(new String[allKeys.size()]);
    }
    
    private String dbKey(String key, long col) {
        return String.format("%s%s%d", key, delimiter, col % splits);
    }
}
