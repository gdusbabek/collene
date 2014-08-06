/**
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

public interface IO {
    
    public void put(String key, long col, byte[] value) throws IOException;
    public byte[] get(String key, long col) throws IOException;
    public int getColSize();
    
    public void delete(String key) throws IOException;
    public void delete(String key, long col) throws IOException;
    public boolean hasKey(String key) throws IOException;
    
    // be careful using this. you only want to use it on rows you know are not very long.
    // it is not intended for long rows.
    public Iterable<byte[]> allValues(String key) throws IOException;
}
