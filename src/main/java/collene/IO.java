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

/**
 * This is the core interface for talking to data stores. If you implement this, then everything else should just work.
 * Data is modeled with long rows. Each row is addressed by a Key and is composed of sequential (Column, Value) pairs.
 * 
 * I've artificially forced the types of each (Key, Column, Value) to be (String, Long, Binary) so that this interface
 * would not have to be generic. This required a little creativity to store other things when needed, but nothing is
 * too bad of a hack. Personally, I like the simplicity of knowing that if you implement this interface, you've done
 * just about everything you need to do.
 * 
 * Keys will typically map to file names. Columns then become a way of addressing the binary data normally held in a
 * file. For example, if the column size of a particular IO implementation is 8192 bytes, then column 0 addresses 
 * bytes 0-8191, column 1 addresses bytes 8192-16383, etc. 
 */
public interface IO {
    
    /** store a column,value */
    public void put(String key, long col, byte[] value) throws IOException;
    
    /** retrive a value */
    public byte[] get(String key, long col) throws IOException;

    /**
     * Return the column size for this IO instance. This value is the length in bytes of each column value.
     */
    public int getColSize();
    
    /** remove an entire row */
    public void delete(String key) throws IOException;
    
    /** remove a column and its value */
    public void delete(String key, long col) throws IOException;
    
    /** return true if there are any values for a particular key. */
    public boolean hasKey(String key) throws IOException;
    
    // be careful using this. you only want to use it on rows you know are not very long.
    // it is not intended for long rows.

    /**
     * iterate all values in a particular row in no particular order. You will not normally do this for a data [file]
     * row. Instead this is mainly used for collect meta information (file lengths, etc.) that are stored in IO 
     * instances.
     */
    public Iterable<byte[]> allValues(String key) throws IOException;
}
