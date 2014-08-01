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

import org.apache.lucene.store.IndexInput;

import java.io.IOError;
import java.io.IOException;

public class RowIndexInput extends IndexInput {
    private final String key;
    private final RowReader io;
    private final long offset;
    protected volatile long pointer = 0;

    public RowIndexInput(String key, RowReader io) {
        this(key, io, 0);
    }
    
    private RowIndexInput(String key, RowReader io, long offset) {
        super(key);
        this.key = key;
        this.io = io;
        this.offset = offset;
    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public long getFilePointer() {
        return pointer;
    }

    @Override
    public void seek(long pos) throws IOException {
        pointer = pos;
    }

    @Override
    public long length() {
        try {
            return io.meta().getLength(key);
        } catch (IOException ex) {
            throw new IOError(ex);
        }
    }

    @Override
    public IndexInput slice(String sliceDescription, final long offset, final long length) throws IOException {
        if (offset < 0 || length < 0 || offset+length > this.length()) {
          throw new IllegalArgumentException("slice() " + sliceDescription + " out of bounds: offset=" + offset + ",length=" + length + ",fileLength="  + this.length() + ": "  + this);
        }
        
        return new RowIndexInput(key, io, pointer + offset) {
            @Override
            public long length() {
                return length;
            }
        };
    }

    @Override
    public byte readByte() throws IOException {
        byte b = io.getByte(offset + pointer);
        pointer += 1;
        return b;
    }

    @Override
    public void readBytes(byte[] b, int offset, int len) throws IOException {
        byte[] buf = io.getBytes(this.offset + pointer, len);
        assert buf.length == len;
        
        // todo: copy alert.
        System.arraycopy(buf, 0, b, offset, len);
        pointer += len;
    }
}
