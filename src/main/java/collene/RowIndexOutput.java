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

import org.apache.lucene.store.IndexOutput;

import java.io.IOException;
import java.util.zip.CRC32;

public class RowIndexOutput extends IndexOutput {
    private final String key;
    private volatile long pointer = 0;
    private final CRC32 crc = new CRC32();
    private RowWriter io;

    public RowIndexOutput(String key, RowWriter io) {
        this.key = key;
        this.io = io;
    }

    @Override
    public void close() throws IOException {
        flush();
    }

    @Override
    public long getFilePointer() {
        return pointer;
    }

    @Override
    public long getChecksum() throws IOException {
        return crc.getValue();
    }

    @Override
    public void writeByte(byte b) throws IOException {
        crc.update(b);
        io.append(pointer, new byte[]{b}, 0, 1);
        pointer += 1;
    }

    @Override
    public void writeBytes(byte[] b, int offset, int length) throws IOException {
        crc.update(b, offset, length);
        io.append(pointer, b, offset, length);
        pointer += length;
    }

    @Override
    public void flush() throws IOException {
        io.flush();
    }
}
