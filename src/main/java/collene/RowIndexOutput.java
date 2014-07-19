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
        // no-op.
        //System.out.println("Closing " + io.toString());
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
        // no-op.
    }
}
