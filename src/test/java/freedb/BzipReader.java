package freedb;

import com.aftexsw.util.bzip.CBZip2InputStream;

import java.io.File;
import java.io.InputStream;
import java.io.IOException;
import java.io.FileInputStream;

/**
 * Reads TarRecords out of a tar.bz2 file.
 */
class BzipReader {

    private static final int BLOCK_SIZE = 512;
    
    private final InputStream in;

    // create the bz input stream.
    BzipReader(File bzip) throws IOException {
        in = new CBZip2InputStream(new FileInputStream(bzip));
    }

    // shut down.
    void close() {
        if (in != null) {
            try {
                in.close(); }
            catch (IOException ex) {
                System.err.println(ex.getMessage()); }
        }
    }

    // read the next record out of the stream.
    synchronized TarRecord next() throws IOException {
        // read the header
        // README and COPYING are files we don't want to handle.

        // read the tar header.
        byte[] rawHead = new byte[BLOCK_SIZE];
        int read = in.read(rawHead);
        if (read < BLOCK_SIZE) {
            // this usually happens at the end of a file.
            if (in.available() < 1) return null;
            else throw new IOException("Could not read entire header and there are bytes left.");
        }
        // in some cases, we have available() == 0 and all bytes in were zeros. The stream is technically empty here.
        // seemed to be a mac thing.
        if (in.available() == 0 && read == BLOCK_SIZE) {
            // if all rawHead are zero, we are done.
            boolean empty = true;
            for (int i = 0; empty && i < read; i++)
                empty = rawHead[i] == 0;
            if (empty)
                return null;
        }
        TarHeader header = new TarHeader(rawHead);
        //System.out.println(header.getName());
        long size = header.getSize();
        long blocks = size / BLOCK_SIZE;
        // there might be a partial block left over if size is not a perfect
        // multiple of BLOCK_SIZE
        int lastBlockSize = (int) (size % BLOCK_SIZE);
        int leftover = 0;
        if (lastBlockSize > 0) {
            blocks++;
            leftover = 512 - lastBlockSize;
        }

        // if no blocks, return empty bytes (not null)
        if (blocks == 0) new TarRecord(header, new byte[]{});

        // read in the data.
        byte[] data = new byte[(int) size];
        read = 0;
        while (read < size)
            read += in.read(data, read, (int) (size - read));

        // read in any nulls in the last block
        byte[] fluff = new byte[leftover];
        read = 0;
        while (read < leftover)
            read += in.read(fluff, read, leftover - read);

        if (data == null)
            throw new IOException("No data in bzip reader.");
        return new TarRecord(header, data);
    }
}
