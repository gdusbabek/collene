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

package freedb;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

/**
 * struct for a tar header block.  Here is how it is built:
 * Width 	Field Name  Meaning
 * 100 	    name 	    name of file
 * 8 	    mode 	    file mode
 * 8 	    uid 	    owner user ID
 * 8 	    gid 	    owner group ID
 * 12 	    size 	    length of file in bytes
 * 12 	    mtime 	    modify time of file
 * 8 	    chksum 	    checksum for header
 * 1 	    link 	    indicator for links
 * 100 	    linkname    name of linked file
 */

class TarHeader {
    private static final int HEADER_LEN = 512;
    private final byte[] data;

    TarHeader(byte[] data)
            throws IOException {
        if (data.length < HEADER_LEN) {
            throw new IOException("invalid header length " + data.length);
        }
        this.data = data;
    }

    String getName() {
        String s = new String(data, 0, 100);
        int zi = s.indexOf(0);
        if (zi > -1) {
            s = s.substring(0, zi);
        }
        return s;
    }

    long getSize() {
        try {
            return parseOctal(data, 124, 12);
        }
        catch (IOException ex) {
            ex.printStackTrace();
            return 0L;
        }
    }

    private long parseOctal(byte[] b, int start, int length)
            throws IOException {
        long retVal = 0;
        int i, pos;
        boolean started = false;
        for (i = 0, pos = start; (i < length) && (b[pos] != 0); i++, pos++) {
            if (started && (b[pos] == ' ')) {
                break;
            }
            if (b[pos] < '0' || b[pos] > '9') {
                throw new IOException("error in parseOctal");
            }
            retVal = (retVal << 3) + (b[pos] - '0');
            started = true;
        }
        return retVal;
    }

    // for testing purposes.
    static void main(String args[]) {
        try {
            File file = new File("songdb.tar");
            long fileSize = file.length();
            long read = 0;
            FileInputStream in = new FileInputStream(file);
            while (read < fileSize) {
                byte[] buf = new byte[512];
                int jr = in.read(buf);
                if (jr < 512) {
                    throw new IOException("wanted 512 bytes");
                }
                read += jr;
                TarHeader header = new TarHeader(buf);
                String name = header.getName();
                System.out.println(name);
                long size = header.getSize();
                long blocks = size / 512;
                if (size % 512 > 0) {
                    blocks++;
                }
                for (int i = 0; i < blocks; i++) {
                    read += in.read(buf);
                }
            }
        }
        catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}