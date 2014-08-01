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
import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * Reader
 */
public class FreeDbReader {
    private Thread parseThread = null;
    private final File bzip;
    private boolean done = false;
    private final BlockingQueue<FreeDbEntry> entries;
    private static final FreeDbEntry POISON = new FreeDbEntry(null);

    public FreeDbReader(File bzip, int queueCapacity) {
        this.bzip = bzip;
        entries = new ArrayBlockingQueue<FreeDbEntry>(queueCapacity);
    }

    public void start() {
        if (parseThread != null)
            throw new RuntimeException("Start called multiple times.");
        parseThread = new Thread("Parser for " + bzip.getAbsolutePath()) {
            public void run() {
                done = false;
                parseFile();
                // poison the queue so we know that data will not be added.
                try {
                    entries.put(POISON);
                } catch (InterruptedException ex) { }
                done = true;
            }
        };
        parseThread.start();
    }

    public FreeDbEntry next() {
        FreeDbEntry entry = nextInternal();
        while (isCrappy(entry))
            entry = nextInternal();
        return entry;
    }

    private static boolean isCrappy(FreeDbEntry entry) {
        if (entry == null) return false;
        else return entry.getTitle().trim().length() <= 1;
    }

    private FreeDbEntry nextInternal() {
        try {
            FreeDbEntry entry = entries.take();
            // if we pulled poison, return null.
            // also, put the poison back in case there are multiple consumers.
            if (entry == POISON) {
                try { entries.put(POISON); } catch (InterruptedException ex) {}
                return null;
            } else
                return entry;
        } catch (InterruptedException ex) {
            return null;
        }
    }

    private void parseFile() {
        RecordParser parser = new RecordParser();
        try {
            BzipReader reader = new BzipReader(bzip);
            TarRecord rec = reader.next();
            while (rec != null) {
                try {
                    FreeDbEntry entry = parser.parse(rec);
                    try {
                        entries.put(entry);
                    } catch (InterruptedException ex) { }
                    try {
                        rec = reader.next();
                    } catch (IOException ex) {
                        ex.printStackTrace();
                    }
                } catch (FormatException ex) {
                    System.err.println(ex.getArchivePath() + "  " + ex.getMessage());
                }
            }
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }
}
