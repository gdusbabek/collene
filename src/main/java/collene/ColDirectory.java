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

import collene.cache.CachingIO;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collection;

/**
 * This is where the work is done. ColDirectory leverages the data access provided by IO implementations to provide
 * a Lucene compatible API experience.
 */
public class ColDirectory extends Directory {
    // I'm starting to think that the concept of "name" should be baked in at the IO level and not exposed here. This
    // already has shaken out in the CassandraIO implementation by using a "rowPrefix". It is essentially a "name" and
    // is used to namespace an index. For this class, it serves to uniquely identify a lock that must be held in order
    // to write. (The Lucene semantics do not hold here, as the lock will end up being stored inside the IO 
    // implementation, and so two ColDirectory instances with the same name but different IO instances could write at
    // the same time.
    private final String name;
    
    // flag for turning fast data copies off and on.
    private boolean allowFastCopy = true;
    
    private LockFactory lockFactory;
    
    // where data gets written to.
    private IO indexIO;
    
    // keeps track of "file" meta information.
    private RowMeta meta;
    
    private ColDirectory(String name, IO indexIO, RowMeta meta, LockFactory lockFactory) {
        
        if (lockFactory == null) {
            throw new RuntimeException("Must supply a lock factory");
        }
        
        this.name = name;
        this.indexIO = indexIO;
        this.meta = meta;
        this.lockFactory = lockFactory;
        
        // link the lock factory to this directory instance.
        // setting to null is following a pattern set by org.apache.lucene.store.Directory.
        lockFactory.setLockPrefix(null);
    }

    /**
     * Open a directory. Data may or may not already be there.
     * @param name Name of this directory. Has little bearing right now other than lock names.
     * @param indexIO this is where data will be written to.
     * @param metaIO this is where row meta information will be written to.
     * @return A ColDirectory instance, ready to go.
     */
    public static ColDirectory open(String name, IO indexIO, IO metaIO) {
        RowMeta rowMeta = new RowMeta(metaIO);
        return new ColDirectory(name, indexIO, rowMeta, new IoLockFactory(indexIO, rowMeta));    
    }

    /**
     * Enable fast copies during directory merges. Keep in mind that the underlying IO implementation must support row
     * translation (e.g., TranslateIO) for this to work.
     */
    public ColDirectory withFastCopy(boolean b) {
        allowFastCopy = b && indexIO instanceof TranslateIO;
        return this;
    }
    
    /** all bits of data associated with this index. includes locks, etc. */
    @Override
    public String[] listAll() throws IOException {
        return meta.allKeys();
    }

    /** delete a file */
    @Override
    public void deleteFile(String name) throws IOException {
        indexIO.delete(name);
        meta.delete(name);
    }

    /** @inheritDoc */
    @Override
    public long fileLength(String name) throws IOException {
        return meta.getLength(name);
    }

    /** ensures that all updates are synced to the backing store. */
    @Override
    public void sync(Collection<String> names) throws IOException {
        if (indexIO instanceof CachingIO) {
            ((CachingIO) indexIO).flush(false);
        }
    }

    /** @inheritDoc */
    @Override
    public Lock makeLock(String name) {
        return lockFactory.makeLock(name);
    }

    /** @inheritDoc */
    @Override
    public void clearLock(String name) throws IOException {
        lockFactory.clearLock(name);
    }

    /** @inheritDoc */
    @Override
    public void close() throws IOException {
        for (String key : listAll()) {
            clearLock(key);
        }
    }

    /** @inheritDoc */
    @Override
    public void setLockFactory(LockFactory lockFactory) throws IOException {
        assert lockFactory != null;
        this.lockFactory = lockFactory;
        lockFactory.setLockPrefix(this.getLockID());
    }

    /** @inheritDoc */
    @Override
    public LockFactory getLockFactory() {
        return this.lockFactory;
    }

    /** @inheritDoc */
    @Override
    public IndexOutput createOutput(String name, IOContext context) throws IOException {
        return new RowIndexOutput(name, new RowWriter(name, indexIO, meta));
    }

    /** @inheritDoc */
    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
        IndexInput input = new RowIndexInput(name, new RowReader(name, indexIO, meta));
        
        // we cannot read a file that does not exist. Lucene relies on the fact that this method will throw an exception
        // when a file is not present.
        try {
            input.length();
        } catch (NullPointerException ex) {
            throw new FileNotFoundException(name + " does not exist");
        }
        
        return input;
    }

    /** for us, a file exists if it has meta data and its length is >= 0. */
    @Override
    public boolean fileExists(String s) throws IOException {
        return meta.getLength(s) > -1;
    }

    /** @inheritDoc */
    @Override
    public String getLockID() {
        return name;
    }

    /** @inheritDoc */
    @Override
    public String toString() {
        return name + " " + super.toString();
    }

    /**
     * We override this in order to attempt a fast copy (changing a database pointer). Otherwise, we do a slow copy.
     */
    @Override
    public void copy(Directory to, String src, String dest, IOContext context) throws IOException {
        if (canFastCopy(this, to)) {
            fastCopy(to, src, dest, context);
        } else {
            super.copy(to, src, dest, context);
        }
    }

    /**
     * Fast copy works by adding a layer of indirection (rows are not really named what you think they are) simply 
     * changing the labels on rows to effect a copy. In general, we can do a fast copy if both directories are 
     * instances of ColDirectory and use a TranslateIO to store the index.
     */
    private boolean canFastCopy(Directory from, Directory to) {
        if (!(from instanceof ColDirectory))
            return false;
        if (!(to instanceof ColDirectory))
            return false;
        ColDirectory cfrom = (ColDirectory)from;
        if (!(cfrom.indexIO instanceof TranslateIO))
            return false;
        ColDirectory cto = (ColDirectory)to;
        if (!(cto.indexIO instanceof TranslateIO))
            return false;
        if (!cfrom.allowFastCopy || !cto.allowFastCopy)
            return false;
        if (!TranslateIO.canLink(cto.indexIO, cfrom.indexIO))
            return false;
        
        // you made it.
        return true;
    }
    
    // do the fast copy.
    private void fastCopy(Directory to, String src, String dest, IOContext context) throws IOException {
        ColDirectory cto = (ColDirectory)to;
        ColDirectory cfrom = this;
        
        TranslateIO fromIO = (TranslateIO)cfrom.indexIO;
        TranslateIO toIO = (TranslateIO)cto.indexIO;
        
        //System.out.println(String.format("copying %s->%s by linking %s->%s", src, dest, dest, fromIO.translate(src)));
        
        // link the file names
        toIO.link(dest, fromIO.translate(src));
        
        // also set the length (so the file can be discovered later)
        cto.meta.setLength(dest, cfrom.meta.getLength(src), true);
        
        // and remove from the src.
        cfrom.meta.delete(src);
    }
}
