package collene;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockFactory;

import java.io.IOException;
import java.util.Collection;

public class ColDirectory extends Directory {
    private LockFactory lockFactory;
    private IO indexIO;
    private IO metaIO;
    
    private ColDirectory(IO indexIO, IO metaIO, LockFactory lockFactory) {
        this.indexIO = indexIO;
        this.metaIO = metaIO;
        this.lockFactory = lockFactory;
    }

    public static ColDirectory open(IO indexIO, IO metaIO, IO lockIO) {
        return new ColDirectory(indexIO, metaIO, new IoLockFactory(lockIO));    
    }
    
    /**
     * all bits of data associated with this index. Let's make these row keys.
     */
    @Override
    public String[] listAll() throws IOException {
        return metaIO.allKeys(); // should be the same as indexIO though.
    }

    @Override
    public void deleteFile(String name) throws IOException {
        indexIO.delete(name);
        metaIO.delete(name);
    }

    @Override
    public long fileLength(String name) throws IOException {
        return new RowMeta(name, metaIO).length();
    }

    @Override
    public void sync(Collection<String> names) throws IOException {
        // this is a no-op.
    }

    @Override
    public Lock makeLock(String name) {
        return lockFactory.makeLock(name);
    }

    @Override
    public void clearLock(String name) throws IOException {
        if (lockFactory != null) {
            lockFactory.clearLock(name);
        }
    }

    @Override
    public void close() throws IOException {
        for (String key : listAll()) {
            clearLock(key);
        }
    }

    @Override
    public void setLockFactory(LockFactory lockFactory) throws IOException {
        assert lockFactory != null;
        this.lockFactory = lockFactory;
        lockFactory.setLockPrefix(this.getLockID());
    }

    @Override
    public LockFactory getLockFactory() {
        return this.lockFactory;
    }

    @Override
    public IndexOutput createOutput(String name, IOContext context) throws IOException {
        return new RowIndexOutput(name, new RowWriter(name, indexIO, metaIO));
    }

    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
        return new RowIndexInput(name, new RowReader(name, indexIO, metaIO));
    }

    @Override
    public boolean fileExists(String s) throws IOException {
        return metaIO.hasKey(s);
    }
}
