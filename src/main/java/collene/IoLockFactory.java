package collene;

import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockFactory;

import java.io.IOException;
import java.util.Random;

public class IoLockFactory extends LockFactory {
    private static final Random rand = new Random(System.nanoTime());
    private static final byte UNLOCKED = 0;
    private static final byte TRYING = 1;
    
    private final IO io;
    private final RowMeta rowMeta;
    
    public IoLockFactory(IO io, RowMeta rowMeta) {
        this.io = io;
        this.rowMeta = rowMeta;
    }

    @Override
    public synchronized Lock makeLock(String lockName) {
        if (lockPrefix != null)
            lockName = lockPrefix + "-" + lockName;
        return new IOLock(lockName);
    }

    @Override
    public void clearLock(String lockName) throws IOException {
        makeLock(lockName).close();
    }
    
    private class IOLock extends Lock {
        private final String path;
        private Object lock;
        // signature also is the tiebreaker.
        private final byte[] signature = new byte[] {
                2,
                (byte)rand.nextInt(),
                (byte)rand.nextInt(),
                (byte)rand.nextInt(),
                (byte)rand.nextInt(),
                (byte)rand.nextInt(),
                (byte)rand.nextInt(),
                (byte)rand.nextInt()
        };
        private long longSignature = Utils.bytesToLong(signature);
        
        public IOLock(String path) {
            this.path = path;    
        }
        
        @Override
        public synchronized boolean obtain() throws IOException {
            // see if you already own this lock.
            if (lock != null)
                return true; 
            
            // get the contents.
            byte[] buf = io.get(path, 0);
            byte status = buf == null ? UNLOCKED : buf[0];
            
            if (status == UNLOCKED) {
                return tryLock();
            } else {
                // is locked or someone else is already trying.
                return false;
            }
        }
        
        // still prone to races but will yeild a clear winner most of the time.
        private boolean tryLock() throws IOException {
            // indicate locked.
            io.put(path, 0, new byte[]{TRYING});
            // cast your lot.
            io.put(path, longSignature, signature);
            // wait a second.
            try { Thread.currentThread().sleep(1000); } catch (Exception ex) {};
            
            long max = Long.MIN_VALUE;
            for (byte[] buf : io.allValues(path)) {
                if (buf.length == 8) {
                    long value = Utils.bytesToLong(buf);
                    max = Math.max(max, value);
                }
            }
            
            if (max == longSignature) {
                // I win.
                lock = signature;
                rowMeta.setLength(path, 0, true);
                return true;
            } else {
                // remove your lot.
                io.delete(path, longSignature);
                return false;
            }
        }


        @Override
        public void close() throws IOException {
            io.delete(path, longSignature);
            if (lock != null) {
                io.delete(path, 0);
            }
        }

        @Override
        public boolean isLocked() throws IOException {
            //if (lock != null) return true;
            byte[] buf = io.get(path, 0);
            return buf != null && buf[0] != UNLOCKED;
        }
        
        public String toString() {
            return "IOLock:" + path;
        }
    }
}
