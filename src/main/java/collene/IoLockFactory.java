package collene;

import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockFactory;

import java.io.IOException;
import java.util.Random;

/**
 * Simple lock factory that uses an IO. @todo Needs testing. I've modeled it after 
 * org.apache.lucene.store.NativeFSLockFactory which may or may not have been a good choice.
 * 
 * @todo There needs to be a way to force clean locks. This will be easy to implement.
 */
public class IoLockFactory extends LockFactory {
    private static final Random rand = new Random(System.nanoTime());
    private static final byte UNLOCKED = 0;
    private static final byte TRYING = 1;
    private static final byte LOCKED = 2;
    
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
    
    // this is where the work of locking actually happens. Every method of the abstract Lock is overridden, essentially
    // treating it as an interface.
    private class IOLock extends Lock {
        private final String path;
        private Object lock;
        
        // this is essentially a 64 bit random with the first byte forced to something that makes comparisons (isLocked)
        // easy.
        private final byte[] signature = new byte[] {
                LOCKED,
                (byte)rand.nextInt(),
                (byte)rand.nextInt(),
                (byte)rand.nextInt(),
                (byte)rand.nextInt(),
                (byte)rand.nextInt(),
                (byte)rand.nextInt(),
                (byte)rand.nextInt()
        };
        
        // convert the signature to a long for fast comparisons with other signatures.
        private long longSignature = Utils.bytesToLong(signature);
        
        public IOLock(String path) {
            this.path = path;    
        }

        /**
         * The documentation for org.apache.lucene.store.Lock doesn't specify what the behavior should be if this 
         * thread already has the lock. But the code in NativeFSLock makes me think that I should return true when
         * that happens.
         * @return true iff the lock has been obtained by this thread and process.
         * @throws IOException
         */
        @Override
        public synchronized boolean obtain() throws IOException {
            // see if you already own this lock.
            if (lock != null)
                return true; 
            
            // get the contents.
            byte[] buf = io.get(path, 0);
            
            // determine the status.
            byte status = buf == null ? UNLOCKED : buf[0];
            
            // if it is unlocked, try to get the lock.
            if (status == UNLOCKED) {
                return tryLock();
            } else {
                // is locked or someone else is already trying.
                return false;
            }
        }
        
        // still prone to races but will yeild a clear winner most of the time.
        private boolean tryLock() throws IOException {
            // indicate locked. may overwrite someone elses claim.
            io.put(path, 0, new byte[]{TRYING});
            
            // cast your lot. this is how ties are broken when multiple people are trying to lock.
            io.put(path, longSignature, signature);
            
            // wait a second. this gives other process a chance to cast their lots.
            try { Thread.currentThread().sleep(1000); } catch (Exception ex) {};
            
            // the biggest requesting signature wins.
            long max = Long.MIN_VALUE;
            for (byte[] buf : io.allValues(path)) {
                if (buf.length == 8) {
                    long value = Utils.bytesToLong(buf);
                    max = Math.max(max, value);
                }
            }
            
            // determine if this thread won.
            if (max == longSignature) {
                // I win.
                lock = signature;
                rowMeta.setLength(path, 0, true);
                return true;
            } else {
                // I lose. remove my lot.
                io.delete(path, longSignature);
                return false;
            }
        }

        /** @inheritDoc */
        @Override
        public void close() throws IOException {
            io.delete(path, longSignature);
            if (lock != null) {
                io.delete(path, 0);
            }
        }

        /** keep in mind this does a database read. @return true if anybody owns the lock. */
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
