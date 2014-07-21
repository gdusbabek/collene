package collene;

import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockFactory;

import java.io.IOException;

public class IoLockFactory extends LockFactory {
    private static final long LOCK_COL = 0;
    private static final byte[] UNLOCKED = new byte[]{0};
    private static final byte[] LOCKED = new byte[]{1};
    private final IO io;
    
    IoLockFactory(IO io) {
        this.io = io;
    }

    @Override
    public Lock makeLock(String lockName) {
        String realName = this.getLockPrefix() + "-" + lockName;
        return new IoLock(realName);
    }

    @Override
    public void clearLock(String lockName) throws IOException {
        makeLock(lockName).close();
    }
    
    private class IoLock extends Lock {
        private String fullName;
        
        private IoLock(String fullName) {
            this.fullName = fullName;
        }
        
        @Override
        public boolean obtain() throws IOException {
            byte[] buf = io.get(fullName, LOCK_COL);
            if (buf == null) {
                buf = UNLOCKED;
            }
            if (buf[0] == 1)
                return false;
            else {
                buf[0] = 1;
                io.put(fullName, LOCK_COL, LOCKED);
                return true;
            }
            
        }

        @Override
        public void close() throws IOException {
            //io.put(fullName, LOCK_COL, UNLOCKED); // this should work, but doesn't.
            io.delete(fullName);
        }

        @Override
        public boolean isLocked() throws IOException {
            return io.get(fullName, LOCK_COL)[0] == 1;
        }
        
        public String toString() {
            return "IOLock:" + fullName;
        }
    }
}
