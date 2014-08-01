package collene;

import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockFactory;

import java.io.IOException;

public class IoLockFactory extends LockFactory {
    private static final long LOCK_COL = 0;
    private static final byte LOCKED = 1;
    private static final byte UNLOCKED = 0;
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
            byte status = buf == null ? UNLOCKED : buf[0];
            if (status != LOCKED) {
                io.put(fullName, LOCK_COL, new byte[] {LOCKED});
            }
            return status != LOCKED;
        }

        @Override
        public void close() throws IOException {
            io.put(fullName, LOCK_COL, new byte[] {UNLOCKED});
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
