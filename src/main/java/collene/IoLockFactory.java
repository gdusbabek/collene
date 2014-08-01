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
