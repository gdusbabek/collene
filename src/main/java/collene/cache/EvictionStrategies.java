package collene.cache;

import collene.time.Clock;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;

public class EvictionStrategies {
    
    public static EvictionStrategy ALWAYS = new AlwaysEvict();
    public static EvictionStrategy NEVER = new NeverEvict();
    public static EvictionStrategy LAST_GET(long age, Clock clock) {
        return new EvictByOldestGet(age, clock);
    }
    public static EvictionStrategy LAST_PUT(long age, Clock clock) {
        return new EvictByOldestPut(age, clock);
    }
    
    private static class NeverEvict implements EvictionStrategy {
        @Override
        public void noteGet(String key, long col) {}

        @Override
        public void notePut(String key, long col) {}

        @Override
        public boolean shouldEvict(String key, long col) { return false; }

        @Override
        public void remove(String key, long col) {}
    }
    
    private static class AlwaysEvict extends NeverEvict {

        @Override
        public boolean shouldEvict(String key, long col) {
            return true;
        }
    }
    
    private static abstract class EvictByTime implements EvictionStrategy {
        private final Clock clock;
        private final long age;
        private final Table<String, Long, Long> accessed = HashBasedTable.create();
        
        public EvictByTime(long age, Clock clock) {
            this.age = age;
            this.clock = clock;
        }
        
        @Override
        public void noteGet(String key, long col) {
            accessed.put(key, col, clock.time());
        }

        @Override
        public void notePut(String key, long col) {
            accessed.put(key, col, clock.time());
        }

        @Override
        public boolean shouldEvict(String key, long col) {
            return accessed.contains(key, col) && clock.time() - accessed.get(key, col) >= age;
        }

        @Override
        public void remove(String key, long col) {
            accessed.remove(key, col);
        }
    }
    
    private static class EvictByOldestGet extends EvictByTime {
        public EvictByOldestGet(long age, Clock clock) {
            super(age, clock);
        }

        @Override
        public void notePut(String key, long col) { /* NOOP */ }
    }
    
    private static class EvictByOldestPut extends EvictByTime {
        public EvictByOldestPut(long age, Clock clock) {
            super(age, clock);
        }

        @Override
        public void noteGet(String key, long col) { }
    }
}
