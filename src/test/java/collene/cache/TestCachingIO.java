package collene.cache;

import collene.IO;
import collene.MemoryIO;
import collene.TestUtil;
import collene.time.Clock;
import com.google.common.collect.Table;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.internal.util.reflection.Whitebox;

import java.io.IOException;

public class TestCachingIO {
    
    private static final int rows = 10;
    private static final int cols = 10;
    
    private SetClock clock;
    
    @Before
    public void setup() {
        this.clock = new SetClock();
    }
    
    @Test
    public void testNeverEvict() throws Exception {
        CachingIO io = new CachingIO(new MemoryIO(256), false, EvictionStrategies.NEVER);
        putManyThings(io);
        clock.setTime(clock.time() + 10);
        getFewThings(io);
        clock.setTime(clock.time() + 10);
        io.flush(false);
        io.forceEvictions();

        Assert.assertEquals(rows * cols, countCacheItems(io));
    }
    
    @Test
    public void testAlwaysEvict() throws Exception {
        CachingIO io = new CachingIO(new MemoryIO(256), false, EvictionStrategies.ALWAYS);
        putManyThings(io);
        clock.setTime(clock.time() + 10);
        getFewThings(io);
        clock.setTime(clock.time() + 10);
        io.flush(false);
        io.forceEvictions();
        
        Assert.assertEquals(0, countCacheItems(io));
    }
    
    @Test
    public void testEvictByLastRead() throws Exception {
        CachingIO io = new CachingIO(new MemoryIO(256), false, EvictionStrategies.LAST_GET(10, clock));
        putManyThings(io);
        clock.setTime(clock.time() + 10);
        getFewThings(io);
        clock.setTime(clock.time() + 10);
        io.flush(false);
        io.forceEvictions();
        
        Assert.assertEquals(rows * cols - Math.min(rows, cols), countCacheItems(io));
    }
    
    @Test
    public void testEvictByLastWrite() throws Exception {
        CachingIO io = new CachingIO(new MemoryIO(256), false, EvictionStrategies.LAST_PUT(10, clock));
        putManyThings(io);
        Assert.assertEquals(rows * cols, countCacheItems(io));
        clock.setTime(clock.time() + 10);
        getFewThings(io);
        clock.setTime(clock.time() + 10);
        putFewThings(io);
        io.flush(false);
        io.forceEvictions();
        
        Assert.assertEquals(Math.min(rows, cols), countCacheItems(io));
        
        clock.setTime(clock.time() + 10);
        io.flush(false);
        io.forceEvictions();
        
        Assert.assertEquals(0, countCacheItems(io));
    }
    
    private static void putManyThings(IO io) throws IOException {
        for (int r = 0; r < rows; r++) {
            String key = Integer.toHexString(r);
            for (long c = 0; c < cols; c++) {
                io.put(key, c, TestUtil.randomString(io.getColSize()).getBytes());
            }
        }
    }
    
    private static void getFewThings(IO io) throws IOException {
        for (int x = 0; x < Math.min(rows, cols); x++) {
            io.get(Integer.toHexString(x), (long)x);
        }
    }
    
    private static void putFewThings(IO io) throws IOException {
        for (int x = 0; x < Math.min(rows, cols); x++) {
            io.put(Integer.toHexString(x), (long)x, TestUtil.randomString(io.getColSize()).getBytes());
        }
    }
    
    private static int countCacheItems(IO io) throws Exception {
        if (!(io instanceof CachingIO)) {
            throw new Exception("Wrong type: " + io.getClass().getSimpleName());
        }

        Table<String, Long, byte[]> cache = (Table<String, Long, byte[]>)Whitebox.getInternalState(io, "cache");
        return cache.size();
    }
    
    private static class SetClock extends Clock {
        private long time = 0L;

        public void setTime(long time) {
            this.time = time;
        }

        @Override
        public long time() {
            return time;
        }
    }
}
