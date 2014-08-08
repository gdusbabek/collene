package collene;

import collene.cache.CachingIO;
import com.google.common.collect.Sets;
import org.cassandraunit.CassandraCQLUnit;
import org.cassandraunit.dataset.cql.ClassPathCQLDataSet;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

@RunWith(Parameterized.class)
public class TestIO {
    private static final boolean isTravis = System.getenv().containsKey("TRAVIS") && System.getenv().get("TRAVIS").equals("true");
    private static final Random rand = new Random(System.nanoTime());
    private static final int rows = 50;
    private static final int cols = 50;
    
    public static CassandraCQLUnit cassandra = new CassandraCQLUnit(new ClassPathCQLDataSet("ddl.cql", "collene"), "/cassandra.yaml", "127.0.0.1", 9042) {{
        try {
            if (!isTravis) {
                this.before();
            }
            this.load();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }};
    
    private IO io;
    private byte[][][] data;
    
    public TestIO(IO io) {
        this.io = io;
    }
    
    // in some cases this will be called with data already in the IO. that's fine.
    @Before
    public void putData() throws IOException {
        data = new byte[rows][][];
        for (int r = 0; r < rows; r++) {
            data[r] = new byte[cols][];
            for (int c = 0; c < cols; c++) {
                data[r][c] = TestUtil.randomString(io.getColSize()).getBytes();
                io.put(Integer.toHexString(r), (long)c, data[r][c]);
            }
        }
        
        // caching IOs need to be flushed in order to see writes.
        if (io instanceof CachingIO) {
            ((CachingIO) io).flush(true);
        }
    }
    
    @Test
    public void testReadBack() throws IOException {
        for (int i = 0; i < rows * cols / 2; i++) {
            int row = rand.nextInt(rows);
            int col = rand.nextInt(cols);
                
            byte[] readData = io.get(Integer.toHexString(row), (long)col);
            Assert.assertArrayEquals(data[row][col], readData);
        }
        System.out.print(".");
    }
    
    @Test
    public void testUpdate() throws IOException {
        for (int i = 0; i < rows * cols / 2; i++) {
            String row = Integer.toHexString(rand.nextInt(rows));
            long col = rand.nextInt(cols);
            
            byte[] oldReadData = io.get(row, col);
            byte[] newData = TestUtil.randomString(io.getColSize()).getBytes();
            assertArrayNotEqual(oldReadData, newData);
            
            io.put(row, col, newData);
            byte[] newReadData = io.get(row, col);
            Assert.assertArrayEquals(newData, newReadData);
            assertArrayNotEqual(newReadData, oldReadData);
        }
        System.out.print(".");
    }
    
    @Test
    public void testHasKey() throws IOException {
        for (int i = 0; i < rows / 2; i++) {
            String goodRow = Integer.toHexString(rand.nextInt(rows));
            String badRow = Integer.toHexString(rand.nextInt(rows) + rows);
            
            Assert.assertTrue(io.hasKey(goodRow));
            Assert.assertFalse(io.hasKey(badRow));
        }
        System.out.print(".");
    }
    
    @Test
    public void testDelete() throws IOException {
        Set<String> deleted = new HashSet<>();
        while (deleted.size() < rows / 2) {
            String candidateKey = Integer.toHexString(rand.nextInt(rows));
            
            if (io.hasKey(candidateKey)) {
                Assert.assertFalse(deleted.contains(candidateKey));
                io.delete(candidateKey);
                deleted.add(candidateKey);
            } else {
                Assert.assertTrue(deleted.contains(candidateKey));
            }
        }
        
        // ensure that a bunch of random columns not in deleted are still there.
        int existing = 1000;
        while (existing-- > 0) {
            String existingKey = null;
            int existingRow = 0;
            while (existingKey == null || deleted.contains(existingKey)) {
                existingRow = rand.nextInt(rows);
                existingKey = Integer.toHexString(existingRow);
            }
            
            int col = rand.nextInt(cols);
            byte[] readData = io.get(existingKey, col);
            Assert.assertNotNull(data);
            Assert.assertArrayEquals(data[existingRow][col], readData);
        }
        
        // ensure that a bunch of random colunn in deleted are gone.
        int notExisting = 1000;
        while (notExisting-- > 0) {
            String deadKey = null;
            int deadRow = 0;
            while (deadKey == null || !deleted.contains(deadKey)) {
                deadRow = rand.nextInt(rows);
                deadKey = Integer.toHexString(deadRow);
            }
            
            int col = rand.nextInt(cols);
            byte[] deadData = io.get(deadKey, col);
            Assert.assertNull(deadData);
            Assert.assertFalse(io.hasKey(deadKey));
        }
        System.out.print(".");
    }
    
    @Test
    public void testDeleteRowAndColumn() throws IOException {
        int existing = 1000;
        while (existing > 0) {
            String candidateKey = Integer.toHexString(rand.nextInt(rows));
            long candidateCol = (long)rand.nextInt(cols);
            if (!io.hasKey(candidateKey)) {
                continue;
            }
            if (io.get(candidateKey, candidateCol) == null) {
                continue;
            }
            
            Assert.assertNotNull(io.get(candidateKey, candidateCol));
            io.delete(candidateKey, candidateCol);
            Assert.assertNull(io.get(candidateKey, candidateCol));
            
            existing -= 1;
        }
        System.out.print(".");
    }
    
    @Test
    public void testAllValues() throws IOException {
        Set<String> allValues = new HashSet<String>();
        for (int r = 0; r < rows; r++) {
            for (int c = 0; c < cols; c++) {
                allValues.add(new String(data[r][c]));
            }
        }
        
        // will blow up here if we have the misfortune of a string collision.
        Assert.assertEquals("Likely random string collision", rows * cols, allValues.size());
        
        Set<String> dbAllValues = new HashSet<String>();
        for (int r = 0; r < rows; r++) {
            for (byte[] bb : io.allValues(Integer.toHexString(r))) {
                dbAllValues.add(new String(bb));
            }
        }
        Assert.assertTrue(rows * cols > 0);
        Assert.assertEquals(allValues.size(), dbAllValues.size());
        Assert.assertEquals(0, Sets.difference(allValues, dbAllValues).size());
        System.out.print(".");
    }
    
    @Test
    public void ok() {
        System.out.println("ok");
    }
    
    private static void assertArrayNotEqual(byte[] a, byte[] b) {
        if (a.length == b.length) {
            for (int i = 0; i < a.length; i++) {
                if (a[i] != b[i]) {
                    return;
                }
            }
            throw new AssertionError("These arrays are equal!");
        }
    }
    
    @Parameterized.Parameters
    public static Collection<Object[]> data() throws Exception {
        Collection<Object[]> list = new ArrayList<>();
        
        CassandraIO parentIO = new CassandraIO("dummy", 32, "collene", "cindex").session(cassandra.session);
        
        IO[] ios = new IO[]{
                new MemoryIO(8192),
                new MemoryIO(4096),
                new SplitRowIO(256, ",", new MemoryIO(1024)),
                new CachingIO(new MemoryIO(1024)),
                
                // note that we turn autoflush on for this CCIO instance. It's required, else no puts are recorded in
                // the backing IO.
                new SplitRowIO(256, ",", new CachingIO(new MemoryIO(1024), true)),
                new CachingIO(new SplitRowIO(256, ",", new MemoryIO(1024))),
                
                // run the same test multiple times with the same cassandra database, keyspace and column family. Only change
                // the prefix. All data should still reside on the database at the end, but should be properly namespaced to
                // avoid collisions.
                parentIO.clone(NextCassandraPrefix.get()),
                parentIO.clone(NextCassandraPrefix.get()),
                parentIO.clone(NextCassandraPrefix.get()),
                
                // mix things up.
                new SplitRowIO(256, ",", parentIO.clone(NextCassandraPrefix.get())),
                new CachingIO(parentIO.clone(NextCassandraPrefix.get())),
        };
        
        for (IO io : ios) {
            list.add(new Object[]{ io });
        }
        
        return list;
    }
    
}
