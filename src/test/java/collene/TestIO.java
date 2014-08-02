package collene;

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
    private static final int rows = 100;
    private static final int cols = 100;
    
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
    }
    
    @Test
    public void testAllKeys() throws IOException {
        String[] allKeys = io.allKeys();
        Assert.assertEquals(rows, allKeys.length);
    }
    
    @Test
    public void testReadBack() throws IOException {
        for (int i = 0; i < rows * cols / 2; i++) {
            int row = rand.nextInt(rows);
            int col = rand.nextInt(cols);
                
            byte[] readData = io.get(Integer.toHexString(row), (long)col);
            Assert.assertArrayEquals(data[row][col], readData);
        }
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
    }
    
    @Test
    public void testHasKey() throws IOException {
        for (int i = 0; i < rows / 2; i++) {
            String goodRow = Integer.toHexString(rand.nextInt(rows));
            String badRow = Integer.toHexString(rand.nextInt(rows) + rows);
            
            Assert.assertTrue(io.hasKey(goodRow));
            Assert.assertFalse(io.hasKey(badRow));
        }
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
        
        list.add(new Object[]{
                new MemoryIO(8192)
        });
        list.add(new Object[]{
                new MemoryIO(4096)
        });
        // run the same test multiple times with the same cassandra database, keyspace and column family. Only change
        // the prefix. All data should still reside on the database at the end, but should be properly namespaced to
        // avoid collisions.
        list.add(new Object[]{
                parentIO.clone(NextCassandraPrefix.get())
        });
        list.add(new Object[]{
                parentIO.clone(NextCassandraPrefix.get())
        });
        list.add(new Object[]{
                parentIO.clone(NextCassandraPrefix.get())
        });
        
        return list;
    }
    
}
