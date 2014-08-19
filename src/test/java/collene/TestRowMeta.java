package collene;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Collection;

public class TestRowMeta {
    private static final int rows = 50;
    
    @Test
    public void testSplitRowIO() throws IOException {
        IO io = new SplitRowIO(20, "/", new MemoryIO(256));
        for (int i = 0; i < 50; i++) {
            io.put("row", i, TestUtil.randomString(io.getColSize()).getBytes());
        }
        
        Collection<byte[]> values = Utils.asCollection(io.allValues("row"));
        Assert.assertEquals(50, values.size());
    }
    
    @Test
    public void testSimpleMem() throws IOException {
        testMeta(new RowMeta(new MemoryIO(256)));
    }
    
    @Test
    public void testSplitRowMeta() throws IOException {
        testMeta(new RowMeta(new SplitRowIO(20, "/", new MemoryIO(256))));
    }
    
    private void testMeta(RowMeta rowMeta) throws IOException {
        for (int i = 0; i < rows; i++) {
            rowMeta.setLength("abcdefghijklmnopqrstuvwxyz " + i, 4096, true);
        }
        String[] keys = rowMeta.allKeys();
        Assert.assertEquals(rows, keys.length);
    }
}
