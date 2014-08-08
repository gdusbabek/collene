package collene;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class TestTranslate {
    
    IO mappingIO;
    IO dataIO;
    Translate translate;
    TranslateIO io;
    
    @Before
    public void setup() {
        mappingIO = new MemoryIO(32);
        dataIO = new MemoryIO(256);
        translate = new SimpleTranslate(mappingIO);
        io = new TranslateIO(translate, dataIO);    
    }
    
    @Test
    public void testTranslation() throws Exception {
        byte[] aaa0 = TestUtil.randomString(io.getColSize()).getBytes();
        byte[] aaa1 = TestUtil.randomString(io.getColSize()).getBytes();
        byte[] bbb0 = TestUtil.randomString(io.getColSize()).getBytes();
        
        io.put("aaa", 0L, aaa0);
        io.put("aaa", 1L, aaa1);
        io.put("bbb", 0L, bbb0);
        
        String aaaTrans = translate.translate("aaa");
        String bbbTrans = translate.translate("bbb");
        
        // these would work anyway.
        Assert.assertArrayEquals(aaa0, io.get("aaa", 0L));
        Assert.assertArrayEquals(aaa1, io.get("aaa", 1L));
        Assert.assertArrayEquals(bbb0, io.get("bbb", 0L));
        
        Assert.assertFalse("aaa".equals(aaaTrans));
        Assert.assertFalse("bbb".equals(bbbTrans));
        
        // these values should not be un the underlying IO.
        Assert.assertNull(dataIO.get("aaa", 0L));
        Assert.assertNull(dataIO.get("aaa", 1L));
        Assert.assertNull(dataIO.get("bbb", 0L));
    }
    
    @Test
    public void testResetting() throws IOException {
        byte[] aaa0 = TestUtil.randomString(io.getColSize()).getBytes();
        io.put("aaa", 0L, aaa0);
        String translation = translate.translate("aaa");
        Assert.assertNull(io.get("bbb", 0L));
        
        Assert.assertArrayEquals(aaa0, dataIO.get(translation, 0L));
        Assert.assertArrayEquals(aaa0, io.get("aaa", 0L));
        
        
        io.link("bbb", translation);
        
        Assert.assertArrayEquals(aaa0, io.get("aaa", 0L));
        Assert.assertArrayEquals(aaa0, io.get("bbb", 0L));
        
    }
}
