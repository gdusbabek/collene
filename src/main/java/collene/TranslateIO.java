package collene;

import java.io.IOException;

public class TranslateIO implements IO {
    private final Translate trans;
    private final IO io;
    
    public TranslateIO(Translate trans, IO io) {
        this.trans = trans;
        this.io = io;
    }

    @Override
    public void put(String key, long col, byte[] value) throws IOException {
        io.put(trans.translate(key), col, value);
    }

    @Override
    public byte[] get(String key, long col) throws IOException {
        return io.get(trans.translate(key), col);
    }

    @Override
    public int getColSize() {
        return io.getColSize();
    }

    @Override
    public void delete(String key) throws IOException {
        io.delete(trans.translate(key));
        trans.unset(key);
    }

    @Override
    public void delete(String key, long col) throws IOException {
        io.delete(trans.translate(key), col);
    }

    @Override
    public boolean hasKey(String key) throws IOException {
        return io.hasKey(trans.translate(key));
    }

    @Override
    public Iterable<byte[]> allValues(String key) throws IOException {
        return io.allValues(trans.translate(key));
    }
    
    public String translate(String key) throws IOException {
        return trans.translate(key);
        
    }
    
    public void link(String newKey, String underlyingKey) throws IOException {
        trans.setTranslation(newKey, underlyingKey);
    }
    
    public static boolean canLink(IO a, IO b) {
        if (!(a instanceof TranslateIO))
            return false;
        if (!(b instanceof TranslateIO))
            return false;
        return true;
    }
}
