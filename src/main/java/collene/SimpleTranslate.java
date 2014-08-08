package collene;


import java.io.IOException;

public class SimpleTranslate implements Translate {
    private final IO io;
    
    public SimpleTranslate(IO io) {
        this.io = io;
    }

    @Override
    public String translate(String key) throws IOException {
        byte[] bytes = io.get(key, 0L);
        if (bytes == null) {
            synchronized (key.intern()) {
                bytes = io.get(key, 0L);
                if (bytes == null) {
                    bytes = Utils.randomString(io.getColSize()).getBytes();
                    io.put(key, 0L, bytes);
                }
            }
        }
        return new String(bytes);
        
    }

    @Override
    public void setTranslation(String key, String translation) throws IOException {
        synchronized (key.intern()) {
            io.put(key, 0L, translation.getBytes());
        }
    }

    @Override
    public void unset(String key) throws IOException {
        synchronized (key.intern()) {
            io.delete(key);
        }
    }
}
