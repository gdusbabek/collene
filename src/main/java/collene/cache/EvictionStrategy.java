package collene.cache;

public interface EvictionStrategy {
    
    public void noteGet(String key, long col);
    public void notePut(String key, long col);
    public void remove(String key, long col);
    public boolean shouldEvict(String key, long col);
}
