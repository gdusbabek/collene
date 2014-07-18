package collene;

import java.util.HashMap;
import java.util.Map;

public class Pointers {
    private final Map<String, Long> map = new HashMap<String, Long>();
    
    public long get(String key) {
        Long p = map.get(key);
        return p == null ? 0L : p;
    }
    
    public void set(String key, long l) {
        map.put(key, l);
    }
}
