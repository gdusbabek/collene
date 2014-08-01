package collene;

import java.util.concurrent.atomic.AtomicInteger;

public final class NextCassandraPrefix {
    private static final String prefix = "unit-test";
    private static final AtomicInteger counter = new AtomicInteger(0);
    
    public static String get() {
        return String.format("%s.%d", prefix, counter.getAndIncrement());
    }
}
