package collene.time;

public abstract class Clock {
    public abstract long time();
    
    public static final Clock SystemClock = new Clock() {
        @Override
        public long time() {
            return System.currentTimeMillis();
        }
    };
}
