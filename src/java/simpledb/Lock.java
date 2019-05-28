package simpledb;

public class Lock {

    private final int lock;

    private Lock(int lock) {this.lock = lock;}

    public String toString() {
        if (lock == 0)
            return "SHARED_LOCK";
        if (lock == 1)
            return "EXCLUSIVE_LOCK";
        return "UNKNOWN";
    }

    public static final Lock SHARED_LOCK = new Lock(0);
    public static final Lock EXCLUSIVE_LOCK = new Lock(1);

}
