package simpledb;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

public class ConcurrentStatus {
    private static AtomicReference<ConcurrentStatus> _instance = new AtomicReference<>(new ConcurrentStatus());

    private static HashMap<PageId, Set<TransactionId>> sLockMap = new HashMap<>();
    private static HashMap<PageId, TransactionId> xLockMap = new HashMap<>();

    private static HashMap<TransactionId, Set<PageId>> holdPages = new HashMap<>();

    private final WaitForGraph graph;

    private ConcurrentStatus() {
        this.graph = new WaitForGraph();
    }

    private static WaitForGraph getGraph() {
        return _instance.get().graph;
    }

    /**
     * Acquire a s/x lock on a page
     */
    public synchronized static void acquireLock(TransactionId transactionId, PageId pageId, Lock lock) throws TransactionAbortedException {
        int MAX_WAIT = 3 * 1000;
        long startTime = System.currentTimeMillis();
        while ((System.currentTimeMillis() - startTime) < MAX_WAIT) {
            try {
                if (lock.equals(Lock.SHARED_LOCK)) {

                    // Check if there is deadlock before acquire the lock
                    // 1. add edge
                    if (xLockMap.containsKey(pageId)) {
                        getGraph().addEdge(transactionId, xLockMap.get(pageId));
                    }
                    // 2. check cycle
                    if (getGraph().containsCycle()) {
                        throw new TransactionAbortedException();
                    }

                    if (!xLockMap.containsKey(pageId) || xLockMap.get(pageId).equals(transactionId)) {
                        sLockMap.putIfAbsent(pageId, new HashSet<>());
                        sLockMap.get(pageId).add(transactionId);

                        holdPages.putIfAbsent(transactionId, new HashSet<>());
                        holdPages.get(transactionId).add(pageId);
                        return;
                    }
                } else if (lock.equals(Lock.EXCLUSIVE_LOCK)) {

                    // Check if there is deadlock before acquire the lock
                    // 1. add edge
                    if (xLockMap.containsKey(pageId)) {
                        getGraph().addEdge(transactionId, xLockMap.get(pageId));
                    }
                    if (sLockMap.get(pageId) != null && sLockMap.get(pageId).size() != 0) {
                        for (TransactionId tId1: sLockMap.get(pageId)) {
                            getGraph().addEdge(transactionId, tId1);
                        }
                    }
                    // 2. check cycle
                    if (getGraph().containsCycle()) {
                        System.out.println("[contain cycle] acquire lock failed: " + transactionId + ", " + pageId + ", " + lock);
                        throw new TransactionAbortedException();
                    }

                    if (!xLockMap.containsKey(pageId) || xLockMap.get(pageId).equals(transactionId)) {
                        // If transaction t is the only transaction holding a shared lock on
                        // an object o, t may upgrade its lock on o to an exclusive lock.
                        if (sLockMap.get(pageId) == null || (sLockMap.get(pageId).size() == 1 && sLockMap.get(pageId).contains(transactionId))) {
                            xLockMap.put(pageId,transactionId);

                            holdPages.putIfAbsent(transactionId, new HashSet<>());
                            holdPages.get(transactionId).add(pageId);
                            return;
                        }
                    }
                }
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        throw new TransactionAbortedException();
    }

    /**
     * Release a s/x lock on a page
     */
    public synchronized static void releaseLock(TransactionId transactionId, PageId pageId) {
        if (sLockMap.get(pageId) != null) {
            sLockMap.get(pageId).remove(transactionId);
        }
        xLockMap.remove(pageId);

        if (holdPages.get(transactionId) != null) {
            holdPages.get(transactionId).remove(pageId);
        }
    }

    /**
     * Release all locks on a page
     */
    public synchronized static void releaseAllLocks(PageId pageId) {
        sLockMap.remove(pageId);
        xLockMap.remove(pageId);

        for (TransactionId transactionId: holdPages.keySet()) {
            holdPages.get(transactionId).remove(pageId);
        }
    }

    /**
     * Release all locks on a transaction
     */
    public synchronized static void releaseAllLocks(TransactionId transactionId) {
        for (PageId pageId: holdPages.get(transactionId)) {
            releaseLock(transactionId,pageId);
        }

        holdPages.remove(transactionId);
    }

    public synchronized static boolean holdsLock(TransactionId transactionId, PageId pageId) {
        if (sLockMap.get(pageId) != null || xLockMap.containsKey(pageId)) {
            return true;
        }
        return false;
    }

    public synchronized static void removeTransaction(TransactionId transactionId) {
        getGraph().removeRertex(transactionId);
    }
}
