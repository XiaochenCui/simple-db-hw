package simpledb;

import org.apache.log4j.Logger;

import java.lang.invoke.MethodHandles;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

public class ConcurrentStatus {

    final static Logger logger = Logger.getLogger(MethodHandles.lookup().lookupClass());

    private static AtomicReference<ConcurrentStatus> _instance = new AtomicReference<>(new ConcurrentStatus());

    private static final int POLL_INTERVAL = 20;
    private static final int TIMEOUT = 20 * 1000;

    private static HashMap<PageId, Set<TransactionId>> sLockMap = new HashMap<>();
    private static HashMap<PageId, TransactionId> xLockMap = new HashMap<>();

    private static HashMap<TransactionId, Set<PageId>> holdPages = new HashMap<>();

    private final WaitForGraph graph;

    private static ReentrantLock globalLock = new ReentrantLock();

    private ConcurrentStatus() {
        this.graph = new WaitForGraph();
    }

    private static WaitForGraph getGraph() {
        return _instance.get().graph;
    }

    private static final boolean showStatus = false;

    /**
     * Acquire a s/x globalLock on a page
     */
    public static void acquireLock(TransactionId transactionId, PageId pageId, Lock lock) throws TransactionAbortedException {

        if (Config.debugTransaction()) {
            logger.info(String.format("%s try to acquire %s on %s", transactionId, lock, pageId));
        }

        long startTime = System.currentTimeMillis();
        while ((System.currentTimeMillis() - startTime) < TIMEOUT) {
            if (lock.equals(Lock.SHARED_LOCK)) {

                // Check if there is deadlock before acquire the globalLock
                // 1. add edge
                if (xLockMap.containsKey(pageId)) {
                    getGraph().addEdge(transactionId, xLockMap.get(pageId));
                }
                // 2. check cycle
                if (getGraph().containsCycle()) {
                    throw new TransactionAbortedException();
                }

                if (!xLockMap.containsKey(pageId) || xLockMap.get(pageId).equals(transactionId)) {
                    addLock(transactionId, pageId, lock);
                    return;
                }
            } else if (lock.equals(Lock.EXCLUSIVE_LOCK)) {

                // Check if there is deadlock before acquire the globalLock
                // 1. add edge
                if (xLockMap.containsKey(pageId)) {
                    getGraph().addEdge(transactionId, xLockMap.get(pageId));
                }
                if (sLockMap.get(pageId) != null && sLockMap.get(pageId).size() != 0) {
                    globalLock.lock();
                    for (TransactionId tId1 : sLockMap.get(pageId)) {
                        getGraph().addEdge(transactionId, tId1);
                    }
                    globalLock.unlock();
                }

                // 2. check cycle
                if (getGraph().containsCycle()) {
                    logger.error("cycle detected");
                    throw new TransactionAbortedException();
                }

                if (!xLockMap.containsKey(pageId) || xLockMap.get(pageId).equals(transactionId)) {
                    // If transaction t is the only transaction holding a shared globalLock on
                    // an object o, t may upgrade its globalLock on o to an exclusive globalLock.
                    if (sLockMap.get(pageId) == null || sLockMap.get(pageId).size() == 0 || (sLockMap.get(pageId).size() == 1 && sLockMap.get(pageId).contains(transactionId))) {
                        addLock(transactionId, pageId, lock);
                        return;
                    }
                }
            }
            try {
                showStatus();
                Thread.sleep(POLL_INTERVAL);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        logger.error("timeout");
        throw new TransactionAbortedException();
    }

    public synchronized static void addLock(TransactionId transactionId, PageId pageId, Lock lock) {
        globalLock.lock();

        if (lock.equals(Lock.SHARED_LOCK)) {
            sLockMap.putIfAbsent(pageId, new HashSet<>());
            sLockMap.get(pageId).add(transactionId);

            holdPages.putIfAbsent(transactionId, new HashSet<>());
            holdPages.get(transactionId).add(pageId);

            if (Config.debugTransaction()) {
                logger.info(String.format("%s success acquire %s on %s", transactionId, lock, pageId));
            }
        } else if (lock.equals(Lock.EXCLUSIVE_LOCK)) {
            xLockMap.put(pageId, transactionId);

            holdPages.putIfAbsent(transactionId, new HashSet<>());
            holdPages.get(transactionId).add(pageId);

            if (Config.debugTransaction()) {
                logger.info(String.format("%s success acquire %s on %s", transactionId, lock, pageId));
            }
        }

        globalLock.unlock();
    }

    /**
     * Release a s/x globalLock on a page
     */
    public synchronized static void releaseLock(TransactionId transactionId, PageId pageId) {
        globalLock.lock();

        logger.debug(String.format("release %s's locks on %s", transactionId, pageId));
        if (sLockMap.get(pageId) != null) {
            sLockMap.get(pageId).remove(transactionId);
        }

        xLockMap.remove(pageId);

        globalLock.unlock();
    }

    /**
     * Release all locks on a page
     */
    public synchronized static void releaseAllLocks(PageId pageId) {
        globalLock.lock();

        if (Config.debugTransaction()) {
            logger.debug(String.format("release all locks on %s", pageId));
        }

        sLockMap.remove(pageId);
        xLockMap.remove(pageId);

        for (TransactionId transactionId : holdPages.keySet()) {
            holdPages.get(transactionId).remove(pageId);
        }

        globalLock.unlock();
    }

    /**
     * Release all locks on a transaction
     */
    public synchronized static void releaseAllLocks(TransactionId transactionId) {
        logger.debug(String.format("release all locks on %s", transactionId));

        if (holdPages.get(transactionId) != null) {
            for (PageId pageId : holdPages.get(transactionId)) {
                releaseLock(transactionId, pageId);
            }
        }

        globalLock.lock();
        holdPages.remove(transactionId);
        globalLock.unlock();
    }

    public synchronized static boolean holdsLock(TransactionId transactionId, PageId pageId) {
        if (sLockMap.get(pageId) != null && sLockMap.get(pageId).contains(transactionId))
            return true;

        if (xLockMap.get(pageId) != null && xLockMap.get(pageId).equals(transactionId))
            return true;

        return false;
    }

    public synchronized static void removeTransaction(TransactionId transactionId) {
        releaseAllLocks(transactionId);
        getGraph().removeVertex(transactionId);
    }

    public synchronized static void showStatus() {
        if (showStatus) {
            globalLock.lock();
            logger.debug("sLockMap: " + sLockMap);
            logger.debug("xLockMap: " + xLockMap);
            logger.debug("holdPages: " + holdPages);
            globalLock.unlock();
        }
    }
}
