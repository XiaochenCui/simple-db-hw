package simpledb;

import org.apache.log4j.Logger;

import java.io.*;

import java.lang.invoke.MethodHandles;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 *
 * @Threadsafe, all fields are final
 */
public class BufferPool {

    final static Logger logger = Logger.getLogger(MethodHandles.lookup().lookupClass());

    /**
     * Bytes per page, including header.
     */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;

    /**
     * Default number of pages passed to the constructor. This is used by
     * other classes. BufferPool should use the numPages argument to the
     * constructor instead.
     */
    public static final int DEFAULT_PAGES = 50;

    private int numPages = 0;
    public ConcurrentHashMap<PageId, Page> buffer;

    // Used to evict oldest page in the buffer pool
    public ConcurrentHashMap<PageId, Long> accessTime;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        this.numPages = numPages;
        this.buffer = new ConcurrentHashMap<>();

        this.accessTime = new ConcurrentHashMap<>();
    }

    public static int getPageSize() {
        return pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
        BufferPool.pageSize = pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
        BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid  the ID of the transaction requesting the page
     * @param pid  the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
            throws TransactionAbortedException, DbException {
        // block and acquire the desired lock before returning a page
        Lock lock = null;
        try {
            if (perm.equals(Permissions.READ_ONLY)) {
                lock = Lock.SHARED_LOCK;
            } else if (perm.equals(Permissions.READ_WRITE)) {
                lock = Lock.EXCLUSIVE_LOCK;
            }
            ConcurrentStatus.acquireLock(tid, pid, lock);
            ConcurrentStatus.showStatus();
        } catch (TransactionAbortedException e) {
            // Release all locks hold by tid
            logger.info("acquire lock failed: " + tid + ", " + pid + ", " + lock);
            ConcurrentStatus.releaseAllLocks(tid);
            throw new TransactionAbortedException();
        }

        Page page = this.buffer.get(pid);

        if (page == null) {
            DbFile f = Database.getCatalog().getDatabaseFile(pid.getTableId());

            if (Config.debugPageRead()) {
                logger.debug(String.format("Read page %s from %s", pid, f.getFile()));
            }

            page = f.readPage(pid);

            // evict page
            evictPage();

            // than insert
            buffer.put(pid, page);
        }

        // refresh accessTime
        Date date = new Date();
        accessTime.put(pid, date.getTime());

        return page;
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public void releasePage(TransactionId tid, PageId pid) {
        ConcurrentStatus.releaseAllLocks(pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        transactionComplete(tid, true);
    }

    /**
     * Return true if the specified transaction has a lock on the specified page
     */
    public boolean holdsLock(TransactionId tid, PageId p) {
        return ConcurrentStatus.holdsLock(tid, p);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid    the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
            throws IOException {
        for (PageId pageId : buffer.keySet()) {
            if (commit) {
                flushPages(tid);
                Page page = buffer.get(pageId);
                page.setBeforeImage();
            } else {
                Page page = buffer.get(pageId);
                if (page.isDirty() != null) {
                    discardPage(pageId);
                }
            }
        }

        ConcurrentStatus.removeTransaction(tid);
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other
     * pages that are updated (Lock acquisition is not needed for lab2).
     * May block if the lock(s) cannot be acquired.
     * <p>
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid     the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t       the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1

        // insert tuple
        ArrayList<Page> pageArrayList = Database.getCatalog().getDatabaseFile(tableId).insertTuple(tid, t);

        for (Page page : pageArrayList) {
            // set pages as dirty
            page.markDirty(true, tid);
            // put page to buffer
            buffer.put(page.getId(), page);
        }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     * <p>
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid the transaction deleting the tuple.
     * @param t   the tuple to delete
     */
    public void deleteTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1

        // get page which the tuple on
        PageId pageId = t.getRecordId().getPageId();

        // acquire a write lock on the page (and any other pages that are updated)

        // delete tuple
        int tableId = pageId.getTableId();
        Database.getCatalog().getDatabaseFile(tableId).deleteTuple(tid, t);
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     * break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1
        for (PageId pageId : buffer.keySet()) {
            flushPage(pageId);
        }
    }

    /**
     * Remove the specific page id from the buffer pool.
     * Needed by the recovery manager to ensure that the
     * buffer pool doesn't keep a rolled back page in its
     * cache.
     * <p>
     * Also used by B+ tree files to ensure that deleted pages
     * are removed from the cache so they can be reused safely
     */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // not necessary for lab1
        buffer.remove(pid);

        accessTime.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     *
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1

        Page page = buffer.get(pid);
        Database.getCatalog().getDatabaseFile(pid.getTableId()).writePage(page);
        page.markDirty(false, null);

//        only write raf log for heap storage structure
        if (pid instanceof HeapPageId) {
            TransactionId tid = new TransactionId();
            Database.getLogFile().logWrite(tid,page.getBeforeImage(),page);
        }

        // release locks associated with the page
        ConcurrentStatus.releaseAllLocks(pid);
    }

    /**
     * Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        for (PageId pageId : buffer.keySet()) {
            if (buffer.get(pageId) != null && holdsLock(tid, pageId)) {
                // write to log
                Page page = buffer.get(pageId);

                flushPage(pageId);
            }
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1
        if (accessTime.size() == 0) {
            return;
        }

        PageId stalestPageId = Collections.min(accessTime.entrySet(), Comparator.comparingLong(Map.Entry::getValue)).getKey();

        // flush in case of the page is dirty
        Page stalestPage = buffer.get(stalestPageId);
        if (stalestPage.isDirty() != null) {
            try {
                flushPage(stalestPageId);
                discardPage(stalestPageId);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
