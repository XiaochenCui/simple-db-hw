package simpledb;

import javax.xml.crypto.Data;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class HeapFileIterator implements DbFileIterator {

    private TransactionId tid;
    private int tableId;
    private int numPages;
    private Permissions perm;

    private boolean isOpen;
    private int pgNo;
    private Iterator<Tuple> tuples;

    public HeapFileIterator(TransactionId tid, int tableId, int numPages, Permissions perm) {
        this.tid = tid;
        this.tableId = tableId;
        this.numPages = numPages;
        this.perm = perm;
    }

    /**
     * Opens the iterator
     *
     * @throws DbException when there are problems opening/accessing the database.
     */
    @Override
    public void open() throws DbException, TransactionAbortedException {
        isOpen = true;
        pgNo = 0;

        tuples = getTuples();
    }

    /**
     * @return true if there are more tuples available, false if no more tuples or iterator isn't open.
     */
    @Override
    public boolean hasNext() throws DbException, TransactionAbortedException {
        if (!isOpen) return false;

        if (tuples.hasNext()) {
            return true;
        } else if (pgNo >= numPages) {
            return false;
        } else {
            tuples = getTuples();
            return tuples.hasNext();
        }
    }

    /**
     * Gets the next tuple from the operator (typically implementing by reading
     * from a child operator or an access method).
     *
     * @return The next tuple in the iterator.
     * @throws NoSuchElementException if there are no more tuples
     */
    @Override
    public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
        if (hasNext()) {
            return tuples.next();
        } else {
            throw new NoSuchElementException();
        }
    }

    /**
     * Resets the iterator to the start.
     *
     * @throws DbException When rewind is unsupported.
     */
    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        close();
        open();
    }

    /**
     * Closes the iterator.
     */
    @Override
    public void close() {
        isOpen = false;
    }

    private Iterator<Tuple> getTuples() throws TransactionAbortedException, DbException {
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(tableId, pgNo), perm);
        pgNo++;
        return page.iterator();
    }
}
