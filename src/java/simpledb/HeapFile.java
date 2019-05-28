package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 *
 * @author Sam Madden
 * @see simpledb.HeapPage#HeapPage
 */
public class HeapFile implements DbFile {

    private final File f;
    private final TupleDesc td;

    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f the file that stores the on-disk backing store for this heap
     *          file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.f = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return f;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     *
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return getFile().getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        try {
            RandomAccessFile rf = new RandomAccessFile(f, "r");
            rf.seek(pid.getPageNumber() * BufferPool.getPageSize());
            byte[] buffer = new byte[BufferPool.getPageSize()];
            rf.read(buffer, 0, BufferPool.getPageSize());
            rf.close();
            return new HeapPage((HeapPageId) pid, buffer);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        byte[] data = page.getPageData();
        RandomAccessFile rf = new RandomAccessFile(f, "rw");
        rf.seek(page.getId().getPageNumber() * BufferPool.getPageSize());
        rf.write(data, 0, BufferPool.getPageSize());
        rf.close();
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int) Math.floorDiv(f.length(), BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        ArrayList<Page> pageArrayList = new ArrayList<>();

        HeapPage page = null;

        // Find a page with an empty slot
        int i = 0;
        for (; i < numPages(); i++) {
            HeapPageId pageId = new HeapPageId(getId(),i);
            page = (HeapPage) Database.getBufferPool().getPage(tid,pageId, Permissions.READ_WRITE);
            if (page.getNumEmptySlots() > 0) {
                page.insertTuple(t);
                pageArrayList.add(page);
                break;
            } else {
                page = null;
            }
        }

        if (page == null) {
            // Create a new page and append it to the physical file on dist
            // if no such pages exist in the HeapFile
            // The RecordID in the tuple must be updated correctly
            page = new HeapPage(new HeapPageId(getId(),i),HeapPage.createEmptyPageData());
            page.insertTuple(t);
            pageArrayList.add(page);
            writePage(page);
        }

        return pageArrayList;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        ArrayList<Page> pageArrayList = new ArrayList<>();

        // get page id
        PageId pageId = t.getRecordId().getPageId();
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid,pageId,Permissions.READ_WRITE);
        page.deleteTuple(t);
        pageArrayList.add(page);

        return pageArrayList;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(tid, getId(), numPages(), null);
    }

}

