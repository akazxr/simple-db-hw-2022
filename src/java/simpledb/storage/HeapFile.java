package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.index.BTreePageId;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 *
 * @author Sam Madden
 * @see HeapPage#HeapPage
 */
public class HeapFile implements DbFile {

    private final File file;

    private final TupleDesc td;

    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f the file that stores the on-disk backing store for this heap
     *          file.
     */
    public HeapFile(File f, TupleDesc td) {
        this.file = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return this.file;
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
        return file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        int tableId = pid.getTableId();
        int pageNumber = pid.getPageNumber();
        RandomAccessFile f = null;
        try {
            f = new RandomAccessFile(this.file, "r");
            if ((long) (pageNumber + 1) * BufferPool.getPageSize() > f.length()) {
                f.close();
                throw new IllegalArgumentException(
                        String.format("table %d page %d does not exist", tableId, pageNumber));
            }
            byte[] bytes = new byte[BufferPool.getPageSize()];
            f.seek((long) pageNumber * BufferPool.getPageSize());
            int read = f.read(bytes, 0, BufferPool.getPageSize());
            // 读出来length对不上 说明page不存在
            if (read != BufferPool.getPageSize()) {
                throw new IllegalArgumentException(
                        String.format("table %d page %d does not exist", tableId, pageNumber));
            }
            return new HeapPage(new HeapPageId(tableId, pageNumber), bytes);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                f.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        throw new IllegalArgumentException(String.format("table %d page %d is invalid", tableId, pageNumber));
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // TODO: some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        return (int) Math.floor(file.length() * 1.0 / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // TODO: some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public List<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException, TransactionAbortedException {
        // TODO: some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        return new HeapFileIterator(this, tid);
    }

    private static final class HeapFileIterator implements DbFileIterator {

        private final HeapFile heapFile;

        private final TransactionId tid;

        private int pagePos;

        private Iterator<Tuple> tupleIterator;

        public HeapFileIterator(HeapFile heapFile, TransactionId tid) {
            this.heapFile = heapFile;
            this.tid = tid;
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            pagePos = 0;
            tupleIterator = getTupleIterator(pagePos);
        }

        private Iterator<Tuple> getTupleIterator(int pagePos) throws TransactionAbortedException, DbException {
            if (pagePos >= 0 && pagePos <= heapFile.numPages()) {
                PageId pid = new HeapPageId(heapFile.getId(), pagePos);
                HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
                return page.iterator();
            }
            throw new DbException(String.format("page %d is not in heapFile %d", pagePos, heapFile.getId()));
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (tupleIterator == null || pagePos >= heapFile.numPages()) {
                return false;
            }
            if (!tupleIterator.hasNext() && pagePos == heapFile.numPages() - 1) {
                return false;
            }
            return true;
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (tupleIterator == null) {
                throw new NoSuchElementException("file not open");
            }
            if (!tupleIterator.hasNext()) {
                pagePos++;
                HeapPage nextPage = (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(heapFile.getId(), pagePos), Permissions.READ_ONLY);
                tupleIterator = nextPage.iterator();
            }
            return tupleIterator.next();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            close();
            open();
        }

        @Override
        public void close() {
            tupleIterator = null;
        }
    }

}

