package simpledb.optimizer;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.execution.Predicate;
import simpledb.execution.SeqScan;
import simpledb.storage.*;
import simpledb.transaction.Transaction;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query.
 * <p>
 * This class is not needed in implementing lab1 and lab2.
 */
public class TableStats {

    private static final ConcurrentMap<String, TableStats> statsMap = new ConcurrentHashMap<>();

    static final int IOCOSTPERPAGE = 1000;

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }

    public static void setStatsMap(Map<String, TableStats> s) {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException | IllegalAccessException | IllegalArgumentException | SecurityException e) {
            e.printStackTrace();
        }
    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;

    private int tableid;

    private int ioCostPerPage;

    private Map<Integer, IntHistogram> intHistogramMap;

    private Map<Integer, StringHistogram> stringHistogramMap;

    private int ntups;

    private HeapFile file;

    private TupleDesc tupDesc;

    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     *
     * @param tableid       The table over which to compute statistics
     * @param ioCostPerPage The cost per page of IO. This doesn't differentiate between
     *                      sequential-scan IO and disk seeks.
     */
    public TableStats(int tableid, int ioCostPerPage) {
        // For this function, you'll have to get the
        // DbFile for the table in question,
        // then scan through its tuples and calculate
        // the values that you need.
        // You should try to do this reasonably efficiently, but you don't
        // necessarily have to (for example) do everything
        // in a single scan of the table.
        // TODO: some code goes here
        this.tableid = tableid;
        this.ioCostPerPage = ioCostPerPage;
        file = (HeapFile) Database.getCatalog().getDatabaseFile(tableid);
        tupDesc = file.getTupleDesc();
        intHistogramMap = new ConcurrentHashMap<>();
        stringHistogramMap = new ConcurrentHashMap<>();
        ntups = 0;
        init();
    }

    private void init() {
        TransactionId tid = new TransactionId();
        SeqScan seqScan = new SeqScan(tid, tableid);
        try {
            Map<Integer, Integer> minMap = new HashMap<>();
            Map<Integer, Integer> maxMap = new HashMap<>();
            seqScan.open();
            while (seqScan.hasNext()) {
                ntups++;
                Tuple t = seqScan.next();
                for (int i = 0; i < tupDesc.numFields(); i++) {
                    if (tupDesc.getFieldType(i).equals(Type.INT_TYPE)) {
                        IntField field = (IntField) t.getField(i);
                        int min = minMap.getOrDefault(i, Integer.MAX_VALUE);
                        minMap.put(i, Math.min(min, field.getValue()));
                        int max = maxMap.getOrDefault(i, Integer.MIN_VALUE);
                        maxMap.put(i, Math.max(max, field.getValue()));
                    } else {
                        StringField field = (StringField) t.getField(i);
                        StringHistogram histogram = stringHistogramMap.getOrDefault(i, new StringHistogram(NUM_HIST_BINS));
                        histogram.addValue(field.getValue());
                        stringHistogramMap.put(i, histogram);
                    }
                }
            }
            for (int i = 0; i < tupDesc.numFields(); i++) {
                if (minMap.get(i) != null) {
                    intHistogramMap.put(i, new IntHistogram(NUM_HIST_BINS, minMap.get(i), maxMap.get(i)));
                }
            }
            seqScan.rewind();
            while (seqScan.hasNext()) {
                Tuple t = seqScan.next();
                for (int i = 0; i < tupDesc.numFields(); i++) {
                    if (tupDesc.getFieldType(i).equals(Type.INT_TYPE)) {
                        IntHistogram histogram = intHistogramMap.get(i);
                        if (histogram == null) {
                            throw new IllegalArgumentException();
                        }
                        IntField field = (IntField) t.getField(i);
                        histogram.addValue(field.getValue());
                        intHistogramMap.put(i, histogram);
                    }
                }
            }
        } catch (DbException | TransactionAbortedException e) {
            e.printStackTrace();
        } finally {
            seqScan.close();
        }
    }

    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * <p>
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     *
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        // TODO: some code goes here
        return 2.0 * ioCostPerPage * file.numPages();
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     *
     * @param selectivityFactor The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     * selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        // TODO: some code goes here
        return (int) (ntups * selectivityFactor);
    }

    /**
     * The average selectivity of the field under op.
     *
     * @param field the index of the field
     * @param op    the operator in the predicate
     *              The semantic of the method is that, given the table, and then given a
     *              tuple, of which we do not know the value of the field, return the
     *              expected selectivity. You may estimate this value from the histograms.
     */
    public double avgSelectivity(int field, Predicate.Op op) {
        // TODO: some code goes here
        return 1.0;
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     *
     * @param field    The field over which the predicate ranges
     * @param op       The logical operation in the predicate
     * @param constant The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     * predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        // TODO: some code goes here
        if (tupDesc.getFieldType(field).equals(Type.INT_TYPE)) {
            return intHistogramMap.get(field).estimateSelectivity(op, ((IntField) constant).getValue());
        } else {
            return stringHistogramMap.get(field).estimateSelectivity(op, ((StringField) constant).getValue());
        }
    }

    /**
     * return the total number of tuples in this table
     */
    public int totalTuples() {
        // TODO: some code goes here
        return ntups;
    }

}
