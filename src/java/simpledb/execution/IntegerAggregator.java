package simpledb.execution;

import simpledb.common.Type;
import simpledb.execution.aggregator.integer.AbstractAggregator;
import simpledb.execution.aggregator.integer.AvgAggregator;
import simpledb.execution.aggregator.integer.CountAggregator;
import simpledb.execution.aggregator.integer.MaxAggregator;
import simpledb.execution.aggregator.integer.MinAggregator;
import simpledb.execution.aggregator.integer.SumAggregator;
import simpledb.storage.Field;
import simpledb.storage.IntField;
import simpledb.storage.StringField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.storage.TupleIterator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbfield;

    private Type gbfieldtype;

    private int afield;

    private Op what;

    private AbstractAggregator aggregator;

    /**
     * Aggregate constructor
     *
     * @param gbfield     the 0-based index of the group-by field in the tuple, or
     *                    NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null
     *                    if there is no grouping
     * @param afield      the 0-based index of the aggregate field in the tuple
     * @param what        the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // TODO: some code goes here
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        switch (what) {
            case MIN:
                aggregator = new MinAggregator();
                break;
            case MAX:
                aggregator = new MaxAggregator();
                break;
            case SUM:
                aggregator = new SumAggregator();
                break;
            case AVG:
                aggregator = new AvgAggregator();
                break;
            case COUNT:
                aggregator = new CountAggregator();
                break;
            default:
                break;
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // TODO: some code goes here
        IntField aFieldInstance = (IntField) tup.getField(afield);
        Field gbFieldInstance = null;
        if (gbfield != NO_GROUPING) {
            gbFieldInstance = tup.getField(gbfield);
        }
        aggregator.mergeTuple(gbFieldInstance, aFieldInstance);
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        // TODO: some code goes here
        List<Tuple> tuples = new ArrayList<>();
        Map<Field, Integer> resultMap = aggregator.getResultMap();
        Type[] typeAr;
        String[] fieldAr;
        TupleDesc tupDesc;
        if (gbfield == NO_GROUPING) {
            typeAr = new Type[] {Type.INT_TYPE};
            fieldAr = new String[] {"aggregateVal"};
            tupDesc = new TupleDesc(typeAr, fieldAr);
            Tuple tup = new Tuple(tupDesc);
            IntField intField = new IntField(resultMap.get(null));
            tup.setField(0, intField);
            tuples.add(tup);
        } else {
            typeAr = new Type[] {gbfieldtype, Type.INT_TYPE};
            fieldAr = new String[] {"groupVal", "aggregateVal"};
            tupDesc = new TupleDesc(typeAr, fieldAr);
            // You only need to support aggregates over a single field, and grouping by a single field.
            // hence the size of resultMap must be 1
            for (Field f : resultMap.keySet()) {
                Tuple tup = new Tuple(tupDesc);
                if (gbfieldtype.equals(Type.INT_TYPE)) {
                    IntField gbField = (IntField)f;
                    tup.setField(0, gbField);
                } else if (gbfieldtype.equals(Type.STRING_TYPE)) {
                    StringField gbField = (StringField)f;
                    tup.setField(0, gbField);
                }
                IntField intField = new IntField(resultMap.get(f));
                tup.setField(1, intField);
                tuples.add(tup);
            }
        }
        return new TupleIterator(tupDesc, tuples);
    }

}
