package simpledb.execution;

import simpledb.common.Type;
import simpledb.execution.aggregator.string.AbstractAggregator;
import simpledb.execution.aggregator.string.CountAggregator;
import simpledb.storage.Field;
import simpledb.storage.IntField;
import simpledb.storage.StringField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.storage.TupleIterator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbfieldIndex;

    private Type gbfieldType;

    private int afield;

    private Op what;

    private AbstractAggregator aggregator;

    /**
     * Aggregate constructor
     *
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // TODO: some code goes here
        this.gbfieldIndex = gbfield;
        this.gbfieldType = gbfieldtype;
        this.afield = afield;
        this.what = what;
        aggregator = new CountAggregator();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // TODO: some code goes here
        Field gbFieldInstance = null;
        if (gbfieldIndex != NO_GROUPING) {
            gbFieldInstance = tup.getField(gbfieldIndex);
        }
        StringField aFieldInstance = (StringField) tup.getField(afield);
        aggregator.mergeTuple(gbFieldInstance, aFieldInstance);
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     * aggregateVal) if using group, or a single (aggregateVal) if no
     * grouping. The aggregateVal is determined by the type of
     * aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // TODO: some code goes here
        Map<Field, Integer> result = aggregator.getResultMap();
        List<Tuple> tuples = new ArrayList<>();
        Type[] typeAr;
        String[] fieldAr;
        TupleDesc tupleDesc;
        if (gbfieldIndex == NO_GROUPING) {
            typeAr = new Type[] {Type.INT_TYPE};
            fieldAr = new String[] {"aggregateVal"};
            tupleDesc = new TupleDesc(typeAr, fieldAr);
            Tuple tup = new Tuple(tupleDesc);
            tup.setField(0, new IntField(result.get(null)));
            tuples.add(tup);
        } else {
            typeAr = new Type[] {gbfieldType, Type.INT_TYPE};
            fieldAr = new String[] {"groupVal", "aggregateVal"};
            tupleDesc = new TupleDesc(typeAr, fieldAr);
            for (Field f : result.keySet()) {
                Tuple tup = new Tuple(tupleDesc);
                if (gbfieldType.equals(Type.INT_TYPE)) {
                    IntField gbField = (IntField)f;
                    tup.setField(0, gbField);
                } else if (gbfieldType.equals(Type.STRING_TYPE)) {
                    StringField gbField = (StringField)f;
                    tup.setField(0, gbField);
                }
                tup.setField(1, new IntField(result.get(f)));
                tuples.add(tup);
            }
        }
        return new TupleIterator(tupleDesc, tuples);
    }

}
