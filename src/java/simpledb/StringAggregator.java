package simpledb;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;

    private HashMap<Field, ArrayList<Integer>> groups;
    private HashMap<Field, Tuple> results;

    private TupleDesc td;
    /**
     * Aggregate constructor
     *
     * @param gbfield     the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield      the 0-based index of the aggregate field in the tuple
     * @param what        aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;

        groups = new HashMap<>();
        results = new HashMap<>();

        Type[] types = new Type[]{gbfieldtype, Type.INT_TYPE};
        td = new TupleDesc(types);
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        // get target key
        Field key = tup.getField(gbfield);

        // get target value
        Integer val = tup.getField(afield).hashCode();

        // append value to key
        if (groups.get(key) == null) {
            groups.put(key, new ArrayList<Integer>());
        }
        groups.get(key).add(val);

        // aggregate
        ArrayList<Integer> integerArrayList = groups.get(key);
        Integer aggregateVal = doAggregate(integerArrayList);

        // construct tuple
        Tuple tuple = new Tuple(td);
        Field f1 = tup.getField(gbfield);
        Field f2 = new IntField(aggregateVal);
        tuple.setField(0, f1);
        tuple.setField(1, f2);

        results.put(key, tuple);
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
        // some code goes here
        return new TupleIterator(td, results.values());
    }

    private Integer doAggregate(ArrayList<Integer> integerArrayList) {
        Integer result = 0;
        switch (what) {
            case MIN:
                break;
            case MAX:
                break;
            case SUM:
                break;
            case AVG:
                break;
            case COUNT:
                result = integerArrayList.size();
                break;
            case SUM_COUNT:
                break;
            case SC_AVG:
                break;
        }
        return result;
    }
}
