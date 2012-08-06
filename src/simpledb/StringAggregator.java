package simpledb;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {
	private HashMap<Integer, Integer> _countData;
	private int _groupBy;
	private Op _op;
	private int _aggregateField;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        _groupBy = gbfield;
        _aggregateField = afield;
        _op = what;
        
        assert (what == Op.COUNT);
        
        _countData = new HashMap<Integer, Integer>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        IntField field = (IntField) tup.getField(_groupBy);
        int key = field.getValue();
        
        if (!_countData.containsKey(key)) {
        	_countData.put(key, 0);
        }
        
        // TODO: Check for duplicate strings?
        int currentCount = _countData.get(key);
        currentCount++;
        _countData.put(key, currentCount);
    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public DbIterator iterator() {
    	Type[] types = new Type[2];
    	types[0] = Type.INT_TYPE;
    	types[1] = Type.INT_TYPE;
    	
    	String[] names = new String[2];
    	names[0] = "key";
    	names[0] = _op.toString();
    	TupleDesc description = new TupleDesc(types, names);
    	
    	ArrayList<Tuple> results = new ArrayList<Tuple>();
    	for (int key : _countData.keySet()) {
    		int value = _countData.get(key);
    		Tuple newTuple = new Tuple(description);
    		Field groupBy = new IntField(key);
    		Field aggregate = new IntField(value);
    		newTuple.setField(0, groupBy);
    		newTuple.setField(1, aggregate);
    		results.add(newTuple);
    	}
    	
    	return new TupleIterator(description, results);

    }

}
