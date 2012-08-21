package simpledb;

import java.util.*;

import simpledb.Aggregator.Op;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min).  Note that we only support aggregates over a single column, grouped
 * by a single column.
 */
public class Aggregate extends Operator {
	private DbIterator _child;
	private int _aggregateField;
	private int _groupBy;
	private Aggregator.Op _op;
	private Aggregator _aggregator;
	private DbIterator _results;

    /**
     * Constructor.  
     *
     *  Implementation hint: depending on the type of afield, you will want to construct an 
     *  IntAggregator or StringAggregator to help you with your implementation of readNext().
     * 
     *
     * @param child The DbIterator that is feeding us tuples.
     * @param afield The column over which we are computing an aggregate.
     * @param gfield The column over which we are grouping the result, or -1 if there is no grouping
     * @param aop The aggregation operator to use
     */
    public Aggregate(DbIterator child, int afield, int gfield, Aggregator.Op aop) {
    	assert (child != null);
        _child = child;
        _aggregateField = afield;
        _groupBy = gfield;
        _op = aop;
        
        createAggregator();
    }

    private void createAggregator() {
    	TupleDesc desc = _child.getTupleDesc();
    	assert (desc != null);
    	Type aggregateType = desc.getFieldType(_aggregateField);
    	Type groupType = desc.getFieldType(_groupBy);
    	
    	if (aggregateType == Type.INT_TYPE) {
    		_aggregator = new IntegerAggregator(_groupBy, groupType, _aggregateField, _op);
    	} else {
    		assert (aggregateType == Type.STRING_TYPE);
    		_aggregator = new StringAggregator(_groupBy, groupType, _aggregateField, _op);
    	}
		
	}

	public static String nameOfAggregatorOp(Aggregator.Op aop) {
        switch (aop) {
        case MIN:
            return "min";
        case MAX:
            return "max";
        case AVG:
            return "avg";
        case SUM:
            return "sum";
        case COUNT:
            return "count";
        }
        return "";
    }

    public void open()
        throws NoSuchElementException, DbException, TransactionAbortedException {
        _child.open();
        while (_child.hasNext()) {
        	Tuple t = _child.next();
        	_aggregator.mergeTupleIntoGroup(t);
        }
        
        _results = _aggregator.iterator();
        _results.open();
    }

    /**
     * Returns the next tuple.  If there is a group by field, then 
     * the first field is the field by which we are
     * grouping, and the second field is the result of computing the aggregate,
     * If there is no group by field, then the result tuple should contain
     * one field representing the result of the aggregate.
     * Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
    	if (_results.hasNext()) {
    		return _results.next();
    	}
    	
    	return null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
    	_results.rewind();
    }

    /**
     * Returns the TupleDesc of this Aggregate.
     * If there is no group by field, this will have one field - the aggregate column.
     * If there is a group by field, the first field will be the group by field, and the second
     * will be the aggregate value column.
     * 
     * The name of an aggregate column should be informative.  For example:
     * "aggName(aop) (child_td.getFieldName(afield))"
     * where aop and afield are given in the constructor, and child_td is the TupleDesc
     * of the child iterator. 
     */
    public TupleDesc getTupleDesc() {
    	return _aggregator.iterator().getTupleDesc();
    }

    public void close() {
    	_results.close();
    }
}
