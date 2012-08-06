package simpledb;
import java.util.*;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {
	private Predicate _predicate;
	private DbIterator _child;

    /**
     * Constructor accepts a predicate to apply and a child
     * operator to read tuples to filter from.
     *
     * @param p The predicate to filter tuples with
     * @param child The child operator
     */
    public Filter(Predicate p, DbIterator child) {
        _predicate = p;
        _child = child;
    }

    public TupleDesc getTupleDesc() {
        return _child.getTupleDesc();
    }

    public void open()
        throws DbException, NoSuchElementException, TransactionAbortedException {
        _child.open();
    }

    public void close() {
        _child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        _child.rewind();
    }

    /**
     * AbstractDbIterator.readNext implementation.
     * Iterates over tuples from the child operator, applying the predicate
     * to them and returning those that pass the predicate (i.e. for which
     * the Predicate.filter() returns true.)
     *
     * @return The next tuple that passes the filter, or null if there are no more tuples
     * @see Predicate#filter
     */
    protected Tuple fetchNext()
        throws NoSuchElementException, TransactionAbortedException, DbException {
        while (_child.hasNext()) {
        	Tuple tuple = _child.next();
        	if (_predicate.filter(tuple)) return tuple;
        }
        
        return null;
    }
}
