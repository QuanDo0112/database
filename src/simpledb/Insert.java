package simpledb;
import java.io.IOException;
import java.util.*;

/**
 * Inserts tuples read from the child operator into
 * the tableid specified in the constructor
 */
public class Insert extends Operator {
	private DbIterator _child;
	private int _tableId;
	private TransactionId _tid;
	private TupleDesc _resultDesc;
	private boolean _didInsert;

    /**
     * Constructor.
     * @param t The transaction running the insert.
     * @param child The child operator from which to read tuples to be inserted.
     * @param tableid The table in which to insert tuples.
     * @throws DbException if TupleDesc of child differs from table into which we are to insert.
     */
    public Insert(TransactionId t, DbIterator child, int tableid)
        throws DbException {
        // some code goes here
    	_tid = t;
    	_child = child;
    	_tableId = tableid;
    	createResultTupleDesc();
    }

    private void createResultTupleDesc() {
    	Type[] types = new Type[1];
    	types[0] = Type.INT_TYPE;
    	
    	String[] names = new String[1];
    	names[0] = "InsertCount";
    	_resultDesc = new TupleDesc(types, names);
    }
    
    public TupleDesc getTupleDesc() {
    	return _resultDesc;
    }

    public void open() throws DbException, TransactionAbortedException {
    	_child.open();
    	_didInsert = false;
    }

    public void close() {
    	_child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
    	_child.rewind();
    }

    /**
     * Inserts tuples read from child into the tableid specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool.
     * An instances of BufferPool is available via Database.getBufferPool().
     * Note that insert DOES NOT need check to see if a particular tuple is
     * a duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
    * null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext()
            throws TransactionAbortedException, DbException {
    	
    	// Can only insert once with this operator. If next is called again
    	// we need to return null
    	if (_didInsert) return null;
    	
    	BufferPool pool = Database.getBufferPool();
    	
    	int count = 0;
    	try {
    		while (_child.hasNext()) {
    			Tuple next = _child.next();
    			count++;
    			pool.insertTuple(_tid, _tableId, next);
    		}
    	} catch (IOException e) {
    		System.out.println("Error inserting tuple");
    		e.printStackTrace();
    		System.out.println(e.getMessage());
    		System.exit(1);
    	}
    	
    	Tuple result = new Tuple(this._resultDesc);
    	IntField intField = new IntField(count);
    	result.setField(0, intField);
    	_didInsert = true;
    	return result;
    }
}
