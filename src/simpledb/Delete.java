package simpledb;

/**
 * The delete operator.  Delete reads tuples from its child operator and
 * removes them from the table they belong to.
 */
public class Delete extends Operator {
	private TransactionId _tid;
	private DbIterator _child;
	private TupleDesc _resultTupleDesc;
	private boolean _didDelete;
	
    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * @param t The transaction this delete runs in
     * @param child The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, DbIterator child) {
    	_tid = t;
    	_child = child;
    	createResultTupleDesc();
    	_didDelete = false;
    }
    
    private void createResultTupleDesc() {
    	Type[] types = new Type[1];
    	types[0] = Type.INT_TYPE;
    	String[] names = new String[1];
    	names[0] = "DeleteCount";
    	
    	_resultTupleDesc = new TupleDesc(types, names);
    }

    public TupleDesc getTupleDesc() {
    	return _resultTupleDesc;
    }

    public void open() throws DbException, TransactionAbortedException {
    	_child.open();
    	_didDelete = false;
    }

    public void close() {
    	_child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
    	_child.close();
    }
    
    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
    	// The first time delete is called, we actually delete the rows
    	// If the delete operator is called a second time, we have to return a null
    	if (_didDelete) return null;
    	
    	Tuple result = new Tuple(getTupleDesc());
    	BufferPool pool = Database.getBufferPool();
    	int count = 0;
    	
    	while (_child.hasNext()) {
    		Tuple next = _child.next();
    		pool.deleteTuple(_tid, next);
    		count++;
    	}

    	IntField resultField = new IntField(count);
    	result.setField(0, resultField);
    	_didDelete = true;
    	return result;
    }
}
