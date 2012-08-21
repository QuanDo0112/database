package simpledb;
import java.util.*;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements DbIterator {
    private TransactionId _transactionId;
    private int _tableId;
    private String _tableAlias;
    private DbFileIterator _iterator;
    private DbFile _file;

    /**
     * Creates a sequential scan over the specified table as a part of the
     * specified transaction.
     *
     * @param tid The transaction this scan is running as a part of.
     * @param tableid the table to scan.
     * @param tableAlias the alias of this table (needed by the parser);
     *         the returned tupleDesc should have fields with name tableAlias.fieldName
     *         (note: this class is not responsible for handling a case where tableAlias
     *         or fieldName are null.  It shouldn't crash if they are, but the resulting
     *         name can be null.fieldName, tableAlias.null, or null.null).
     */
    public SeqScan(TransactionId tid, int tableid, String tableAlias) {
        _transactionId = tid;
        _tableId = tableid;
        _tableAlias = tableAlias;
        _file = Database.getCatalog().getDbFile(_tableId);
        assert (_file != null);
    }

    public SeqScan(TransactionId tid, int tableid) {
	this(tid, tableid, Database.getCatalog().getTableName(tableid));
    }

    public void open()
        throws DbException, TransactionAbortedException {
         HeapFile heapFile = (HeapFile) _file;
         assert (heapFile != null);
         _iterator = new HeapFileIterator(_transactionId, heapFile);
         _iterator.open();
    }

    /**
     * Returns the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor. This prefix
     * becomes useful when joining tables containing a field(s) with the same
     * name.
     * @return the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor.
     */
    public TupleDesc getTupleDesc() {
    	assert (_file != null);
        TupleDesc fileDesc = _file.getTupleDesc();
        int length = fileDesc.numFields();
        
        Type[] types = new Type[length];
        String[] names = new String[length];
        for (int i = 0; i < length; i++) {
            types[i] = fileDesc.getFieldType(i);
            // Table Alias Requires a table.field name
            names[i] = _tableAlias + "." + fileDesc.getFieldName(i);
        }

        return new TupleDesc(types, names);
    }

    public boolean hasNext() throws TransactionAbortedException, DbException {
        return _iterator.hasNext();
    }

    public Tuple next()
        throws NoSuchElementException, TransactionAbortedException, DbException {
        return _iterator.next();
    }

    public void close() {
         _iterator.close();
    }

    public void rewind()
        throws DbException, NoSuchElementException, TransactionAbortedException {
        _iterator.rewind();
    }
}
