package simpledb;

/** TableStats represents statistics (e.g., histograms) about base tables in a query */
public class TableStats {
	
	private class IntStats {
		public int _low;
		public int _high;
	}
    
    /**
     * Number of bins for the histogram.
     * Feel free to increase this value over 100,
     * though our tests assume that you have at least 100 bins in your histograms.
     */
    static final int NUM_HIST_BINS = 100;
    
    private int _tableId;
    private int _costPerPage;
    private HeapFile _tableFile;
    private Object[] _histograms; // One histogram per field in a table
    private IntStats[] _intRange;

    /**
     * Create a new TableStats object, that keeps track of statistics on each column of a table
     * 
     * @param tableid The table over which to compute statistics
     * @param ioCostPerPage The cost per page of IO.  
     * 		                This doesn't differentiate between sequential-scan IO and disk seeks.
     */
    public TableStats (int tableid, int ioCostPerPage) {
        // For this function, you'll have to get the DbFile for the table in question,
    	// then scan through its tuples and calculate the values that you need.
    	// You should try to do this reasonably efficiently, but you don't necessarily
    	// have to (for example) do everything in a single scan of the table.
    	// some code goes here
    	_tableId = tableid;
    	_costPerPage = ioCostPerPage;
    	_tableFile = (HeapFile) Database.getCatalog().getDbFile(tableid);
    	assert (_tableFile != null);
    	
    	generateStatistics();
    }
    
    private void generateStatistics() {
    	TransactionId scanTransaction = new TransactionId();
    	DbFileIterator fileIterator = _tableFile.iterator(scanTransaction);
    	TupleDesc schema = _tableFile.getTupleDesc();
    	
    	try {
    		fileIterator.open();
    	
    		findTupleRanges(schema, scanTransaction, fileIterator);
    		initializeHistograms();
    		fillHistograms(schema, scanTransaction, fileIterator);
    	
    		Database.getBufferPool().transactionComplete(scanTransaction);
    	} catch (Exception e) {
    		System.err.println("Error genrating table stats");
    		e.printStackTrace();
    	}
    }

    private void fillHistograms(TupleDesc schema,
			TransactionId scanTransaction, DbFileIterator fileIterator) 
    	throws TransactionAbortedException, DbException {
    	fileIterator.rewind();
    	
    	while (fileIterator.hasNext()) {
    		Tuple tuple = fileIterator.next();
    		insertIntoHistogram(schema, tuple);
    	}
	}

	private void insertIntoHistogram(TupleDesc schema, Tuple tuple) {
		for (int i = 0; i < schema.numFields(); i++) {
			Field field = tuple.getField(i);
			
			switch (field.getType()) {
			case INT_TYPE:
			{
				int value = ((IntField) field).getValue();
				IntHistogram histogram = (IntHistogram) _histograms[i];
				histogram.addValue(value);
				break;
			}
			case STRING_TYPE:
			{
				String value = ((StringField) field).getValue();
				StringHistogram histogram = (StringHistogram) _histograms[i];
				histogram.addValue(value);
				break;
			}
			default:
				assert (false);
				break;
			}
		}
	}

	private void findTupleRanges(TupleDesc schema,
			TransactionId scanTransaction, DbFileIterator fileIterator) {
    	int columnCount = schema.numFields();
    	initializeIntRanges(columnCount);
    	
    	try {
    		fileIterator.rewind();
    		while (fileIterator.hasNext()) {
    			Tuple tuple = fileIterator.next();
    			findRange(schema, tuple);
    		}
    	
    	} catch (Exception e) {
    		System.err.println("error finding tuple ranges");
    		e.printStackTrace();
    		System.exit(1);
    	}
	}

	private void initializeIntRanges(int columnCount) {
		_intRange = new IntStats[columnCount];
    	for (int i = 0; i < columnCount; i++) {
    		_intRange[i] = new IntStats();
    	}
	}

	private void findRange(TupleDesc schema, Tuple tuple) {
		int fieldCount = schema.numFields();
		for (int i = 0; i < fieldCount; i++) {
			Field column = tuple.getField(i);
			
			switch (column.getType()) {
			case INT_TYPE:
			{
				IntField intField = (IntField) column;
				int value = intField.getValue();
				updateIntRange(i, value);
				break;
			}
			// Strings already have a default min/max range
			// implemented in StringHistogram
			case STRING_TYPE:
			default:
				break;
			}
		}
	}

	private void updateIntRange(int fieldIndex, int value) {
		IntStats columnStats = _intRange[fieldIndex];
		assert (columnStats != null);
		if (value < columnStats._low) {
			columnStats._low = value;
		}
		if (value > columnStats._high) {
			columnStats._high = value;
		}
	}

	private void initializeHistograms() {
    	TupleDesc schema = _tableFile.getTupleDesc();
    	int fieldCount = schema.numFields();
    	_histograms = new Object[fieldCount];
    	
    	for (int i = 0; i < fieldCount; i++) {
    		Type fieldType = schema.getFieldType(i);
    		switch (fieldType) {
			case INT_TYPE:
			{
				IntStats columnStats = _intRange[i];
				assert (columnStats != null);
				int low = columnStats._low;
				int high = columnStats._high;
				_histograms[i] = new IntHistogram(NUM_HIST_BINS, low, high);
				break;
			}
			case STRING_TYPE:
				_histograms[i] = new StringHistogram(NUM_HIST_BINS);
				break;
			default:
				assert (false);
				break;
    		}
    	}
	}

	/** 
     * Estimates the
     * cost of sequentially scanning the file, given that the cost to read
     * a page is costPerPageIO.  You can assume that there are no
     * seeks and that no pages are in the buffer pool.
     * 
     * Also, assume that your hard drive can only read entire pages at once,
     * so if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page.  (Most real hard drives can't efficiently
     * address regions smaller than a page at a time.)
     * 
     * @return The estimated cost of scanning the table.
     */ 
    public double estimateScanCost() {
    	return _tableFile.numPages() * _costPerPage;
    }

    /** 
     * This method returns the number of tuples in the relation,
     * given that a predicate with selectivity selectivityFactor is
     * applied.
	 *
     * @param selectivityFactor The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
    	// some code goes here
    	TupleDesc schema = _tableFile.getTupleDesc();
    	int field = 0;
    	double totalCount = 0;
    	
    	switch (schema.getFieldType(field)) {
		case INT_TYPE:
		{
			IntHistogram histogram = (IntHistogram) _histograms[field];
			totalCount = histogram.valueCount();
			break;
		}
		case STRING_TYPE:
		{
			StringHistogram histogram = (StringHistogram) _histograms[field];
			totalCount = histogram.valueCount();
			break;
		}
    	}
    	
    	return (int) Math.floor(selectivityFactor * totalCount);
    }

    /** 
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the table.
     * 
     * @param field The field over which the predicate ranges
     * @param op The logical operation in the predicate
     * @param constant The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
    	switch (constant.getType()) {
		case INT_TYPE:
		{
			IntHistogram histogram = (IntHistogram) _histograms[field];
			assert (histogram != null);
			int value = ((IntField) constant).getValue();
			return histogram.estimateSelectivity(op, value);
		}
		case STRING_TYPE:
		{
			StringHistogram histogram = (StringHistogram) _histograms[field];
			assert (histogram != null);
			String value = ((StringField) constant).getValue();
			return histogram.estimateSelectivity(op, value);
		}
		default:
			assert (false);
			break;
    	}
    	
    	assert (false);
    	return -1;
    }
} // end class
