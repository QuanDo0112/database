package simpledb;

import Zql.*;

import java.io.*;
import java.util.*;

import jline.ArgumentCompletor;
import jline.ConsoleReader;
import jline.SimpleCompletor;

public class Parser {
    static boolean explain = false;
    static HashMap<String, TableStats> statsMap = new HashMap<String,TableStats>();
    
    private static final int IO_COST_PER_PAGE = 1000;
    static Transaction _currentTransaction = null;
    
    public static void setStatsMap(HashMap<String, TableStats> _statsMap) {
    	statsMap = _statsMap;
    }
    
    static Predicate.Op getOp(String s) throws simpledb.ParsingException {
        if (s.equals("=")) return Predicate.Op.EQUALS;
        if (s.equals(">")) return Predicate.Op.GREATER_THAN;
        if (s.equals(">=")) return Predicate.Op.GREATER_THAN_OR_EQ;
        if (s.equals("<")) return Predicate.Op.LESS_THAN;
        if (s.equals("<=")) return Predicate.Op.LESS_THAN_OR_EQ;
        if (s.equals("LIKE")) return Predicate.Op.LIKE;
        if (s.equals("~")) return Predicate.Op.LIKE;
        if (s.equals("<>")) return Predicate.Op.NOT_EQUALS;
        if (s.equals("!=")) return Predicate.Op.NOT_EQUALS;

        throw new simpledb.ParsingException("Unknown predicate " + s);
    }

    static void processExpression(TransactionId tid, ZExpression wx, LogicalPlan lp) throws simpledb.ParsingException {
        if (wx.getOperator().equals("AND")) {
            processAnd(tid, wx, lp);
        } else if (wx.getOperator().equals("OR")) {
            throw new simpledb.ParsingException("OR expressions currently unsupported.");
        } else {
            // this is a binary expression comparing two constants
            @SuppressWarnings("unchecked")
            Vector<ZExp> ops = wx.getOperands();
            processBinaryExpression(tid, wx, lp, ops);
        } // end else
    } // end method

	private static void processBinaryExpression(TransactionId tid,
			ZExpression wx, LogicalPlan lp, Vector<ZExp> ops)
			throws ParsingException {
		if (ops.size() != 2) {
		    throw new simpledb.ParsingException("Only simple binary expresssions of the form A op B are currently supported.");
		}

		boolean isJoin = false;
		Predicate.Op op = getOp(wx.getOperator());

		boolean op1const = ops.elementAt(0) instanceof ZConstant; //otherwise is a Query
		boolean op2const = ops.elementAt(1) instanceof ZConstant; //otherwise is a Query
		if (op1const && op2const) {
		    isJoin  = ((ZConstant)ops.elementAt(0)).getType() == ZConstant.COLUMNNAME &&  ((ZConstant)ops.elementAt(1)).getType() == ZConstant.COLUMNNAME;
		} else if (ops.elementAt(0) instanceof ZQuery || ops.elementAt(1) instanceof ZQuery) {
		    isJoin = true;
		} else if (ops.elementAt(0) instanceof ZExpression || ops.elementAt(1) instanceof ZExpression) {
		    throw new simpledb.ParsingException("Only simple binary expresssions of the form A op B are currently supported, where A or B are fields, constants, or subqueries.");
		} else isJoin = false;

		if (isJoin) {       //join node
		    processJoin(tid, lp, ops, op, op1const, op2const);
		} else { //select node
		    processSelect(lp, ops, op);
		} // end isJoin
	}

	private static void processSelect(LogicalPlan lp, Vector<ZExp> ops,
			Predicate.Op op) throws ParsingException {
		String column;
		String compValue;
		ZConstant op1 = (ZConstant)ops.elementAt(0);
		ZConstant op2 = (ZConstant)ops.elementAt(1);
		if (op1.getType() == ZConstant.COLUMNNAME) {
		    column = op1.getValue();
		    compValue = new String(op2.getValue());
		} else {
		    column = op2.getValue();
		    compValue = new String(op1.getValue());
		}

		lp.addFilter(column, op, compValue);
	}

	private static void processJoin(TransactionId tid, LogicalPlan lp,
			Vector<ZExp> ops, Predicate.Op op, boolean op1const,
			boolean op2const) throws ParsingException {
		String tab1field="", tab2field="";
		if (!op1const)  { //left op is a nested query
		    //generate a virtual table for the left op
		    //this isn't a valid ZQL query
		} else {
		    tab1field = ((ZConstant)ops.elementAt(0)).getValue();
		}

		if (!op2const) { //right op is a nested query
		    try {
		        lp.addJoin(tab1field,parseQuery(tid, (ZQuery)ops.elementAt(1)), op);
		    } catch (IOException e) {
		        throw new simpledb.ParsingException("Invalid subquery " + ops.elementAt(1));
		    } catch (Zql.ParseException e) {
		        throw new simpledb.ParsingException("Invalid subquery " + ops.elementAt(1));
		    }
		} else {
		    tab2field = ((ZConstant)ops.elementAt(1)).getValue();
		    lp.addJoin(tab1field,tab2field,op);
		}
	}

	private static void processAnd(TransactionId tid, ZExpression wx,
			LogicalPlan lp) throws ParsingException {
		for (int i = 0; i < wx.nbOperands(); i++) {
		    if (!(wx.getOperand(i) instanceof ZExpression)) {
		        throw new simpledb.ParsingException("Nested queries are currently unsupported.");
		    }
		    
		    ZExpression newWx = (ZExpression)wx.getOperand(i);
		    processExpression(tid, newWx, lp);
		}
	}

    public static LogicalPlan parseQueryLogicalPlan(TransactionId tid, ZQuery query) throws IOException, Zql.ParseException, simpledb.ParsingException { 
        @SuppressWarnings("unchecked")
        Vector<ZFromItem> from = query.getFrom();
        LogicalPlan logicalPlan = new LogicalPlan();
        logicalPlan.setQuery(query.toString());
        
        getFromTables(from, logicalPlan);
        getWhereClauses(tid, query, logicalPlan);
        String groupByField = getGroupBy(query);

        // walk the select list, pick out aggregates, and check for query validity
        @SuppressWarnings("unchecked")
        Vector<ZSelectItem> selectList = query.getSelect();
        String aggField = null;
        String aggFun = null;

        // TODO: Figure out how to refactor this
        for (int i = 0; i < selectList.size(); i++) {
            ZSelectItem si = selectList.elementAt(i);
            if (si.getAggregate() == null && (si.isExpression() && !(si.getExpression() instanceof ZConstant))) {
                throw new simpledb.ParsingException("Expressions in SELECT list are not supported.");
            }
            if (si.getAggregate() != null) {
            	assert (aggField != null);
                aggField = ((ZConstant)((ZExpression)si.getExpression()).getOperand(0)).getValue();
                aggFun = si.getAggregate();
                System.out.println ("Aggregate field is " + aggField + ", agg fun is : " + aggFun);
                logicalPlan.addProjectField(aggField, aggFun);
            } else {
                if (groupByField != null && ! (groupByField.equals(si.getTable() + "." + si.getColumn()) || groupByField.equals(si.getColumn()))) {
                    throw new simpledb.ParsingException("Non-aggregate field " + si.getColumn() + " does not appear in GROUP BY list.");
                }
                logicalPlan.addProjectField(si.getTable() + "." + si.getColumn(), null);
            }
        }

        if (groupByField != null && aggFun == null) {
            throw new simpledb.ParsingException("GROUP BY without aggregation.");
        }
        
        if (aggFun != null) {
            logicalPlan.addAggregate(aggFun, aggField, groupByField);
        }
        
        sortData(query, logicalPlan);
        return logicalPlan;
    }

	private static void sortData(ZQuery query, LogicalPlan logicalPlan) {
		// sort the data
        if (query.getOrderBy() != null) {
            @SuppressWarnings("unchecked")
            Vector<ZOrderBy> obys = query.getOrderBy();
            ZOrderBy oby = obys.elementAt(0);
            
            assert obys.size() <= 1 : "Multi attribute Order by not supported";
            assert oby.getExpression() instanceof ZConstant : "Complex order by not supported";
            
            ZConstant f = (ZConstant)oby.getExpression();
            logicalPlan.addOrderBy(f.getValue(), oby.getAscOrder());
        }
	}

	private static String getGroupBy(ZQuery query) throws ParsingException {
		ZGroupBy groupBy = query.getGroupBy();
        String groupByField = null;
        if (groupBy != null) {
            @SuppressWarnings("unchecked")
            Vector<ZExp> gbs = groupBy.getGroupBy();
            assert gbs.size() <= 1 : "At most one grouping field supproted";
            
            if (gbs.size() == 1) {
                ZExp gbe = gbs.elementAt(0);
                assert gbe instanceof ZConstant : "Complex group expressions not supported";
                groupByField = ((ZConstant)gbe).getValue();
                System.out.println ("GROUP BY FIELD : " + groupByField);
            }
        }
        
		return groupByField;
	}

	private static void getWhereClauses(TransactionId tid, ZQuery query,
			LogicalPlan logicalPlan) throws ParsingException {
		// now parse the where clause, creating Filter and Join nodes as needed
        ZExp where = query.getWhere();
        if (where != null) {
        	assert where instanceof ZExpression : "Nested Queries are unsupported";
            ZExpression wx = (ZExpression) where;
            processExpression(tid, wx, logicalPlan);
        }
	}

	private static void getFromTables(Vector<ZFromItem> from,
			LogicalPlan logicalPlan) throws ParsingException {
		for (int i = 0; i < from.size(); i++) {
            ZFromItem fromIt = from.elementAt(i);
            try {
                int id = Database.getCatalog().getTableId(fromIt.getTable()); //will fall through if table doesn't exist
                String name = fromIt.getTable();
                if (fromIt.getAlias() != null) {
                    name = fromIt.getAlias();
                }
                    
                logicalPlan.addScan(id, name);
            } catch (NoSuchElementException e) {
            	e.printStackTrace();
                throw new simpledb.ParsingException("Table " + fromIt.getTable() + " is not in catalog");
            }
        }
	}
        
    public static void handleQueryStatement(ZQuery s) throws TransactionAbortedException, DbException, IOException, simpledb.ParsingException, Zql.ParseException  {
        // and run it
        DbIterator node;
        node = parseQuery(_currentTransaction.getId(), s);
        Query query = new Query(node, _currentTransaction.getId());
        TupleDesc td = node.getTupleDesc();

        String names = "";
        for (int i = 0; i < td.numFields(); i ++) {
            names += td.getFieldName(i) + "\t";
        }
        
        int rows = runQuery(query);
        System.out.println("Rows: " + rows);
    }

    public static void handleInsertStatement(ZInsert insertStatement) throws TransactionAbortedException, DbException, IOException, simpledb.ParsingException, Zql.ParseException  {
        int id;
        try {
            id = Database.getCatalog().getTableId(insertStatement.getTable()); //will fall through if table doesn't exist
        } catch (NoSuchElementException e) {
            throw new simpledb.ParsingException ("Unknown table : " + insertStatement.getTable());
        }

        TupleDesc tupleDesc = Database.getCatalog().getTupleDesc(id);
        Tuple t = new Tuple(tupleDesc);
        
        int i = 0;
        DbIterator newTups;

        if (insertStatement.getValues() != null) {
            @SuppressWarnings("unchecked")
            Vector<ZExp> values = (Vector<ZExp>)insertStatement.getValues();
            assert (tupleDesc.numFields() == values.size());
            
            for (ZExp expression : values) {
            	assert expression instanceof ZConstant : "Complex expressions not allowed in INSERT statements";
                fillTuple(tupleDesc, t, i, expression);
                i++;
            }
            
            ArrayList<Tuple> tups = new ArrayList<Tuple>();
            tups.add(t);
            newTups = new TupleArrayIterator(tups);
        } else {
            ZQuery query = (ZQuery)insertStatement.getQuery();
            newTups = parseQuery(_currentTransaction.getId(),query);
        }

        Query insertQuery = new Query(new Insert(_currentTransaction.getId(), newTups, id), _currentTransaction.getId());
        runQuery(insertQuery);
    }

	private static void fillTuple(TupleDesc tupleDesc, Tuple t, int i,
			ZExp expression) throws ParsingException {
		ZConstant zc = (ZConstant)expression;
		
		if (zc.getType() == ZConstant.NUMBER) {
			assert (tupleDesc.getFieldType(i) == Type.INT_TYPE);
		    IntField f= new IntField(new Integer(zc.getValue()));
		    t.setField(i,f);
		} else if(zc.getType() == ZConstant.STRING) {
			assert (tupleDesc.getFieldType(i) == Type.STRING_TYPE);
		    StringField f= new StringField(zc.getValue(), Type.STRING_LEN);
		    t.setField(i,f);
		} else {
		    throw new simpledb.ParsingException("Only string or int fields are supported.");
		}
	}
    
    public static int runQuery(Query query) throws IOException, DbException, TransactionAbortedException {
    	assert (query != null);
    	int count = 0;
    	query.start();
    	while (query.hasNext()) {
    		Tuple tuple = query.next();
    		count++;
    	}
    	
    	query.close();
    	return count;
    }

    public static void handleDeleteStatement(ZDelete deleteStatement) throws TransactionAbortedException, DbException, IOException, simpledb.ParsingException, Zql.ParseException  {
        int id;
        try {
            id = Database.getCatalog().getTableId(deleteStatement.getTable()); //will fall through if table doesn't exist
        } catch (NoSuchElementException e) {
            throw new simpledb.ParsingException ("Unknown table : " + deleteStatement.getTable());
        }
        
        String name = deleteStatement.getTable();
        LogicalPlan logicalPlan = new LogicalPlan();
        logicalPlan.setQuery(deleteStatement.toString());
        logicalPlan.addScan(id, name);
        
        if (deleteStatement.getWhere() != null) {
            processExpression(_currentTransaction.getId(), (ZExpression)deleteStatement.getWhere(), logicalPlan);
        }
        
        logicalPlan.addProjectField("null.*",null);
        
        Query deleteQuery = new Query(new Delete(_currentTransaction.getId(), logicalPlan.physicalPlan(_currentTransaction.getId(), statsMap, false)), _currentTransaction.getId());
        runQuery(deleteQuery);
    }
    
    private static boolean isCommit(ZTransactStmt statement) {
    	return statement.getStmtType().equals("COMMIT");
    }
    
    private static boolean isRollback(ZTransactStmt statement) {
    	return statement.getStmtType().equals("ROLLBACK");
    }

    public static void handleTransactStatement(ZTransactStmt statement) throws TransactionAbortedException, DbException, IOException, simpledb.ParsingException, Zql.ParseException {
        if (isCommit(statement)) {
            _currentTransaction.transactionComplete(false);
            _currentTransaction = null;
            System.out.println("Transaction committed.");
        } else if (isRollback(statement)) {
            _currentTransaction.transactionComplete(true);
            _currentTransaction = null;
            System.out.println("Transaction aborted.");
        } else {
            throw new simpledb.ParsingException("Can't start new transactions until current transaction has been committed or rolledback.");
        }
    }

    public static LogicalPlan generateLogicalPlan(TransactionId tid, String queryString) throws simpledb.ParsingException {
        ByteArrayInputStream bis = new ByteArrayInputStream(queryString.getBytes());
        ZqlParser parser = new ZqlParser(bis);
        
        try {
            ZStatement statement = parser.readStatement();
            assert (statement instanceof ZQuery);
            return parseQueryLogicalPlan(tid, (ZQuery)statement);
        } catch (Zql.ParseException e) {
            throw new simpledb.ParsingException("Invalid SQL expression: \n \t " + e);
        } catch (IOException e) {
            throw new simpledb.ParsingException(e);
        } catch (Exception e) {
	        throw new simpledb.ParsingException("Cannot generate logical plan for expression : " + queryString);
        }
    }

    public static void setTransaction(Transaction t) {
    	_currentTransaction = t;
    }

    public static Transaction getTransaction() {
    	return _currentTransaction;
    }

    public static void processNextStatement(String s) {
    	try {
			processNextStatement(new ByteArrayInputStream(s.getBytes("UTF-8")));
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}    	
    }
    
    public static void processNextStatement(InputStream is) {
        try {
            ZqlParser p = new ZqlParser(is);
            ZStatement s = p.readStatement();

            if (s instanceof ZTransactStmt)
                handleTransactStatement((ZTransactStmt)s);
            else if (s instanceof ZInsert)
                handleInsertStatement((ZInsert)s);
            else if (s instanceof ZDelete)
                handleDeleteStatement((ZDelete)s);
            else if (s instanceof ZQuery)
                handleQueryStatement((ZQuery)s);
            else {
                System.out.println("Can't parse " + s + "\n -- parser only handles SQL transactions, insert, delete, and select statements");
            }
        } catch (simpledb.ParsingException e) {
            System.out.println("Invalid SQL expression: \n \t" + e.getMessage());
            e.printStackTrace();
        } catch (Zql.ParseException e) {
            System.out.println("Invalid SQL expression: \n \t " + e);
            e.printStackTrace();
        } catch (Zql.TokenMgrError e) {
            System.out.println("Invalid SQL expression: \n \t " + e);
            e.printStackTrace();
        } catch (Exception e) {
        	e.printStackTrace();
        }
    }

    // Basic SQL completions
    static final String[] SQL_COMMANDS = {
        "select",
        "from",
        "where",
        "group by",
        "max(",
        "min(",
        "avg(",
        "count",
        "rollback",
        "commit",
        "insert",
        "delete",
        "values",
        "into"
    };
    
    public static DbIterator parseQuery(TransactionId tid, ZQuery q) throws IOException, Zql.ParseException, simpledb.ParsingException {
        return parseQueryLogicalPlan(tid, q).physicalPlan(tid, statsMap, explain);
    }

    public static void main(String argv[]) throws IOException {

        String usage = "Usage: parser catalogFile [-explain] [-f queryFile]";

        if (argv.length < 1 || argv.length > 4) {
            System.out.println("Invalid number of arguments.\n" + usage);
            System.exit(0);
        }

        //first add tables to database
        Database.getCatalog().loadSchema(argv[0]);

        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IO_COST_PER_PAGE);
            statsMap.put(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");

        boolean interactive = true;
        String queryFile = null;

        if (argv.length > 1) {
            for (int i = 1; i<argv.length; i++) {
                if (argv[i].equals("-explain")) {
                    explain = true;
                    System.out.println("Explain mode enabled.");
                } else if (argv[i].equals("-f")) {
                    interactive = false;
                    if (i++ == argv.length) {
                        System.out.println("Expected file name after -f\n" + usage);
                        System.exit(0);
                    }
                    queryFile = argv[i];

                } else {
                    System.out.println("Unknown argument " + argv[i] + "\n " + usage);
                }
            }
        }
        if (!interactive) {
                try {
                    _currentTransaction = new Transaction();
                    _currentTransaction.start();
                    processNextStatement(new FileInputStream(new File(queryFile)));
                } catch (FileNotFoundException e) {
                    System.out.println("Unable to find query file" + queryFile);
                    e.printStackTrace();
                }
        } else { // no query file, run interactive prompt
            ConsoleReader reader = new ConsoleReader();

            // Add really stupid tab completion for simple SQL
            ArgumentCompletor completor = new ArgumentCompletor(new SimpleCompletor(SQL_COMMANDS));
            completor.setStrict(false);  // match at any position
            reader.addCompletor(completor);

            StringBuilder buffer = new StringBuilder();
            String line;
            while ((line = reader.readLine("SimpleDB> ")) != null) {
                // Split statements at ';': handles multiple statements on one line, or one
                // statement spread across many lines
                while (line.indexOf(';') >= 0) {
                    int split = line.indexOf(';');
                    buffer.append(line.substring(0, split+1));
                    byte[] statementBytes = buffer.toString().getBytes("UTF-8");

                    //create a transaction for the query
                    if (_currentTransaction == null) {
                        _currentTransaction = new Transaction();
                        _currentTransaction.start();
                        System.out.println("Started a new transaction tid = " + _currentTransaction.getId().getId());
                    }
                    long startTime = System.currentTimeMillis();
                    processNextStatement(new ByteArrayInputStream(statementBytes));
                    long time = System.currentTimeMillis() - startTime;
                    System.out.printf("----------------\n%.2f seconds\n\n", ((double)time/1000.0));
                    // Grab the remainder of the line
                    line = line.substring(split+1);
                    buffer = new StringBuilder();
                }
                if (line.length() > 0) {
                    buffer.append(line);
                    buffer.append("\n");
                }
            }
        }
    }
}

class TupleArrayIterator implements DbIterator {
    ArrayList<Tuple> tups;
    Iterator<Tuple> it = null;

    public TupleArrayIterator(ArrayList<Tuple> tups) {
        this.tups = tups;
    }

    public void open()
        throws DbException, TransactionAbortedException {
        it = tups.iterator();
    }

    /** @return true if the iterator has more items. */
    public boolean hasNext() throws DbException, TransactionAbortedException {
        return it.hasNext();
    }

    /**
     * Gets the next tuple from the operator (typically implementing by reading
     * from a child operator or an access method).
     *
     * @return The next tuple in the iterator, or null if there are no more tuples.

     */
    public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
        return it.next();
    }

    /**
     * Resets the iterator to the start.
     * @throws DbException When rewind is unsupported.
     */
    public void rewind() throws DbException, TransactionAbortedException {
        it = tups.iterator();
    }

    /**
     * Returns the TupleDesc associated with this DbIterator.
     */
    public TupleDesc getTupleDesc() {
        return tups.get(0).getTupleDesc();
    }

    /**
     * Closes the iterator.
     */
    public void close() {
    }
}
