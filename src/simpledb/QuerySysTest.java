package simpledb;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Vector;

public class QuerySysTest {
	
	/**
	 * Given a matrix of tuples from SystemTestUtil.createRandomHeapFile, create an identical HeapFile table
	 * @param tuples Tuples to create a HeapFile from
	 * @param columns Each entry in tuples[] must have "columns == tuples.get(i).size()"
	 * @param colPrefix String to prefix to the column names (the columns are named after their column number by default)
	 * @return a new HeapFile containing the specified tuples
	 * @throws IOException if a temporary file can't be created to hand to HeapFile to open and read its data
	 */
	public static HeapFile createDuplicateHeapFile(ArrayList<ArrayList<Integer>> tuples, int columns, String colPrefix) throws IOException {
        File temp = File.createTempFile("table", ".dat");
        temp.deleteOnExit();
        HeapFileEncoder.convert(tuples, temp, BufferPool.PAGE_SIZE, columns);
        return Utility.openHeapFile(columns, colPrefix, temp);
	}
	
	public void queryTest() throws IOException, DbException, TransactionAbortedException {
		// This test is intended to approximate the join described in the
		// "Query Planning" section of 2009 Quiz 1,
		// though with some minor variation due to limitations in simpledb
		// and to only test your integer-heuristic code rather than
		// string-heuristic code.		
		final int IO_COST = 101;
		
		HashMap<String, TableStats> stats = new HashMap<String, TableStats>();
		
		// Create all of the tables, and add them to the catalog
		ArrayList<ArrayList<Integer>> empTuples = new ArrayList<ArrayList<Integer>>();
		HeapFile emp = SystemTestUtil.createRandomHeapFile(6, 100000, null, empTuples, "c");	
		Database.getCatalog().addTable(emp, "emp");
		
		ArrayList<ArrayList<Integer>> deptTuples = new ArrayList<ArrayList<Integer>>();
		HeapFile dept = SystemTestUtil.createRandomHeapFile(3, 1000, null, deptTuples, "c");	
		Database.getCatalog().addTable(dept, "dept");
		
		ArrayList<ArrayList<Integer>> hobbyTuples = new ArrayList<ArrayList<Integer>>();
		HeapFile hobby = SystemTestUtil.createRandomHeapFile(6, 1000, null, hobbyTuples, "c");
		Database.getCatalog().addTable(hobby, "hobby");
		
		ArrayList<ArrayList<Integer>> hobbiesTuples = new ArrayList<ArrayList<Integer>>();
		HeapFile hobbies = SystemTestUtil.createRandomHeapFile(2, 200000, null, hobbiesTuples, "c");
		Database.getCatalog().addTable(hobbies, "hobbies");
		
		// Get TableStats objects for each of the tables that we just generated.
		stats.put("emp", new TableStats(Database.getCatalog().getTableId("emp"), IO_COST));
		stats.put("dept", new TableStats(Database.getCatalog().getTableId("dept"), IO_COST));
		stats.put("hobby", new TableStats(Database.getCatalog().getTableId("hobby"), IO_COST));
		stats.put("hobbies", new TableStats(Database.getCatalog().getTableId("hobbies"), IO_COST));

		Parser.setStatsMap(stats);
		
		Transaction t = new Transaction();
		t.start();
		Parser.setTransaction(t);
		
		// Each of these should return around 20,000
		// This Parser implementation currently just dumps to stdout, so checking that isn't terribly clean.
		// So, don't bother for now; future TODO.
		// Regardless, each of the following should be optimized to run quickly,
		// even though the worst case takes a very long time.
		Parser.processNextStatement("SELECT * FROM emp,dept,hobbies,hobby WHERE emp.c1 = dept.c0 AND hobbies.c0 = emp.c2 AND hobbies.c1 = hobby.c0 AND emp.c3 < 1000;");
	}
}
