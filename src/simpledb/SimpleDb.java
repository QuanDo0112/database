package simpledb;

import java.util.*;
import java.io.*;

import simpledb.TestUtil.SkeletonFile;

public class SimpleDb {
	
    public static final int[][] EXAMPLE_VALUES = new int[][] {
        { 31933, 862 },
        { 29402, 56883 },
        { 1468, 5825 },
        { 17876, 52278 },
        { 6350, 36090 },
        { 34784, 43771 },
        { 28617, 56874 },
        { 19209, 23253 },
        { 56462, 24979 },
        { 51440, 56685 },
        { 3596, 62307 },
        { 45569, 2719 },
        { 22064, 43575 },
        { 42812, 44947 },
        { 22189, 19724 },
        { 33549, 36554 },
        { 9086, 53184 },
        { 42878, 33394 },
        { 62778, 21122 },
        { 17197, 16388 }
    };
    
    public static final byte[] EXAMPLE_DATA;
    static {
        // Build the input table
        ArrayList<ArrayList<Integer>> table = new ArrayList<ArrayList<Integer>>();
        for (int[] tuple : EXAMPLE_VALUES) {
            ArrayList<Integer> listTuple = new ArrayList<Integer>();
            for (int value : tuple) {
                listTuple.add(value);
            }
            table.add(listTuple);
        }

        // Convert it to a HeapFile and read in the bytes
        try {
            File temp = File.createTempFile("table", ".dat");
            temp.deleteOnExit();
            HeapFileEncoder.convert(table, temp, BufferPool.PAGE_SIZE, 2);
            EXAMPLE_DATA = TestUtil.readFileBytes(temp.getAbsolutePath());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
	
	private HeapFile _table;
	
	public SimpleDb() {
		createTable();
	}
	
	private TupleDesc getTupleDescriptor() {
		Type[] types = new Type[] { Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE };
		String[] names = new String[] { "First", "Second", "Third" };
		return new TupleDesc(types, names);
	}
	
	void createTable() {
		_table = new HeapFile(new File("testData.dat"), getTupleDescriptor());
		Database.getCatalog().addTable(_table);
	}	
	
	void markDirty() {
		try {
			 Database.getCatalog().addTable(new SkeletonFile(-1, Utility.getTupleDesc(2)), UUID.randomUUID().toString());
		HeapPageId pid = new HeapPageId(-1, -1);
        TransactionId tid = new TransactionId();
        HeapPage page = new HeapPage(pid, SimpleDb.EXAMPLE_DATA);
        page.markDirty(true, tid);
        TransactionId dirtier = page.isDirty();
        TestUtil.assertEquals(true, dirtier != null);
        TestUtil.assertEquals(true, dirtier == tid);

        page.markDirty(false, tid);
        dirtier = page.isDirty();
        TestUtil.assertEquals(false, dirtier != null);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
    }
	
	public void runTest(TransactionId tid1, PageId pid1, Permissions perm1,
			TransactionId tid2, PageId pid2, Permissions perm2) throws Exception {
		
		BufferPool pool = Database.getBufferPool();
		pool.getPage(tid1,  pid1,  perm1);
		
		TestThread testThread = new TestThread(tid2, pid2, perm2);
		testThread.start();
		Thread.sleep(100);
		System.out.println("Got lock: " + testThread.acquired);
		testThread.stop();
	}
	
	public void acquireRelease(TransactionId tid, PageId pid)
		throws Exception {
		BufferPool pool = Database.getBufferPool();
		pool.getPage(tid,  pid,  Permissions.READ_WRITE);
		pool.releasePage(tid,  pid);
		pool.getPage(tid,  pid,  Permissions.READ_WRITE);
		System.out.println("FINISHED!");
	}
	
	public void otherHeapFile() throws Exception {
		File emptyFile = File.createTempFile("empty", ".dat");
		emptyFile.deleteOnExit();
		Database.reset();
        HeapFile empty = Utility.createEmptyHeapFile(emptyFile.getAbsolutePath(), 2);
        Database.getCatalog().addTable(empty);
        
        TransactionId tid = new TransactionId();
        TransactionId tid2 = new TransactionId();
                                
        // we should be able to add 504 tuples on an empty page.
        for (int i = 0; i < 504; ++i) {
            empty.insertTuple(tid, Utility.getHeapTuple(i, 2));
            TestUtil.assertEquals(1, empty.numPages());
        }
        
        /*
        HeapPageId pid = new HeapPageId(empty.getId(), 0);
        acquireRelease(tid, pid);
        */
        
        
        HeapPageId pid = new HeapPageId(empty.getId(), 0);
        HeapPageId pid2 = new HeapPageId(empty.getId(), 1);
        runTest(tid, pid, Permissions.READ_WRITE,
        		tid2, pid, Permissions.READ_WRITE);
                
	}
	
	void heapFile() throws Exception {
		File emptyFile = File.createTempFile("empty", ".dat");
		emptyFile.deleteOnExit();
         
		Database.reset();
        HeapFile empty = Utility.createEmptyHeapFile(emptyFile.getAbsolutePath(), 2);
        System.out.println("ID is: " + empty.getId());
        Database.getCatalog().addTable(empty);
        
        TransactionId tid = new TransactionId();
        
        // we should be able to add 504 tuples on an empty page.
        for (int i = 0; i < 504; ++i) {
            empty.insertTuple(tid, Utility.getHeapTuple(i, 2));
            TestUtil.assertEquals(1, empty.numPages());
        }

        // the next 512 additions should live on a new page
        for (int i = 0; i < 504; ++i) {
            empty.insertTuple(tid, Utility.getHeapTuple(i, 2));
            TestUtil.assertEquals(2, empty.numPages());
        }

        // and one more, just for fun...
        empty.insertTuple(tid, Utility.getHeapTuple(0, 2));
        TestUtil.assertEquals(3, empty.numPages());
	
	}
	
	void insertTuples() {
		try {
		 HeapPageId pid = new HeapPageId(-1, -1);
		 Database.getCatalog().addTable(new SkeletonFile(-1, Utility.getTupleDesc(2)), UUID.randomUUID().toString());
		 
		 HeapPage page = new HeapPage(pid, SimpleDb.EXAMPLE_DATA);
	        int free = page.getNumEmptySlots();

	        // NOTE(ghuo): this nested loop existence check is slow, but it
	        // shouldn't make a difference for n = 504 slots.

	        for (int i = 0; i < free; ++i) {
	            Tuple addition = Utility.getHeapTuple(i, 2);
	            page.insertTuple(addition);
	            TestUtil.assertEquals(free-i-1, page.getNumEmptySlots());

	            // loop through the iterator to ensure that the tuple actually exists
	            // on the page
	            Iterator<Tuple >it = page.iterator();
	            boolean found = false;
	            while (it.hasNext()) {
	                Tuple tup = it.next();
	                if (TestUtil.compareTuples(addition, tup)) {
	                    found = true;

	                    // verify that the RecordId is sane
	                    TestUtil.assertEquals(page.getId(), tup.getRecordId().getPageId());
	                    break;
	                }
	            }
	            TestUtil.assertTrue(found);
	        }

	        // now, the page should be full.
	        try {
	            page.insertTuple(Utility.getHeapTuple(0, 2));
	            throw new Exception("page should be full; expected DbException");
	        } catch (DbException e) {
	            // explicitly ignored
	        }
		} catch (Exception e) {
			System.out.println("Exception here");
			e.printStackTrace();
			System.out.println(e.getMessage());
			System.exit(1);
		}
	}
	
	public void deadlockTest() {
		CustomDeadlockTest test = new CustomDeadlockTest();
		try {
			test.setUp();
			test.testUpgradeWriteDeadlock();
		} catch (Exception e) {
			e.printStackTrace();
			System.err.println("Got an exception");
		}
	}
	
	public void abortEvictText() {
		AbortEvictionTest test = new AbortEvictionTest();
		try {
			test.testDoNotEvictDirtyPages();
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("Error running abort eviction test");
		}
	}
	
	public void transactionTest() {
		TransactionTest t = new TransactionTest();
		try {
			t.setUp();
			//t.testSingleThread();
			//t.testFiveThreads();
			t.testTenThreads();
			//t.testAllDirtyFails();
			
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("Error running transaction test");
		}
	}
	
	public static void histogramTests() {

		IntHistogram h = new IntHistogram(10, -60, -10);
		h.addValue(-10);

		// All of the values here are negative.
		// Also, there are more of them than there are bins.
		for (int c = -60; c <= -10; c++) {
			h.addValue(c);
			h.estimateSelectivity(Predicate.Op.EQUALS, c);
		}
		double result = h.estimateSelectivity(Predicate.Op.EQUALS, -33);
		System.out.println("Estiatimg selectivity: " + result);
		// Even with just 10 bins and 50 values,
		// the selectivity for this particular value should be at most 0.2.
		TestUtil.assertTrue(h.estimateSelectivity(Predicate.Op.EQUALS, -33) < 0.3);

		// And it really shouldn't be 0.
		// Though, it could easily be as low as 0.02, seeing as that's
		// the fraction of elements that actually are equal to -33.
		TestUtil.assertTrue(h.estimateSelectivity(Predicate.Op.EQUALS, -33) > 0.001);
	}
	
	public static void greaterHistogramTest() {
	    IntHistogram h = new IntHistogram(10, 1, 10);

	    // Set some values
	    h.addValue(3);
	    h.addValue(3);
	    h.addValue(3);
	    h.addValue(1);
	    h.addValue(10);

	    // Be conservative in case of alternate implementations
	    //TestUtil.assertTrue(h.estimateSelectivity(Predicate.Op.LESS_THAN_OR_EQ, -1) < 0.001);
	    //TestUtil.assertTrue(h.estimateSelectivity(Predicate.Op.LESS_THAN_OR_EQ, 2) < 0.4);
	    TestUtil.assertTrue(h.estimateSelectivity(Predicate.Op.LESS_THAN_OR_EQ, 3) > 0.45);
	    
	    //TestUtil.assertTrue(h.estimateSelectivity(Predicate.Op.LESS_THAN_OR_EQ, 4) > 0.6);
	    //TestUtil.assertTrue(h.estimateSelectivity(Predicate.Op.LESS_THAN_OR_EQ, 12) > 0.999);
	}

    public static void customTests() {
    	try {
    		SimpleDb simpledb = new SimpleDb();
    		//simpledb.transactionTest();
    		//simpledb.histogramTests();
    		simpledb.greaterHistogramTest();
    		//simpledb.deadlockTest();
    		//simpledb.abortEvictText();
    	} catch (Exception e) {
    		System.out.println("WTF exception");
    		e.printStackTrace();
    		System.out.println(e);
    		System.exit(1);
    	}
    }
    
    public static void main (String args[])
            throws DbException, TransactionAbortedException, IOException {

        if (args.length == 0) {
            customTests();
            System.out.println("Did custom tests");
            System.exit(1);
        }
        
        // convert a file
        if(args[0].equals("convert")) {
        try {
        if (args.length == 3) {
            HeapFileEncoder.convert(new File(args[1]),
                        new File(args[1].replaceAll(".txt", ".dat")),
                        BufferPool.PAGE_SIZE,
                        Integer.parseInt(args[2]));
        }
        else if (args.length == 4) {
            ArrayList<Type> ts = new ArrayList<Type>();
            String[] typeStringAr = args[3].split(",");
            for (String s: typeStringAr) {
            if (s.toLowerCase().equals("int"))
                ts.add(Type.INT_TYPE);
            else if (s.toLowerCase().equals("string"))
                ts.add(Type.STRING_TYPE);
            else {
                System.out.println("Unknown type " + s);
                return;
            }
            }
            HeapFileEncoder.convert(new File(args[1]),
                        new File(args[1].replaceAll(".txt", ".dat")),
                        BufferPool.PAGE_SIZE,
                        Integer.parseInt(args[2]), ts.toArray(new Type[0]));

        } else {
            System.out.println("Unexpected number of arguments to convert ");
        }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        } else if (args[0].equals("print")) {
            File tableFile = new File(args[1]);
            int columns = Integer.parseInt(args[2]);
            DbFile table = Utility.openHeapFile(columns, tableFile);
            TransactionId tid = new TransactionId();
            DbFileIterator it = table.iterator(tid);
            
            if(null == it){
               System.out.println("Error: method HeapFile.iterator(TransactionId tid) not yet implemented!");
            } else {
               it.open();
               while (it.hasNext()) {
                  Tuple t = it.next();
                  System.out.println(t);
               }
               it.close();
            }
        }
        else if (args[0].equals("parser")) {
            // Strip the first argument and call the parser
            String[] newargs = new String[args.length-1];
            for (int i = 1; i < args.length; ++i) {
                newargs[i-1] = args[i];
            }
            
            try {
                //dynamically load Parser -- if it doesn't exist, print error message
                Class<?> c = Class.forName("simpledb.Parser");
                Class<?> s = String[].class;
                
                java.lang.reflect.Method m = c.getMethod("main", s);
                m.invoke(null, (java.lang.Object)newargs);
            } catch (ClassNotFoundException cne) {
                System.out.println("Class Parser not found -- perhaps you are trying to run the parser as a part of lab1?");
            }
            catch (Exception e) {
                System.out.println("Error in parser.");
                e.printStackTrace();
            }

        }
        else {
            System.err.println("Unknown command: " + args[0]);
            System.exit(1);
        }
    }

}
