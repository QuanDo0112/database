
package simpledb;

import java.io.*;
import java.util.*;
import java.lang.reflect.*;

/**
LogFile implements the recovery subsystem of SimpleDb.  This class is
able to write different log records as needed, but it is the
responsibility of the caller to ensure that write ahead logging and
two-phase locking discipline are followed.  <p>

<u> Locking note: </u>
<p>

Many of the methods here are synchronized (to prevent concurrent log
writes from happening); many of the methods in BufferPool are also
synchronized (for similar reasons.)  Problem is that BufferPool writes
log records (on page flushed) and the log file flushes BufferPool
pages (on checkpoints and recovery.)  This can lead to deadlock.  For
that reason, any LogFile operation that needs to access the BufferPool
must not be declared synchronized and must begin with a block like:

<p>
<pre>
    synchronized (Database.getBufferPool()) {
       synchronized (this) {

       ..

       }
    }
</pre>
*/

/**
<p> The format of the log file is as follows:

<ul>

<li> The first long integer of the file represents the offset of the
last written checkpoint, or -1 if there are no checkpoints

<li> All additional data in the log consists of log records.  Log
records are variable length.

<li> Each log record begins with an integer type and a long integer
transaction id.

<li> Each log record ends with a long integer file offset representing
the position in the log file where the record began.

<li> There are five record types: ABORT, COMMIT, UPDATE, BEGIN, and
CHECKPOINT

<li> ABORT, COMMIT, and BEGIN records contain no additional data

<li>UPDATE RECORDS consist of two entries, a before image and an
after image.  These images are serialized Page objects, and can be
accessed with the LogFile.readPageData() and LogFile.writePageData()
methods.  See LogFile.print() for an example.

<li> CHECKPOINT records consist of active transactions at the time
the checkpoint was taken and their first log record on disk.  The format
of the record is an integer count of the number of transactions, as well
as a long integer transaction id and a long integer first record offset
for each active transaction.

</ul>

*/

public class LogFile {

    File logFile;
    RandomAccessFile raf;
    Boolean recoveryUndecided; // no call to recover() and no append to log

    static final int ABORT_RECORD = 1;
    static final int COMMIT_RECORD = 2;
    static final int UPDATE_RECORD = 3;
    static final int BEGIN_RECORD = 4;
    static final int CHECKPOINT_RECORD = 5;
    static final int UNDO_CLR_RECORD = 6;
    static final long NO_CHECKPOINT_ID = -1;
    static final long NO_PREV_LSN = -2;

    static int INT_SIZE = 4;
    static int LONG_SIZE = 8;

    long currentOffset = -1;
    int pageSize;
    int totalRecords = 0; // for PatchTest

    HashMap<Long,Long> tidToFirstLogRecord = new HashMap<Long,Long>();
    
    // Recovery fields
    HashMap<Long, ArrayList<Long>> _transactionPrevLSN;
    HashMap<Long, Long> _transactionTable;
    HashMap<Integer, Long> _dirtyPageTable;
    HashMap<Integer, Long> _pageLSN;

    /** Constructor.
        Initialize and back the log file with the specified file.
        We're not sure yet whether the caller is creating a brand new DB,
        in which case we should ignore the log file, or whether the caller
        will eventually want to recover (after populating the Catalog).
        So we make this decision lazily: if someone calls recover(), then
        do it, while if someone starts adding log file entries, then first
        throw out the initial log file contents.

        @param f The log file's name
    */
    public LogFile(File f) throws IOException {
	this.logFile = f;
        raf = new RandomAccessFile(f, "rw");
        recoveryUndecided = true;

        // install shutdown hook to force cleanup on close
        // Runtime.getRuntime().addShutdownHook(new Thread() {
                // public void run() { shutdown(); }
            // });

        //XXX WARNING -- there is nothing that verifies that the specified
        // log file actually corresponds to the current catalog.
        // This could cause problems since we log tableids, which may or
        // may not match tableids in the current catalog.
    }

    // we're about to append a log record. if we weren't sure whether the
    // DB wants to do recovery, we're sure now -- it didn't. So truncate
    // the log.
    void preAppend() throws IOException {
        totalRecords++;
        if(recoveryUndecided){
            recoveryUndecided = false;
            raf.seek(0);
            raf.setLength(0);
            raf.writeLong(NO_CHECKPOINT_ID);
            raf.seek(raf.length());
            currentOffset = raf.getFilePointer();
        }
    }

    public int getTotalRecords() {
        return totalRecords;
    }
    
    /** Write an abort record to the log for the specified tid, force
        the log to disk, and perform a rollback
        @param tid The aborting transaction.
    */
    public void logAbort(TransactionId tid) throws IOException {
        // must have buffer pool lock before proceeding, since this
        // calls rollback

        synchronized (Database.getBufferPool()) {

            synchronized(this) {
                preAppend();
                //Debug.log("ABORT");
                //should we verify that this is a live transaction?

                // must do this here, since rollback only works for
                // live transactions (needs tidToFirstLogRecord)
                rollback(tid);

                raf.writeInt(ABORT_RECORD);
                raf.writeLong(tid.getId());
                raf.writeLong(currentOffset);
                currentOffset = raf.getFilePointer();
                force();
                tidToFirstLogRecord.remove(tid.getId());
            }
        }
    }

    /** Write a commit record to disk for the specified tid,
        and force the log to disk.

        @param tid The committing transaction.
    */
    public synchronized void logCommit(TransactionId tid) throws IOException {
        preAppend();
        //should we verify that this is a live transaction?

        raf.writeInt(COMMIT_RECORD);
        raf.writeLong(tid.getId());
        raf.writeLong(currentOffset);
        currentOffset = raf.getFilePointer();
        force();
        tidToFirstLogRecord.remove(tid.getId());
    }

    /** Write an UPDATE record to disk for the specified tid and page
        (with provided         before and after images.)
        @param tid The transaction performing the write
        @param before The before image of the page
        @param after The after image of the page

        @see simpledb.Page#getBeforeImage
    */
    public  synchronized void logWrite(TransactionId tid, Page before,
                                       Page after)
        throws IOException  {
        Debug.log("WRITE, offset = " + raf.getFilePointer());
        preAppend();
        /* update record conists of

           record type
           transaction id
           before page data (see writePageData)
           after page data
           start offset
        */
        raf.writeInt(UPDATE_RECORD);
        raf.writeLong(tid.getId());

        writePageData(raf,before);
        writePageData(raf,after);
        raf.writeLong(currentOffset);
        currentOffset = raf.getFilePointer();

        Debug.log("WRITE OFFSET = " + currentOffset);
    }

    void writePageData(RandomAccessFile raf, Page p) throws IOException{
        PageId pid = p.getId();
        int pageInfo[] = pid.serialize();

        //page data is:
        // page class name
        // id class name
        // id class bytes
        // id class data
        // page class bytes
        // page class data

        String pageClassName = p.getClass().getName();
        String idClassName = pid.getClass().getName();

        raf.writeUTF(pageClassName);
        raf.writeUTF(idClassName);

        raf.writeInt(pageInfo.length);
        for (int i = 0; i < pageInfo.length; i++) {
            raf.writeInt(pageInfo[i]);
        }
        byte[] pageData = p.getPageData();
        raf.writeInt(pageData.length);
        raf.write(pageData);
        //        Debug.log ("WROTE PAGE DATA, CLASS = " + pageClassName + ", table = " +  pid.getTableId() + ", page = " + pid.pageno());
    }

    Page readPageData(RandomAccessFile raf) throws IOException {
        PageId pid;
        Page newPage = null;

        String pageClassName = raf.readUTF();
        String idClassName = raf.readUTF();

        try {
            Class<?> idClass = Class.forName(idClassName);
            Class<?> pageClass = Class.forName(pageClassName);

            Constructor<?>[] idConsts = idClass.getDeclaredConstructors();
            int numIdArgs = raf.readInt();
            Object idArgs[] = new Object[numIdArgs];
            for (int i = 0; i<numIdArgs;i++) {
                idArgs[i] = new Integer(raf.readInt());
            }
            pid = (PageId)idConsts[0].newInstance(idArgs);

            Constructor<?>[] pageConsts = pageClass.getDeclaredConstructors();
            int pageSize = raf.readInt();

            byte[] pageData = new byte[pageSize];
            raf.read(pageData); //read before image

            Object[] pageArgs = new Object[2];
            pageArgs[0] = pid;
            pageArgs[1] = pageData;

            newPage = (Page)pageConsts[0].newInstance(pageArgs);

            //            Debug.log("READ PAGE OF TYPE " + pageClassName + ", table = " + newPage.getId().getTableId() + ", page = " + newPage.getId().pageno());
        } catch (ClassNotFoundException e){
            e.printStackTrace();
            throw new IOException();
        } catch (InstantiationException e) {
            e.printStackTrace();
            throw new IOException();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
            throw new IOException();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
            throw new IOException();
        }
        return newPage;

    }

    /** Write a BEGIN record for the specified transaction
        @param tid The transaction that is beginning

    */
    public synchronized  void logXactionBegin(TransactionId tid)
        throws IOException {
        Debug.log("BEGIN");
        if(tidToFirstLogRecord.get(tid.getId()) != null){
            System.err.printf("logXactionBegin: already began this tid\n");
            throw new IOException("double logXactionBegin()");
        }
        preAppend();
        raf.writeInt(BEGIN_RECORD);
        raf.writeLong(tid.getId());
        raf.writeLong(currentOffset);
        tidToFirstLogRecord.put(tid.getId(), currentOffset);
        currentOffset = raf.getFilePointer();

        Debug.log("BEGIN OFFSET = " + currentOffset);
    }

    /** Checkpoint the log and write a checkpoint record. */
    public void logCheckpoint() throws IOException {
        //make sure we have buffer pool lock before proceeding
        synchronized (Database.getBufferPool()) {
            synchronized (this) {
                //Debug.log("CHECKPOINT, offset = " + raf.getFilePointer());
                preAppend();
                long startCpOffset, endCpOffset;
                Set<Long> keys = tidToFirstLogRecord.keySet();
                Iterator<Long> els = keys.iterator();
                force();
                Database.getBufferPool().flushAllPages();
                startCpOffset = raf.getFilePointer();
                raf.writeInt(CHECKPOINT_RECORD);
                raf.writeLong(-1); //no tid , but leave space for convenience

                //write list of outstanding transactions
                raf.writeInt(keys.size());
                while (els.hasNext()) {
                    Long key = els.next();
                    Debug.log("WRITING CHECKPOINT TRANSACTION ID: " + key);
                    raf.writeLong(key);
                    Debug.log("WRITING CHECKPOINT TRANSACTION OFFSET: " + tidToFirstLogRecord.get(key));
                    raf.writeLong(tidToFirstLogRecord.get(key));
                }

                //once the CP is written, make sure the CP location at the
                // beginning of the log file is updated
                endCpOffset = raf.getFilePointer();
                raf.seek(0);
                raf.writeLong(startCpOffset);
                raf.seek(endCpOffset);
                raf.writeLong(currentOffset);
                currentOffset = raf.getFilePointer();
                //Debug.log("CP OFFSET = " + currentOffset);
            }
        }

        logTruncate();
    }

    /** Truncate any unneeded portion of the log to reduce its space
        consumption */
    public synchronized void logTruncate() throws IOException {
        preAppend();
        raf.seek(0);
        long cpLoc = raf.readLong();

        long minLogRecord = cpLoc;

        if (cpLoc != -1L) {
            raf.seek(cpLoc);
            int cpType = raf.readInt();
            @SuppressWarnings("unused")
            long cpTid = raf.readLong();

            if (cpType != CHECKPOINT_RECORD) {
                throw new RuntimeException("Checkpoint pointer does not point to checkpoint record");
            }

            int numOutstanding = raf.readInt();

            for (int i = 0; i < numOutstanding; i++) {
                @SuppressWarnings("unused")
                long tid = raf.readLong();
                long firstLogRecord = raf.readLong();
                if (firstLogRecord < minLogRecord) {
                    minLogRecord = firstLogRecord;
                }
            }
        }

        // we can truncate everything before minLogRecord
        File newFile = new File("logtmp" + System.currentTimeMillis());
        RandomAccessFile logNew = new RandomAccessFile(newFile, "rw");
        logNew.seek(0);
        logNew.writeLong((cpLoc - minLogRecord) + LONG_SIZE);

        raf.seek(minLogRecord);

        //have to rewrite log records since offsets are different after truncation
        while (true) {
            try {
                int type = raf.readInt();
                long record_tid = raf.readLong();
                long newStart = logNew.getFilePointer();

                Debug.log("NEW START = " + newStart);

                logNew.writeInt(type);
                logNew.writeLong(record_tid);

                switch (type) {
                case UPDATE_RECORD:
                    Page before = readPageData(raf);
                    Page after = readPageData(raf);

                    writePageData(logNew, before);
                    writePageData(logNew, after);
                    break;
                case CHECKPOINT_RECORD:
                    int numXactions = raf.readInt();
                    logNew.writeInt(numXactions);
                    while (numXactions-- > 0) {
                        long xid = raf.readLong();
                        long xoffset = raf.readLong();
                        logNew.writeLong(xid);
                        logNew.writeLong((xoffset - minLogRecord) + LONG_SIZE);
                    }
                    break;
                case BEGIN_RECORD:
                    tidToFirstLogRecord.put(record_tid,newStart);
                    break;
                }

                //all xactions finish with a pointer
                logNew.writeLong(newStart);
                raf.readLong();

            } catch (EOFException e) {
                break;
            }
        }

        Debug.log("TRUNCATING LOG;  WAS " + raf.length() + " BYTES ; NEW START : " + minLogRecord + " NEW LENGTH: " + (raf.length() - minLogRecord));

        raf.close();
        logFile.delete();
        newFile.renameTo(logFile);
        raf = new RandomAccessFile(logFile, "rw");
        raf.seek(raf.length());
        newFile.delete();

        currentOffset = raf.getFilePointer();
        //print();
    }
    
    private boolean isEOF() {
    	try {
    		return raf.getFilePointer() == raf.length();
    	} catch (IOException e) {
    		System.err.println("Could not detect EOF: " + e);
    		return false;
    	}
    }
    
    private void parseRollback(long transactionId) 
    	throws NoSuchElementException, IOException {
    	
    	// Start at the beginning of the transaction until the end of the file
    	while (!isEOF()) {
    		long startingOffset = raf.getFilePointer();
    		int recordType = raf.readInt();
    		long logTransactionId = raf.readLong();
    		long endTransactionOffset = 0;
    		
    		switch (recordType) {
    		case ABORT_RECORD:
    		case BEGIN_RECORD:
    		case COMMIT_RECORD:
    			break;
    		case CHECKPOINT_RECORD:
    		{
    			int transactionCount = raf.readInt();
    			assert (logTransactionId == NO_CHECKPOINT_ID);
    			parseCheckpointRecords(transactionCount);
    			break;
    		}
    		case UPDATE_RECORD:
    		{
    			updateRollback(transactionId, logTransactionId);
    			break;
    		}
    		default:
    			System.out.println("Unknown key: " + recordType);
    			assert (false);
			}
    		
    		
    		endTransactionOffset = raf.readLong();
    		consistencyCheckRollback(startingOffset, transactionId, logTransactionId,
					endTransactionOffset);
    	}
    }

    /*
     * CHECKPOINT records consist of active transactions at the time
	    the checkpoint was taken and their first log record on disk.  The format
	    of the record is an integer count of the number of transactions, as well
	    as a long integer transaction id and a long integer first record offset
	    for each active transaction.
	 */
	private void parseCheckpointRecords(int transactionCount) {
		try {
			for (int i = 0; i < transactionCount; i++) {
				long transactionId = raf.readLong();
				long offset = raf.readLong();
				// Can't actually assert transactionId and offset
				// Are in the idToFirstLogRecord
				// Becauase a commit clears the transaction id from the map
				//System.out.println("transaction id: " + transactionId + " offset: " + offset);
			}
		} catch (IOException e) {
			System.err.println("Error reading long records in parseCheckpointRecords " + e);
			e.printStackTrace();
		}
	}

	private void consistencyCheckRollback(long startingOffset,
			long transactionId, long logTransactionId, long endTransactionOffset) {
		
		assert (endTransactionOffset != 0);
		if (transactionId == logTransactionId) {
			assert (endTransactionOffset == startingOffset);
		}
	}

    private void updateRollback(long transactionId, long logTransactionId) {
    	try {
	    	Page beforePage = this.readPageData(raf);
	    	Page afterPage = this.readPageData(raf); // Don't use but have to move file descriptor pointer forward
	    	PageId pid = beforePage.getId();
	    	
	    	if (transactionId == logTransactionId) {
		    	DbFile file = Database.getCatalog().getDbFile(pid.getTableId());
		    	file.writePage(beforePage);
		    	BufferPool pool = Database.getBufferPool();
		    	pool.evictPage(beforePage);
	    	}
    	} catch (IOException e) {
    		System.err.println("Error reading page data during parse update: " + e);
    	}
	}

	/** Rollback the specified transaction, setting the state of any
        of pages it updated to their pre-updated state.  To preserve
        transaction semantics, this should not be called on
        transactions that have already committed (though this may not
        be enforced by this method.)

        @param tid The transaction to rollback
    */
    public void rollback(TransactionId tid)
        throws NoSuchElementException, IOException {
        synchronized (Database.getBufferPool()) {
            synchronized(this) {
                preAppend();
                // some code goes here
                long startingOffset = tidToFirstLogRecord.get(tid.getId());
                raf.seek(startingOffset);
                parseRollback(tid.getId());
            }
        }
    }

    /** Shutdown the logging system, writing out whatever state
        is necessary so that start up can happen quickly (without
        extensive recovery.)
    */
    public synchronized void shutdown() {
        try {
            logCheckpoint();  //simple way to shutdown is to write a checkpoint record
            raf.close();
        } catch (IOException e) {
            System.out.println("ERROR SHUTTING DOWN -- IGNORING.");
            e.printStackTrace();
        }
    }

    /** Recover the database system by ensuring that the updates of
        committed transactions are installed and that the
        updates of uncommitted transactions are not installed.
    */
    public void recover() throws IOException {
        synchronized (Database.getBufferPool()) {
            synchronized (this) {
                recoveryUndecided = false;
                initializeTransactionDataStructures();
                long checkpoint = getLastCheckpoint();
                recoverDatabaseFromCheckpoint(checkpoint);
                Database.getBufferPool().clean();
            }
         }
    }
    
	private void initializeTransactionDataStructures() {
		_transactionPrevLSN = new HashMap<Long, ArrayList<Long>>();
		_transactionTable =  new HashMap<Long,Long>();
		_dirtyPageTable = new HashMap<Integer, Long>();
		_pageLSN = new HashMap<Integer, Long>();
	}
	
	private ArrayList<Long> getPrevLSN(long transactionId) {
		if (!_transactionPrevLSN.containsKey(transactionId)) {
			_transactionPrevLSN.put(transactionId, new ArrayList<Long>());
		}
		
		return _transactionPrevLSN.get(transactionId);
	}
	
	// Prev LSNs must be in ascending order
	private void addPrevLSN(long transactionId, long LSN) {
		ArrayList<Long> lsns = getPrevLSN(transactionId);
		if (lsns.size() > 0) {
			long prevLSN = lsns.get(lsns.size() - 1);
			assert (prevLSN < LSN);
		}
		
		lsns.add(LSN);
	}
	
	private void clearPrevLSN(long transactionId) {
		assert (_transactionPrevLSN.containsKey(transactionId));
		_transactionPrevLSN.remove(transactionId);
	}
	
	private long getLastCheckpoint() throws IOException {
    	raf.seek(0);
    	long lastCheckpoint = raf.readLong();
    	if (lastCheckpoint == NO_CHECKPOINT_ID) return raf.getFilePointer();
    	return lastCheckpoint;
    }

	/***
	 * According to ARIES paper, log files should have a chain of
	 * previousLSN in each LOG RECORD. Previous LSN is a list
	 * of LSNs for a given transaction. The log file in this DB
	 * does not have prevLSN, so do one more scan from the beginning to create previousLSN.
	 * 
	 * In addition, checkpoint's aren't ARIES checkpoints. ARIES
	 * checkpoints should have live transactions (which we have) AND
	 * the dirty page table (which we don't). We could technically find out
	 * by asking the lock manager, but that seems ugly and still incorrect.
	 * 
	 * Finally, since we don't have the dirty page table, we have to restart REDO
	 * from first LSN in a live transaction. 
	 * 
	 * LSN is implicit here, LSN is actually log offset
	 * 
	 * TODO: Modify log from supplied log format to ARIES format
	 * @throws IOException
	 */
	private void recoverDatabaseFromCheckpoint(long checkpoint) 
		throws IOException {
		raf.seek(checkpoint);
		assert (tidToFirstLogRecord.isEmpty());
		//System.out.println("Starting Analysis phase at LSN : " + checkpoint);
		
		// Perform ARIES logging protocol with slight modification
		analysisPhase();
		//printRecoverData();
		redoPhase();
		undoPhase();
		//System.out.println("Finished recovering");
  	}

	private void analysisPhase() throws IOException {
	  	while (!isEOF()) {
	  		long lsn = raf.getFilePointer();
    		int recordType = raf.readInt();
    		long logTransactionId = raf.readLong();
    		//System.out.println("Analyzing transaction : " + logTransactionId + " with record type: " + recordType);
    		    		
    		switch (recordType) {
    		case COMMIT_RECORD:
    		{
    			if (_transactionTable.containsKey(lsn)) {
	    			_transactionTable.remove(lsn);
    			}
    		}
    		case ABORT_RECORD: // Means we have to abort 
    		{
    			break;
    		}
    		case BEGIN_RECORD:
    		{
    			//System.out.println("Putting in lsn to transaction id: " + lsn + " = " + logTransactionId);
    			tidToFirstLogRecord.put(logTransactionId, lsn);
    			_transactionTable.put(lsn, logTransactionId);
    			break;
    		}
    		case CHECKPOINT_RECORD:
    		{
    			int transactionCount = raf.readInt();
    			analyzeCheckpoint(transactionCount);
    			break;
    		}
    		case UPDATE_RECORD:
    		{
    			analysisUpdatePhase(logTransactionId, lsn);
    			break;
    		}
    		default:
    			System.out.println("Unknown key: " + recordType);
    			assert (false);
			}
    		
    		long endLSN = raf.readLong(); // read end transaction id
    		assert (lsn == endLSN);
    	}
	}
	
	public void printRecoverData() {
		System.out.println("=== ARIES Recovery Data ====\n");
		StringBuffer buffer = new StringBuffer();
		
		for (long lsn : _transactionTable.keySet()) {
			buffer.append(lsn);
			buffer.append(" = ");
			buffer.append(_transactionTable.get(lsn) + "\n");
		}
		
		System.out.println("Transactions table: \n" + buffer.toString());
		
		StringBuffer dirtyPages = new StringBuffer();
		for (int pageNumber : _dirtyPageTable.keySet()) {
			dirtyPages.append(pageNumber);
			long lsn = _dirtyPageTable.get(pageNumber);
			dirtyPages.append(" = " + lsn);
			dirtyPages.append("\n");
		}
		
		System.out.println("Dirty page table: \n" + dirtyPages.toString());
		System.out.println("\n=== End ARIES Recovery Data ===");
	}
	
	private void analyzeCheckpoint(int transactionCount) throws IOException {
		for (int i = 0; i < transactionCount; i++) {
			long transactionId = raf.readLong();
			long lsn = raf.readLong();
			//System.out.println("Analyzing checkpoint transaction id: " + transactionId);
			_transactionTable.put(lsn, transactionId);
		}
	}

	private void analysisUpdatePhase(long logTransactionId, long lsn) throws IOException {
		Page oldPage = this.readPageData(raf);
		Page newPage = this.readPageData(raf); 
		
		assert (oldPage.getId().equals(newPage.getId()));
		assert (lsn != 0);
		
		PageId pid = oldPage.getId();
		int pageNumber = pid.pageNumber();
		_dirtyPageTable.put(pageNumber, lsn);
	}
	
	private void redoPhase() throws IOException {
		long start = redoGetMinLSN();
		raf.seek(start);
		//System.out.println("Starting Redo phase at LSN: " + start);
		
	  	while (!isEOF()) {
	  		long lsn = raf.getFilePointer();
    		int recordType = raf.readInt();
    		long logTransactionId = raf.readLong();
    		addPrevLSN(logTransactionId, lsn);
    		
    		switch (recordType) {
    		case ABORT_RECORD:
    		{
    			redoRollback(logTransactionId);
                removeFromTransactionTable(logTransactionId);
    			break;
    		}
    		case BEGIN_RECORD:
    		{
    			tidToFirstLogRecord.put(logTransactionId, lsn);
    			break;
    		}
    		case COMMIT_RECORD:
    		{
    			//System.out.println("Log transaction: " + logTransactionId + " commited");
    			removeFromTransactionTable(logTransactionId);
    			break;
    		}
    		case CHECKPOINT_RECORD:
    		{
    			int transactionCount = raf.readInt();
    			parseCheckpointRecords(transactionCount);
    			break;
    		}
    		case UPDATE_RECORD:
    		{
    			redoUpdate(logTransactionId, lsn);
    			break;
    		}
    		default:
    			System.out.println("Unknown key: " + recordType);
    			assert (false);
			}
    		
    		long endLSN = raf.readLong(); // read end transaction id
    		assert (lsn == endLSN);
    	}
	}

	private void redoRollback(long logTransactionId) throws IOException {
		long currentOffset = raf.getFilePointer();
		assert (tidToFirstLogRecord.containsKey(logTransactionId));
		long startingOffset = tidToFirstLogRecord.get(logTransactionId);
		raf.seek(startingOffset);
		parseRollback(logTransactionId);
		raf.seek(currentOffset);
	}
	
	private void removeFromTransactionTable(long logTransactionId) {
		for (long prevLSN : getPrevLSN(logTransactionId)) {
			if (_transactionTable.containsKey(prevLSN)) {
				_transactionTable.remove(prevLSN);
			}
		}
	}

	private void redoUpdate(long logTransactionId, long lsn) throws IOException {
		Page oldPage = readPageData(raf);
		Page newPage = readPageData(raf);
		commitToDisk(newPage);
		
		PageId pid = oldPage.getId();
		_pageLSN.put(pid.pageNumber(), lsn);
	}

	/***
	 * Since our checkpoint doesn't record dirty page tables
	 * Start at min offset of live transactions. Read Analysis Phase
	 * @return
	 */
	private long redoGetMinLSN() {
		long min = Long.MAX_VALUE;
		for (long lsn : _transactionTable.keySet()) {
			if (lsn < min) {
				min = lsn;
			}
		}
		
		assert (min != Long.MAX_VALUE);
		return min;
	}
	
	private void undoPhase() {
		for (long lsn : _transactionTable.keySet()) {
			long loserTransaction = _transactionTable.get(lsn);
			//System.out.println("Undoing transaction: " + loserTransaction);
			
			long prevLSN = getNextPrevLSN(loserTransaction);
			while (prevLSN != NO_PREV_LSN) {
				//printLSNs(loserTransaction);
				undoTransaction(loserTransaction, prevLSN);
				prevLSN = getNextPrevLSN(loserTransaction);
			}
		}
	}
	
	private void printLSNs(long transactionId) {
		ArrayList<Long> previousLSN = getPrevLSN(transactionId);
		if (previousLSN.isEmpty()) return;
		
		System.out.println("Previous LSNs for transaction: " + transactionId);
		long prevLSN = -1;
		for (long lsn : previousLSN) {
			assert (lsn > prevLSN);
			System.out.println("Previous LSN: " + lsn);
		}
		
		//System.out.println("Finished previous LNS\n");
	}

	private void undoTransaction(long loserTransaction, long lsn) {
		try {
			assert (lsn != NO_CHECKPOINT_ID);
			raf.seek(lsn);
			int recordType = raf.readInt();
			long transactionId = raf.readLong();
			assert (loserTransaction == transactionId);
			//System.out.println("Undoing transaction : " + transactionId + " at lsn: " + lsn);
			
			switch (recordType) {
			case BEGIN_RECORD:
			case ABORT_RECORD:
			case COMMIT_RECORD:
				break;
			case CHECKPOINT_RECORD:
			{
				int recordCount = raf.readInt();
				parseCheckpointRecords(recordCount);
				break;
			}
			case UPDATE_RECORD:
			{
				undoUpdate(loserTransaction, lsn);
				break;
			}
			default:
				System.err.println("Got unknown recordType: " + recordType);
				assert (false);
			}
			
			long endRecord = raf.readLong();
			assert (endRecord == lsn);
		} catch (IOException e) {
			System.err.println("Error undoing transaction: " + e);
			e.printStackTrace();
		}
		
		addCLRRecord(loserTransaction, lsn);
	}

	private void undoUpdate(long loserTransaction, long lsn) {
		try {
			Page oldPage = readPageData(raf);
			Page newPage = readPageData(raf);
			commitToDisk(oldPage);
			// TODO Auto-generated method stub
		} catch (IOException e) {
			System.err.println("Error doing undo update " + e);
		}
	}
	
	private DbFile getDbFile(PageId pid) {
		DbFile file = Database.getCatalog().getDbFile(pid.getTableId());
		assert (file != null);
		return file;
	}

	private void commitToDisk(Page page) throws IOException {
		DbFile file = getDbFile(page.getId());
		file.writePage(page);
	}

	private long getNextPrevLSN(long loserTransaction) {
		ArrayList<Long> prevLSNs = getPrevLSN(loserTransaction);
		if (prevLSNs.isEmpty()) return NO_PREV_LSN;
		int size = prevLSNs.size();
		int lastIndex = size - 1;
		long prevLSN = prevLSNs.get(lastIndex);
		prevLSNs.remove(lastIndex);
		return prevLSN;
	}

	private void addCLRRecord(long loserTransaction, long prevLSN) {
		try { 
			raf.seek(raf.length());
			preAppend();
			
			raf.writeInt(UNDO_CLR_RECORD);
            raf.writeLong(loserTransaction);
            raf.writeLong(prevLSN);
            raf.writeLong(currentOffset);
            currentOffset = raf.getFilePointer();
            force();
		} catch (IOException e) {
			System.err.println("Error adding CLR record: " + e);
		}
	}

	/** Print out a human readable represenation of the log */
    public void print() throws IOException {
        // some code goes here
    }

    public  synchronized void force() throws IOException {
        raf.getChannel().force(true);
    }

}
