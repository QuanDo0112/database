package simpledb;

import java.io.*;
import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 */
public class BufferPool {
    /** Bytes per page, including header. */
    public static final int PAGE_SIZE = 4096;

    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    private int _numPages;
    private HashMap<Integer, Page> _cachedPages;
    private LinkedList<Integer> _recentlyUsed;
    private TransactionLockManager _lockManager;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        _numPages = numPages;
        _cachedPages = new HashMap<Integer, Page>();
        _recentlyUsed = new LinkedList<Integer>();
        _lockManager = new TransactionLockManager();
    }

    private boolean isFull() {
    	assert (_recentlyUsed.size() == _cachedPages.size());
    	return _cachedPages.size() >= _numPages;
    }
    
    private void refreshUse(PageId pid) {
    	int hashCode = pid.hashCode();
    	assert (_recentlyUsed.contains(hashCode));
    	int index = _recentlyUsed.indexOf(hashCode);
    	_recentlyUsed.remove(index);
    	_recentlyUsed.addFirst(hashCode);
    }
    
    synchronized void checkConsistency() {
    	//System.out.println("Recently used: " + _recentlyUsed.size());
    	//System.out.println("Cached pages: " + _cachedPages.size());
    	//assert (_recentlyUsed.size() == _cachedPages.size());
    }
    
    synchronized void checkConsistency(PageId pid) {
    	int hashCode = pid.hashCode();
    	assert (_recentlyUsed.contains(hashCode));
    	assert (_cachedPages.containsKey(hashCode));
    	checkConsistency();
    }
    
        
    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, an page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public synchronized Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
    	int pageHash = pid.hashCode();    	
        if (!_cachedPages.containsKey(pageHash)) {
        	// Holding a lock shouldn't matter! Buffer pool is independent of lock manager
        	if (isFull()) {
            	evictPage();
            	assert (!isFull());
            }

            int tableId = pid.getTableId();
            DbFile file = Database.getCatalog().getDbFile(tableId);
            
            _cachedPages.put(pageHash, file.readPage(pid));
            _recentlyUsed.addFirst(pageHash);
        }

        refreshUse(pid);
        checkConsistency();
        Page page =_cachedPages.get(pageHash);

        _lockManager.getLock(tid,  pid,  perm);
        return page;
    }
    
    
    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public synchronized void releasePage(TransactionId tid, PageId pid) {
    	_lockManager.clearLock(tid, pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public synchronized void transactionComplete(TransactionId tid) throws IOException {
    	boolean commit = true;
    	transactionComplete(tid, commit);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId pid, Permissions permission) {
    	return _lockManager.hasLock(tid, pid, permission);
    }
    
    /***
     * Returns true if the specific page id is 
     * a) in the buffer cache
     * b) Has a write lock
     * c) Is actually dirty and needs to be flushed
     * If we only have a read lock, then no need to flush
     * If it isn't in the buffer cache then no need to flush as well
     * because eviction strategy only evicts non-dirty pages
     * and to get a dirty page, we need a write lock
     * @param pid
     * @return
     */
    private boolean isRecoverable(PageId pid) {
    	int hashcode = pid.hashCode();
    	if (_cachedPages.containsKey(hashcode) && 
    			_lockManager.hasWriteLock(pid)) {
    		Page page = _cachedPages.get(hashcode);
    		TransactionId tid = page.isDirty();
    		return tid != null;
    	}
    	
    	return false;
    }
    
    private synchronized void recoverPage(TransactionId tid, PageId pid) {
    	if (isRecoverable(pid)) {
    		int hashCode = pid.hashCode();
    		Page page = _cachedPages.get(hashCode);
    		_cachedPages.put(hashCode,  page.getBeforeImage());
    		refreshUse(pid);
    		boolean isDirty = false;
    		page.markDirty(isDirty, null);
    		return;
    	}
    	// Otherwise it was a read lock and we don't have to do anything
    	// to recover. Our eviction strategy guarantees we don't evict
    	// dirty pages so we should only recover dirty pages in the buffer pool
    	//System.out.println("Asked to recover page: " + pid.pageNumber());
    	assert (_lockManager.hasLock(tid, pid, Permissions.READ_ONLY));
    }
    
    private synchronized void recoverPages(TransactionId tid) {
    	for (PageId pid : _lockManager.getPagesInTransaction(tid)) {
    		recoverPage(tid, pid);
    	}
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
        throws IOException {
    	if (commit) {
    		flushPages(tid);
    	} else {
    		recoverPages(tid);
    	}
    	
    	_lockManager.clearAllLocks(tid);
    }

    /**
     * Add a tuple to the specified table behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to(Lock 
     * acquisition is not needed for lab2). May block if the lock cannot 
     * be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and updates cached versions of any pages that have 
     * been dirtied so that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
    	DbFile file = Database.getCatalog().getDbFile(tableId);
    	file.insertTuple(tid, t);
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from. May block if
     * the lock cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit.  Does not need to update cached versions of any pages that have 
     * been dirtied, as it is not possible that a new page was created during the deletion
     * (note difference from addTuple).
     *
     * @param tid the transaction adding the tuple.
     * @param t the tuple to add
     */
    public void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, TransactionAbortedException {
    	PageId pid = t.getRecordId().getPageId();
    	DbFile file = Database.getCatalog().getDbFile(pid.getTableId());
    	file.deleteTuple(tid, t);
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
    	for (int pageHash : _cachedPages.keySet()) {
    		Page page = _cachedPages.get(pageHash);
    		flushPage(page.getId(), page.isDirty());
    	}
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
    */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // only necessary for lab5
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private void flushPage(PageId pid, TransactionId tid) throws IOException {
    	DbFile file = Database.getCatalog().getDbFile(pid.getTableId());
    	int pageHash = pid.hashCode();
    	assert (_cachedPages.containsKey(pageHash));
    	Page page = _cachedPages.get(pageHash);
    	
    	if (tid != null) {
    		LogFile log = Database.getLogFile();
    		log.logWrite(tid, page.getBeforeImage(), page);
    		log.force();
    		
    		file.writePage(page);
    		boolean isDirty = false;
    		page.markDirty(isDirty, tid);
    		page.setBeforeImage();
    	}
    }

    /** Write all pages of the specified transaction to disk.
     */
    public void flushPages(TransactionId tid) throws IOException {
    	for (PageId pid : _lockManager.getPagesInTransaction(tid)) {
    		if (isRecoverable(pid)) {
    			flushPage(pid, tid);
    		} 
    	}
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
    	assert (_cachedPages.size() == _recentlyUsed.size());
    	assert (_recentlyUsed.size() <= this._numPages);
    	
    	//System.out.println("Evicting page unmber of pages: " + _numPages);
    	try {
    		for (int i = 0; i < _numPages; i++) {
    			int lastUsed  =_recentlyUsed.removeLast();
    			assert (_cachedPages.containsKey(lastUsed));
    			Page page = _cachedPages.get(lastUsed);
    			assert (page.getId().hashCode() == lastUsed);
    			
    			if (page.isDirty() == null) {
    				flushPage(page.getId(), null);
        			_cachedPages.remove(lastUsed);
        			//System.out.println("Evicting page: " + page.getId().pageNumber());
        			return;
    			} else {
    				_recentlyUsed.addFirst(lastUsed);
    				assert (_cachedPages.size() == _recentlyUsed.size());
    			}
    		}
    		
    		throw new DbException("Cannot evict a page. All in transaction");
    	} catch (IOException e) {
    		System.out.println("Error evicting page");
    		System.out.println(e.getMessage());
    		System.exit(1);
    	}
    }

} // end class
