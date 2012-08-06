package simpledb;

import java.util.*;

public class HeapFileIterator implements DbFileIterator {
    private TransactionId _transactionId;
    private HeapFile _file;
    private int _currentPageId;
    private Page _currentPage;
    private int _numPages;
    private Iterator<Tuple> _tupleIterator;

    public HeapFileIterator(TransactionId tid, HeapFile file) {
        _transactionId = tid;
        _file = file;
        _currentPageId = 0;
        _numPages = _file.numPages();
    }

    public void open()
        throws DbException, TransactionAbortedException {
        _currentPage = readPage(_currentPageId++);
        _tupleIterator = _currentPage.iterator();
    }

    public boolean hasNext()
        throws DbException, TransactionAbortedException {
        if (_tupleIterator == null) return false;
        if (_tupleIterator.hasNext()) return true;

        // If we have more pages
        while (_currentPageId <= (_numPages - 1)) {
        	_currentPage = readPage(_currentPageId++);
        	_tupleIterator = _currentPage.iterator();
        	if (_tupleIterator.hasNext()) {
        		return true;
        	}
        } 
        
        return false;
    }

    public Tuple next()
        throws DbException, TransactionAbortedException {
        if (_tupleIterator == null) {
            throw new NoSuchElementException("Tuple iterator not opened");
        }
        
        assert (_tupleIterator.hasNext());
        return _tupleIterator.next();
    }

    public void rewind()
        throws DbException, TransactionAbortedException {
        close();
        open();
    }

    public void close() {
        _currentPageId = 0;
        _tupleIterator = null;
    }

    private Page readPage(int pageNumber) 
    	throws DbException, TransactionAbortedException {
        // File == table because we do one file per table
        int tableId = _file.getId();
        int pageId = pageNumber;
        HeapPageId pid = new HeapPageId(tableId, pageId);
        return Database.getBufferPool().getPage(_transactionId, pid, Permissions.READ_ONLY);
    }
}