package simpledb;

import java.util.*;

public class HeapPageIterator implements Iterator<Tuple> {
    private HeapPage _page;
    private int _numTuples;
    private int _currentTuple;
        
    // Assumes pages cannot be modified while iterating over them
    // Iterates over only valid tuples
    public HeapPageIterator(HeapPage page) {
        _page = page;
        _currentTuple = 0;
        _numTuples = _page.getNumValidTuples();
    }
        
    public boolean hasNext() {
        return _currentTuple < _numTuples;
    }
        
    public Tuple next() {
        return _page.tuples[_currentTuple++];
    }
        
    public void remove() throws UnsupportedOperationException {
        throw new UnsupportedOperationException("Cannot remove on HeapPageIterator");
    }
}