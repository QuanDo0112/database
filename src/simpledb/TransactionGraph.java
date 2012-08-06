package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

/***
 * Represents a graph. Each transaction Graph contains
 * a transaction object with links to the pages it uses.
 * Provides helper methods to detect conflicts etc
 * Currently linked to a HeapFile and HeapPageId
 * @author masonchang
 *
 */

class PageNode {
	PageNode(PageId pid) {
		assert (pid != null);
		assert (pid instanceof HeapPageId);
		
		HeapPageId heapPage = (HeapPageId) pid;
		_pid = pid;
		_pageNumber = heapPage.pageNumber();
		_tableId = heapPage.getTableId();
		_isWritable = false;
	}
	
	public PageId toPageId() {
		return new HeapPageId(_tableId, _pageNumber);
	}
	
	public void setWritable() {
		_isWritable = true;
	}
	
	public boolean isWritable() {
		return _isWritable;
	}
	
	public int getHash() {
		return _pid.hashCode();
	}
	
	private PageId _pid;
	private int _pageNumber;
	private int _tableId;
	private boolean _isWritable;
}

class TransactionNode {
	TransactionId _tid;
	TransactionNode(TransactionId tid) {
		_tid = tid;
	}
	
	TransactionId toId() {
		return _tid;
	}
}

/***
 * A Transaction graph is actually a bipartite graph.
 * One set is transactions
 * The other is the pages in use
 * If a page has a write lock, there should only be one edge between a 
 * transaction and a page. 
 * Read locks can have multiple transaction <--> page links
 * Each node has a read/write marker
 * @author masonchang
 *
 */
public class TransactionGraph {
	private HashMap<Long, TransactionNode> _transactions;
	private HashMap<Integer, PageNode> _pages;
	
	private HashMap<TransactionNode, HashSet<PageNode>> _tidToPages;
	private HashMap<PageNode, HashSet<TransactionNode>> _pageToTid;
	
	public TransactionGraph() {
		_transactions = new HashMap<Long, TransactionNode>();
		_pages = new HashMap<Integer, PageNode>();
		
		_tidToPages = new HashMap<TransactionNode, HashSet<PageNode>>();
		_pageToTid = new HashMap<PageNode, HashSet<TransactionNode>>();
	}
	
	/***
	 * Need the transaction / page wrappers because
	 * we can have multiple different VM objects that represent the same
	 * Transaction and page.
	 * @param tid
	 * @return
	 */
	private TransactionNode getTransactionNode(TransactionId tid) {
		long id = 0;
		if (tid != null) id = tid.getId();
		
		if (!_transactions.containsKey(id)) {
			addTransactionNode(tid);
		}
		return _transactions.get(id);
	}
	
	private PageNode getPageNode(PageId pid) {
		int id = pid.hashCode();
		if (!_pages.containsKey(id)) {
			addPageNode(pid);
		}
		return _pages.get(id);
	}
	
	private void addTransactionNode(TransactionId tid) {
		long id = 0;
		if (tid != null) id = tid.getId();
		
		assert (!_transactions.containsKey(id));
		_transactions.put(id,new TransactionNode(tid));
	}
	
	private void addPageNode(PageId pid) {
		int hash = pid.hashCode();
		assert (!_pages.containsKey(hash));
		_pages.put(hash, new PageNode(pid));
	}
	
	private HashSet<PageNode> getTransactionToPageEdges(TransactionNode node) {
		if (!_tidToPages.containsKey(node)) {
			_tidToPages.put(node, new HashSet<PageNode>());
		}
		
		return _tidToPages.get(node);
	}
	
	private HashSet<TransactionNode> getPageToTransactionEdges(PageNode node) {
		if (!_pageToTid.containsKey(node)) {
			_pageToTid.put(node, new HashSet<TransactionNode>());
		}
		
		return _pageToTid.get(node);
	}
	
	private void clearTransactionNode(TransactionId tid) {
		long id = tid.getId();
		assert (_transactions.containsKey(id));
		_transactions.remove(id);
	}
	
	
	
	private void removeEdge(TransactionNode transactionNode, PageNode pageNode) {
		assert (getTransactionToPageEdges(transactionNode).contains(pageNode));
		
		getTransactionToPageEdges(transactionNode).remove(pageNode);
		getPageToTransactionEdges(pageNode).remove(transactionNode);
	}
	
	private void clearDeadPages() {
		for (PageNode node : _pageToTid.keySet()) {
			if (getPageToTransactionEdges(node).isEmpty()) {
				_pageToTid.remove(node);
			}
		}
	}
	
	public synchronized void transactionComplete(TransactionId tid) {
		TransactionNode transactionNode = getTransactionNode(tid);
		for (PageNode pageNode : getTransactionToPageEdges(transactionNode)) {
			removeEdge(transactionNode, pageNode);
		}
		
		clearTransactionNode(tid);
		clearDeadPages();
	}
	
	public synchronized HashSet<PageId> pagesInTransaction(TransactionId tid) {
		TransactionNode transactionNode = getTransactionNode(tid);
		HashSet<PageId> pages = new HashSet<PageId>();
		for (PageNode pageNode : getTransactionToPageEdges(transactionNode)) {
			pages.add(pageNode.toPageId());
		}
		
		return pages;
	}
	
	public synchronized HashSet<TransactionId> transactionsUsingPage(PageId pid) {
		PageNode node = getPageNode(pid);
		HashSet<TransactionId> transactions = new HashSet<TransactionId>();
		
		for (TransactionNode transactionNode : getPageToTransactionEdges(node)) {
			transactions.add(transactionNode.toId());
		}
		
		return transactions;
	}
	
	public synchronized  int numPages(TransactionId tid) {
		return pagesInTransaction(tid).size();
	}
	
	public synchronized int numTransactions(PageId pid) {
		return transactionsUsingPage(pid).size();
	}
	
	public synchronized boolean hasWriteLock(PageId pid) {
		return numTransactions(pid) == 1;
	}
	
	public synchronized void addEdge(TransactionId transaction, PageId pid) {
		TransactionNode transactionNode = getTransactionNode(transaction);
		PageNode page = getPageNode(pid);
		
		getTransactionToPageEdges(transactionNode).add(page);
		getPageToTransactionEdges(page).add(transactionNode);
	}
	
	public synchronized void removeEdge(TransactionId transaction, PageId pid) {
		TransactionNode transactionNode = getTransactionNode(transaction);
		PageNode pageNode = getPageNode(pid);
		removeEdge(transactionNode, pageNode);
	}
	
	public synchronized boolean readsPage(TransactionId tid, PageId pid) {
		TransactionNode node = getTransactionNode(tid);
		PageNode page = getPageNode(pid);
		return getTransactionToPageEdges(node).contains(page);
	}
	
	public synchronized boolean writesPage(TransactionId tid, PageId pid) {
		TransactionNode transactionNode = getTransactionNode(tid);
		PageNode pageNode = getPageNode(pid);
		
		HashSet<TransactionNode> nodes = getPageToTransactionEdges(pageNode);
		return (nodes.size() == 1) && (nodes.contains(transactionNode));
		
	}
}
