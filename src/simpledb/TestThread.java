package simpledb;

public final class TestThread extends Thread {
	TransactionId tid;
    PageId pid;
    Permissions perm;
    boolean acquired;
    Exception error;
    Object alock;
    Object elock;

    /**
     * @param tid the transaction on whose behalf we want to acquire the lock
     * @param pid the page over which we want to acquire the lock
     * @param perm the desired lock permissions
     */
    public TestThread(TransactionId tid, PageId pid, Permissions perm) {
        this.tid = tid;
        this.pid = pid;
        this.perm = perm;
        this.acquired = false;
        this.error = null;
        this.alock = new Object();
        this.elock = new Object();
    }

    public void run() {
        try {
            Database.getBufferPool().getPage(tid, pid, perm);
            synchronized(alock) {
                acquired = true;
            }
        } catch (Exception e) {
            e.printStackTrace();
            synchronized(elock) {
                error = e;
            }

            try {
                Database.getBufferPool().transactionComplete(tid, false);
            } catch (java.io.IOException e2) {
                e2.printStackTrace();
            }
        }
    }

    /**
     * @return true if we successfully acquired the specified lock
     */
    public boolean acquired() {
        synchronized(alock) {
            return acquired;
        }
    }

    /**
     * @return an Exception instance if one occured during lock acquisition;
     *   null otherwise
     */
    public Exception getError() {
        synchronized(elock) {
            return error;
        }
    }
}
