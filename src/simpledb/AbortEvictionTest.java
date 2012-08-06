package simpledb;

import java.io.IOException;

public class AbortEvictionTest extends SimpleDbTestBase {
    /** Aborts a transaction and ensures that its effects were actually undone.
     * This requires dirty pages to <em>not</em> get flushed to disk.
     */
    public void testDoNotEvictDirtyPages()
            throws IOException, DbException, TransactionAbortedException {
        // Allocate a file with ~10 pages of data
        HeapFile f = SystemTestUtil.createRandomHeapFile(2, 512*10, null, null);
        Database.resetBufferPool(2);

        // BEGIN TRANSACTION
        Transaction t = new Transaction();
        t.start();

        // Insert a new row
        EvictionTest.insertRow(f, t);

        // The tuple must exist in the table
        boolean found = EvictionTest.findMagicTuple(f, t);
        TestUtil.assertTrue(found);
        // ABORT
        t.transactionComplete(true);

        // A second transaction must not find the tuple
        t = new Transaction();
        t.start();
        found = EvictionTest.findMagicTuple(f, t);
        TestUtil.assertFalse(found);
        t.commit();
    }
}
