package simpledb;


import java.io.IOException;
import java.util.ArrayList;

public class DeleteTest extends FilterBase {
    ArrayList<ArrayList<Integer>> expectedTuples = null;

    @Override
    protected int applyPredicate(HeapFile table, TransactionId tid, Predicate predicate)
            throws DbException, TransactionAbortedException, IOException {
        SeqScan ss = new SeqScan(tid, table.getId(), "");
        Filter filter = new Filter(predicate, ss);
        Delete deleteOperator = new Delete(tid, filter);
        Query q = new Query(deleteOperator, tid);

        q.start();
        boolean hasResult = false;
        int result = -1;
        while (q.hasNext()) {
            Tuple t = q.next();
            TestUtil.assertFalse(hasResult);
            hasResult = true;
            TestUtil.assertEquals(SystemTestUtil.SINGLE_INT_DESCRIPTOR, t.getTupleDesc());
            result = ((IntField) t.getField(0)).getValue();
        }
        TestUtil.assertTrue(hasResult);

        deleteOperator.close();

        // As part of the same transaction, scan the table
        if (result == 0) {
            // Deleted zero tuples: all tuples still in table
            expectedTuples = createdTuples;
        } else {
            assert result == createdTuples.size();
            expectedTuples = new ArrayList<ArrayList<Integer>>();
        }
        SystemTestUtil.matchTuples(table, tid, expectedTuples);
        return result;
    }

    @Override
    protected void validateAfter(HeapFile table)
            throws DbException, TransactionAbortedException, IOException {
        // As part of a different transaction, scan the table
        SystemTestUtil.matchTuples(table, expectedTuples);
    }
}
