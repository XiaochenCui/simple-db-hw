package simpledb.test;

import simpledb.*;

import java.io.File;

public class joinTest {

    public static void main(String[] argv) {
        // construct a 3-column table schema
        Type types[] = new Type[]{Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE};
        String names[] = new String[]{"field0", "field1", "field2"};

        TupleDesc tupleDesc = new TupleDesc(types, names);

        // create the tables, associate them with the data files
        // and tell the catalog about the schema the tables.
        HeapFile table1 = new HeapFile(new File("data/lab2/some_data_file1.dat"), tupleDesc);
        Database.getCatalog().addTable(table1, "t1");

        HeapFile table2 = new HeapFile(new File("data/lab2/some_data_file2.dat"), tupleDesc);
        Database.getCatalog().addTable(table2, "t2");

        // construct the query: we use two SeqScans, which spoon-feed
        // tuples via iterators into join
        TransactionId tid = new TransactionId();

        SeqScan ss1 = new SeqScan(tid, table1.getId(), "t1");
        SeqScan ss2 = new SeqScan(tid, table2.getId(), "t2");

        // create a filter for the where condition
        Filter sf1 = new Filter(
                new Predicate(0, Predicate.Op.GREATER_THAN, new IntField(1)),
                ss1
        );

        JoinPredicate p = new JoinPredicate(1, Predicate.Op.EQUALS, 1);
        Join j = new Join(p, sf1, ss2);

        // and run it
        try {
            j.open();
            while (j.hasNext()) {
                Tuple tuple = j.next();
                System.out.println(tuple);
            }
            j.close();
            Database.getBufferPool().transactionComplete(tid);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
