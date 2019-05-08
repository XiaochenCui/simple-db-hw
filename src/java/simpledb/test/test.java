package simpledb.test;

import simpledb.*;

import java.io.File;
import java.io.IOException;

public class test {

    public static void main(String[] argv) {
        // construct a 3-column table schema
        Type types[] = new Type[]{ Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE };
        String names[] = new String[]{ "field0", "field1", "field2" };
        TupleDesc desc = new TupleDesc(types, names);

        // create the table, associate it with some_data_file.dat
        // and tell the catalog about the schema of this table.
        HeapFile table1 = new HeapFile(new File("data/lab1_some_data_file.dat"), desc);
        Database.getCatalog().addTable(table1, "test");

        // construct the query: we use a simple SeqScan, which
        // spoonfeeds tuples via its iterator.
        TransactionId tid = new TransactionId();
        SeqScan f = new SeqScan(tid, table1.getId());

        try {
            // and run it
            f.open();
            while (f.hasNext()) {
                Tuple tuple = f.next();
                System.out.println(tuple);
            }
            f.close();
            Database.getBufferPool().transactionComplete(tid);
        } catch (DbException e) {
            e.printStackTrace();
        } catch (TransactionAbortedException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
