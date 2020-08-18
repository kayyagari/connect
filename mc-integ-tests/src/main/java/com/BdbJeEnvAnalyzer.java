package com;

import java.io.PrintStream;
import java.util.Properties;

import org.capnproto.StructFactory;

import com.mirth.connect.donkey.model.DatabaseConstants;
import com.mirth.connect.donkey.model.message.CapnpModel.CapConnectorMessage;
import com.mirth.connect.donkey.model.message.CapnpModel.CapMessage;
import com.mirth.connect.donkey.model.message.CapnpModel.CapMessageContent;
import com.mirth.connect.donkey.model.message.CapnpModel.CapMetadata;
import com.mirth.connect.donkey.server.BdbJeDataSource;
import com.mirth.connect.donkey.util.SerializerUtil;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Transaction;

public class BdbJeEnvAnalyzer {
    private BdbJeDataSource ds;
    private StructReaderPrinter printer;
    public BdbJeEnvAnalyzer(String envDirPath) {
        Properties props = new Properties();
        props.put(DatabaseConstants.DATABASE, BdbJeDataSource.DB_BDB_JE);
        props.put(DatabaseConstants.DATABASE_URL, envDirPath);
        ds = BdbJeDataSource.create(props);
        printer = new StructReaderPrinter();
    }

    public void readTable(String name, StructFactory factory) {
        readTable(name, factory, System.out);
    }
    
    public void readTable(String name, StructFactory factory, PrintStream target) {
        Database db = ds.getDbMap().get(name);
        Transaction txn = ds.getBdbJeEnv().beginTransaction(null, null);
        Cursor cursor = db.openCursor(txn, null);
        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();
        int count = 0;
        while(cursor.getNext(key, data, null) == OperationStatus.SUCCESS) {
            try {
                Object sr = SerializerUtil.readMessage(data.getData()).getRoot(factory);
                count++;
                printer.print(sr, target);
            }
            catch(Exception e) {
                e.printStackTrace();
            }
        }
        cursor.close();
        txn.commit();
        printer.reset();
        System.out.println(String.format("table %s has %d records", name, count));
    }

    public static void main(String[] args) throws Exception {
        String envDirPath = "/Users/dbugger/projects/zen/k-connect/server/appdata/mirthdb";
        BdbJeEnvAnalyzer analyzer = new BdbJeEnvAnalyzer(envDirPath);
        //analyzer.readTable("alert", CapAlert.factory);
        //analyzer.readTable("d_mc2", CapMessageContent.factory);
        //analyzer.readTable("d_mcm2", CapMetadata.factory);
        //analyzer.readTable("person", CapPerson.factory);
        //analyzer.readTable("d_ms2", CapStatistics.factory);
        analyzer.readTable("d_mm2", CapConnectorMessage.factory);
        
        //PrintStream out = new PrintStream(new FileOutputStream("/tmp/d_ms2.csv"));
        //analyzer.readTable("d_m2", CapMessage.factory, System.out);
        //out.flush();
        //out.close();
    }
}
