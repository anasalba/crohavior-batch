package edu.upc.bip.Tests;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HBaseUtils2 {

    private static Configuration conf = null;
    private static Connection connection = null;
    /**
     * Initialization
     */
    static {


        conf = HBaseConfiguration.create();
        conf.clear();
        conf.set("hbase.zookeeper.quorum", "127.0.0.1");
        conf.set("hbase.zookeeper.property.clientPort","2181");


        try {
            connection = ConnectionFactory.createConnection(conf);
        }catch (Exception e)
        {
            System.out.println("fuck");
        }
    }

    /**
     * Create a table
     */
    public static void creatTable(String tableName, String[] familys)
            throws Exception {
        try {
            Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();
        if (admin.tableExists(TableName.valueOf(tableName))) {
            System.out.println("table already exists!");
        } else {
            HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(tableName));
            for (int i = 0; i < familys.length; i++) {
                tableDesc.addFamily(new HColumnDescriptor(familys[i]));
            }
            admin.createTable(tableDesc);
            System.out.println("create table " + tableName + " ok.");
        }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Delete a table
     */
    public static void deleteTable(String tableName) throws Exception {
        try {
            Connection connection = ConnectionFactory.createConnection(conf);
            Admin admin = connection.getAdmin();
            admin.disableTable(TableName.valueOf(tableName));
            admin.deleteTable(TableName.valueOf(tableName));
            System.out.println("delete table " + tableName + " ok.");
        } catch (MasterNotRunningException e) {
            e.printStackTrace();
        } catch (ZooKeeperConnectionException e) {
            e.printStackTrace();
        }
    }

    /**
     * Put (or insert) a row
     */

    public static void addRecord(String tableName, String rowKey,
                                 String family, String qualifier, String value) throws Exception {
        try {

            HTable table = (HTable) connection.getTable(TableName.valueOf(tableName));
            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes
                    .toBytes(value));
            table.put(put);
            System.out.println("insert recored " + rowKey + " to table "
                    + tableName + " ok.");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Delete a row
     */
    public static void delRecord(String tableName, String rowKey)
            throws IOException {
        Connection connection = ConnectionFactory.createConnection(conf);
        HTable table = (HTable) connection.getTable(TableName.valueOf(tableName));
        List<Delete> list = new ArrayList<Delete>();
        Delete del = new Delete(rowKey.getBytes());
        list.add(del);
        table.delete(list);
        System.out.println("del recored " + rowKey + " ok.");
    }

    /**
     * Get a row
     */
    public static void getOneRecord (String tableName, String rowKey) throws IOException{
        Connection connection = ConnectionFactory.createConnection(conf);
        HTable table = (HTable) connection.getTable(TableName.valueOf(tableName));
        Get get = new Get(rowKey.getBytes());
        Result rs = table.get(get);

        for(Cell cell : rs.rawCells()){

            String row = new String(CellUtil.cloneRow(cell));
            String family = new String(CellUtil.cloneFamily(cell));
            String column = new String(CellUtil.cloneQualifier(cell));
            String value = new String(CellUtil.cloneValue(cell));
            long timestamp = cell.getTimestamp();
            System.out.printf("%s column=%s:%s, timestamp=%s, value=%s\n", row, family, column, timestamp, value);
        }

    }
    /**
     * Get a row
     */
    public static List<String> getRecordRangeValues (String tableName, String startRowKey, String stopRowKey) throws IOException{
        try{
            Connection connection = ConnectionFactory.createConnection(conf);
            HTable table = (HTable) connection.getTable(TableName.valueOf(tableName));
            Scan s = new Scan();

            List<String> result = new ArrayList<>();
            s.setStartRow(Bytes.toBytes(startRowKey));
            s.setStopRow(Bytes.toBytes(stopRowKey));
            ResultScanner ss = table.getScanner(s);

            for(Result r:ss){
                for(Cell cell : r.rawCells()){
                    result.add(new String(CellUtil.cloneValue(cell)));
                }
            }
            ss.close();
            return result;
        } catch (IOException e){
            e.printStackTrace();
        }
        return null;
    }
    /**
     * Scan (or list) a table
     */
    public static void getAllRecord (String tableName) {
        try{
            Connection connection = ConnectionFactory.createConnection(conf);
            HTable table = (HTable) connection.getTable(TableName.valueOf(tableName));
            Scan s = new Scan();


            //filter

//            FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
//            SingleColumnValueFilter filter1 = new SingleColumnValueFilter(Bytes.toBytes("course"),Bytes.toBytes("art"), CompareFilter.CompareOp.EQUAL,Bytes.toBytes("87"));
//            filterList.addFilter(filter1);
//            s.setFilter(filterList);
            //end filter
//            s.setStartRow(Bytes.toBytes("ban"));
//            s.setStopRow(Bytes.toBytes("ban"));
            ResultScanner ss = table.getScanner(s);

            for(Result r:ss){
                for(Cell cell : r.rawCells()){
                    String row = new String(CellUtil.cloneRow(cell));
                    String family = new String(CellUtil.cloneFamily(cell));
                    String column = new String(CellUtil.cloneQualifier(cell));
                    String value = new String(CellUtil.cloneValue(cell));
                    long timestamp = cell.getTimestamp();
                    System.out.printf("%s column=%s:%s, timestamp=%s, value=%s\n", row, family, column, timestamp, value);
                }
            }
        } catch (IOException e){
            e.printStackTrace();
        }
    }

    public static void main(String[] agrs) {
        try {
            String tablename = "datamining";
            String[] familys = { "data"};
            HBaseUtils2.deleteTable(tablename);
            HBaseUtils2.creatTable(tablename, familys);

            // add record zkb
            HBaseUtils2.addRecord(tablename, "ss2008-10-23T02:56:15e2008-10-23T04:56:15", "data", "",  "{trajectories:[{points:[{a:2.0, o:2.0}], f:4}, {points:[{a:2.0, o:3.0}], f:3}, {points:[{a:2.0, o:3.0}, {a:2.0, o:2.0}], f:3}, {points:[{a:5.0, o:2.0}], f:2}, {points:[{a:5.0, o:2.0}, {a:2.0, o:2.0}], f:1}, {points:[{a:4.0, o:3.0}], f:2}]}");
            HBaseUtils2.addRecord(tablename, "ss2008-10-23T04:56:15e2008-10-23T06:56:15", "data", "",  "{trajectories:[{points:[{a:2.0, o:2.0}], f:4}, {points:[{a:2.0, o:6.0}], f:6}, {points:[{a:2.0, o:6.0}, {a:2.0, o:2.0}], f:6}, {points:[{a:10.0, o:2.0}], f:2}, {points:[{a:10.0, o:2.0}, {a:2.0, o:2.0}], f:1}, {points:[{a:4.0, o:6.0}], f:2}]}");
            HBaseUtils2.addRecord(tablename, "ss2008-10-23T06:56:15e2008-10-23T08:56:15", "data", "",  "{trajectories:[{points:[{a:10.0, o:10.0}], f:1}, {points:[{a:10.0, o:3.0}], f:3}, {points:[{a:10.0, o:3.0}, {a:10.0, o:10.0}], f:3}, {points:[{a:5.0, o:10.0}], f:10}, {points:[{a:5.0, o:10.0}, {a:10.0, o:10.0}], f:1}, {points:[{a:1.0, o:3.0}], f:10}]}");

//            // add record baoniu
//            HBaseUtils2.addRecord(tablename, "baoniu", "grade", "", "4");
//            HBaseUtils2.addRecord(tablename, "baoniu", "course", "math", "89");
//
//            System.out.println("===========get one record========");
//            HBaseUtils2.getOneRecord(tablename, "zkb");
//
//            System.out.println("===========show all record========");
//            HBaseUtils2.getAllRecord(tablename);
//
//            System.out.println("===========del one record========");
//            HBaseUtils2.delRecord(tablename, "baoniu");
//            HBaseUtils2.getAllRecord(tablename);
//
//            System.out.println("===========show all record========");
//            HBaseUtils2.getAllRecord(tablename);

//            SparkConf sparkConf = new SparkConf().setAppName("JavaHBaseBulkGetExample ");
//            JavaSparkContext jsc = new JavaSparkContext(sparkConf);
//            Configuration conf = HBaseConfiguration.create();
//            JavaHBaseContext hbaseContext = new JavaHBaseContext(jsc, conf);
//            hbaseContext.hbaseRDD("sdfsdf", new Scan());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}