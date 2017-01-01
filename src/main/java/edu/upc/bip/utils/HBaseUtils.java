package edu.upc.bip.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HBaseUtils {

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
    public static String getOneRecordValue (String tableName, String rowKey) throws IOException{
        Connection connection = ConnectionFactory.createConnection(conf);
        HTable table = (HTable) connection.getTable(TableName.valueOf(tableName));
        Get get = new Get(rowKey.getBytes());
        Result rs = table.get(get);
        if(rs.rawCells().length>0) {
            Cell cell = rs.rawCells()[0];
            return new String(CellUtil.cloneValue(cell));
        }
        return "";
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
     * Get a row
     */
    public static List<String> getAllRawIDs (String tableName) throws IOException{
        try{
            Connection connection = ConnectionFactory.createConnection(conf);
            HTable table = (HTable) connection.getTable(TableName.valueOf(tableName));
            Scan s = new Scan();

            List<String> result = new ArrayList<>();
            ResultScanner ss = table.getScanner(s);

            for(Result r:ss){
                for(Cell cell : r.rawCells()){
                    result.add(new String(CellUtil.cloneRow(cell)));
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
            String tablename = "scores";
            String[] familys = { "grade", "course" };
            HBaseUtils.creatTable(tablename, familys);

            // add record zkb
            HBaseUtils.addRecord(tablename, "zkb", "grade", "", "5");
            HBaseUtils.addRecord(tablename, "zkb", "course", "", "90");
            HBaseUtils.addRecord(tablename, "zkb", "course", "math", "97");
            HBaseUtils.addRecord(tablename, "zkb", "course", "art", "87");
            // add record baoniu
            HBaseUtils.addRecord(tablename, "baoniu", "grade", "", "4");
            HBaseUtils.addRecord(tablename, "baoniu", "course", "math", "89");

            List<String> asd = HBaseUtils.getAllRawIDs(tablename);
            List<String> adsd = HBaseUtils.getRecordRangeValues(tablename,"zkb","zkb");

            System.out.println("===========get one record========");
            HBaseUtils.getOneRecord(tablename, "zkb");

            System.out.println("===========show all record========");
            HBaseUtils.getAllRecord(tablename);

            System.out.println("===========del one record========");
            HBaseUtils.delRecord(tablename, "baoniu");
            HBaseUtils.getAllRecord(tablename);

            System.out.println("===========show all record========");
            HBaseUtils.getAllRecord(tablename);

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