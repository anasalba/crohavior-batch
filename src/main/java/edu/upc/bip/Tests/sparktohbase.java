package edu.upc.bip.Tests;

import edu.upc.bip.utils.HBaseUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import scala.reflect.internal.Trees;
//import org.joda.time.DateTime;
import java.io.IOException;
import java.time.LocalDateTime;

/**
 * Created by osboxes on 04/01/17.
 */
//create 'tableName', 'cf1', {SPLITS => ['3','6','9','d']}
public class sparktohbase {

    public static void main (String[] args) throws Exception {

        //HBaseUtils.deleteTable("csvtohbase");
        HBaseUtils.creatTable("csvtohbase", new String[] {"data"});

        JavaSparkContext spark = new JavaSparkContext(
                new SparkConf().setAppName("JavaWordCount").setMaster("local[200]").set("spark.executor.memory","8g").set("spark.driver.maxResultSize","2g")
        );

        Configuration conf = null;
        Connection connection = null;

        conf = HBaseConfiguration.create();
        conf.clear();
        conf.set("hbase.zookeeper.quorum", "127.0.0.1");
        conf.set("hbase.zookeeper.property.clientPort","2181");

//        2017-01-04T19:43:22.707
        //2017-01-04T19:36:32.164
        connection = ConnectionFactory.createConnection(conf);
        // create connection with HBase


        conf.set(TableInputFormat.INPUT_TABLE, "csvtohbase");

        JavaRDD<String> javaRDD = spark.textFile("/home/bip16-admin/data_orig/*/Trajectory/*.csv").filter(
                new Function<String, Boolean>() {
                    @Override
                    public Boolean call(String line) throws Exception {
                        if(line.contains("lat"))
                            return false;
                        else
                            return true;
                    }
                }
        ).cache();
// new Hadoop API configuration
        Job newAPIJobConfiguration1 = Job.getInstance(conf);
        newAPIJobConfiguration1.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, "csvtohbase");
        newAPIJobConfiguration1.setOutputFormatClass(org.apache.hadoop.hbase.mapreduce.TableOutputFormat.class);

// create Key, Value pair to store in HBase
        JavaPairRDD<ImmutableBytesWritable, Put> hbasePuts = javaRDD.mapToPair(
                new PairFunction<String, ImmutableBytesWritable, Put>() {
                    @Override
                    public Tuple2<ImmutableBytesWritable, Put> call(String line) throws Exception {

                        String words[]=line.split(";");
                        Put put = new Put(Bytes.toBytes(words[4]+words[7]));
                        put.addColumn(Bytes.toBytes("data"), Bytes.toBytes(""), Bytes
                                .toBytes(words[0]+";"+words[1]+";"+words[4]+";"+words[7]));

                        return new Tuple2<ImmutableBytesWritable, Put>(new ImmutableBytesWritable(), put);
                    }
                }).cache();
//        System.out.println("*****************************  "+hbasePuts.count());
        // save to HBase- Spark built-in API method
        System.out.println(LocalDateTime.now());
        hbasePuts.saveAsNewAPIHadoopDataset(newAPIJobConfiguration1.getConfiguration());
        System.out.println(LocalDateTime.now());
    }
}
