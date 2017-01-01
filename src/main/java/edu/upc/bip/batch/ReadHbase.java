package edu.upc.bip.batch;

import edu.upc.bip.model.Coordinate;
import edu.upc.bip.model.Transaction;
import edu.upc.bip.model.heatmapConverter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.spark.JavaHBaseContext;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import scala.Tuple1;
import scala.Tuple2;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * Created by osboxes on 11/12/16.
 */
public class ReadHbase {

    private static DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    static String convertScanToString(Scan scan) throws IOException {

        ClientProtos.Scan proto = ProtobufUtil.toScan(scan);
        return Base64.encodeBytes(proto.toByteArray());
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = null;
        Connection connection = null;

        conf = HBaseConfiguration.create();
        conf.clear();
        conf.set("hbase.zookeeper.quorum", "127.0.0.1");
        conf.set("hbase.zookeeper.property.clientPort","2181");

        connection = ConnectionFactory.createConnection(conf);
        Scan s = new Scan();
    //    s.setStartRow(Bytes.toBytes("t2008-10-23 02:53:151"));
    //    s.setStopRow(Bytes.toBytes("t2008-10-23 02:53:161"));
        conf.set(TableInputFormat.SCAN, convertScanToString(s));
        SparkConf sparkConf = new SparkConf().setAppName("Read data From HBase table").setMaster("local");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

        conf.set(TableInputFormat.INPUT_TABLE, "heatmap");
        JavaPairRDD<ImmutableBytesWritable, Result> hbaseRDD = javaSparkContext.newAPIHadoopRDD(conf, TableInputFormat.class, ImmutableBytesWritable.class, Result.class);
        JavaRDD<heatmapConverter> studentRDD = hbaseRDD.map(new Function<Tuple2<ImmutableBytesWritable,Result>, heatmapConverter>() {
            public heatmapConverter  call(Tuple2<ImmutableBytesWritable, Result> tuple) throws Exception {
                heatmapConverter  bean = new heatmapConverter();
                try {
                    bean.data = Bytes.toString(tuple._2.getValue(Bytes.toBytes("data"),Bytes.toBytes("")));
                    bean.id = Bytes.toString(tuple._2.getRow());
                    return bean;
                } catch(Exception e) {
                    e.printStackTrace();
                    return null;
                }
            }
        });
        System.out.println("**************"+studentRDD.count()+"***************");
//        for (Transaction t : studentRDD.collect()) {
//            System.out.println("user: "+t.getUserID()+"t: "+ t.getTimestamp());
//        }
    }

}
