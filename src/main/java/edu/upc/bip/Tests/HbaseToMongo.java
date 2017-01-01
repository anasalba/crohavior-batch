package edu.upc.bip.Tests;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;
import edu.upc.bip.model.heatmapConverter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.bson.Document;
import scala.Tuple1;
import scala.Tuple2;

import java.io.IOException;
import java.time.format.DateTimeFormatter;
import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

/**
 * Created by osboxes on 18/12/16.
 */
public class HbaseToMongo {
    private static DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    static String convertScanToString(Scan scan) throws IOException {

        ClientProtos.Scan proto = ProtobufUtil.toScan(scan);
        return Base64.encodeBytes(proto.toByteArray());
    }
    public static void main(String[] agrs) throws Exception {

        SparkConf sc = new SparkConf()
                .setMaster("local")
                .setAppName("MongoSparkConnectorTour")
                .set("spark.mongodb.input.uri", "mongodb://katya:echo216@ds135798.mlab.com:35798/heroku_41659s43.heatmap")
                .set("spark.mongodb.output.uri", "mongodb://katya:echo216@ds135798.mlab.com:35798/heroku_41659s43.heatmap");

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

        JavaSparkContext javaSparkContext = new JavaSparkContext(sc);

        conf.set(TableInputFormat.INPUT_TABLE, "heatmap");
        JavaPairRDD<ImmutableBytesWritable, Result> hbaseRDD = javaSparkContext.newAPIHadoopRDD(conf, TableInputFormat.class, ImmutableBytesWritable.class, Result.class);

//        JavaRDD<heatmapConverter> studentRDD = hbaseRDD.map(new Function<Tuple2<ImmutableBytesWritable,Result>, heatmapConverter>() {
//            public heatmapConverter  call(Tuple2<ImmutableBytesWritable, Result> tuple) throws Exception {
//                heatmapConverter  bean = new heatmapConverter();
//                try {
//                    bean.data = Bytes.toString(tuple._2.getValue(Bytes.toBytes("data"),Bytes.toBytes(""))).split("data\":")[1];
//                    bean.data = bean.data.substring(0,bean.data.length()-1);
//                    bean.id = Bytes.toString(tuple._2.getRow());
//                    return bean;
//                } catch(Exception e) {
//                    e.printStackTrace();
//                    return null;
//                }
//            }
//        });
//        System.out.println("**************"+studentRDD.count()+"***************");
//        List<heatmapConverter> output = studentRDD.collect();
//        for (heatmapConverter tuple : output) {
//            System.out.println(tuple.getId() + ": " + tuple.getData());
//            break;
//        }

        String data = "[{\"a\":39.981,\"c\":6,\"o\":116.343},{\"a\":39.883,\"c\":1,\"o\":116.471},{\"a\":39.964,\"c\":6,\"o\":116.396},{\"a\":39.908,\"c\":2,\"o\":116.405},{\"a\":39.868,\"c\":1,\"o\":116.442},{\"a\":39.88,\"c\":6,\"o\":116.259},{\"a\":39.959,\"c\":6,\"o\":116.297},{\"a\":39.923,\"c\":3,\"o\":116.256},{\"a\":39.891,\"c\":19,\"o\":116.455},{\"a\":39.979,\"c\":6,\"o\":116.314},{\"a\":39.951,\"c\":3,\"o\":116.328},{\"a\":39.991,\"c\":22,\"o\":116.315},{\"a\":39.904,\"c\":1,\"o\":116.455},{\"a\":39.99,\"c\":3,\"o\":116.388},{\"a\":39.94,\"c\":3,\"o\":116.312},{\"a\":39.961,\"c\":3,\"o\":116.365},{\"a\":39.865,\"c\":4,\"o\":116.378},{\"a\":39.882,\"c\":3,\"o\":11}]";
//        String data = Bytes.toString(tuple._2.getValue(Bytes.toBytes("data"),Bytes.toBytes("")));
//        data = data.substring(8,data.length()-1);
        Document sasd = Document.parse("{_id: " + "\"10s2008-10-23T02:57:35\"" + ", data: " + 6+ "}");
        JavaRDD<Document> documents = hbaseRDD.map(new Function<Tuple2<ImmutableBytesWritable,Result>, Document>() {
            public Document  call(Tuple2<ImmutableBytesWritable, Result> tuple) throws Exception {

                String id = "\""+Bytes.toString(tuple._2.getRow())+"\"";
                    return Document.parse("{_id: " + id+ ", data:" +data+ "}");
            }
        });
       System.out.println("**************"+documents.count()+"***************");
      //  List<Document> lll= documents.collect();

        MongoSpark.save(documents,Document.class);

    }


}
