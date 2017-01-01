package edu.upc.bip.Tests;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import com.mongodb.spark.MongoSpark;
import org.bson.Document;

import com.mongodb.spark.rdd.api.java.JavaMongoRDD;
import java.util.List;

import static java.util.Collections.singletonList;
public class mongos {

    public static void main(String[] agrs) {

        SparkConf sc = new SparkConf()
                .setMaster("local")
                .setAppName("MongoSparkConnectorTour")
                .set("spark.mongodb.input.uri", "mongodb://katya:echo216@ds135798.mlab.com:35798/heroku_41659s43.heatmap")
                .set("spark.mongodb.output.uri", "mongodb://katya:echo216@ds135798.mlab.com:35798/heroku_41659s43.heatmap");

        JavaSparkContext jsc = new JavaSparkContext(sc); // Create a Java Spark Context

//        JavaRDD<Document> documents = jsc.parallelize(asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)).map
//                (new Function<Integer, Document>() {
//                    public Document call(final Integer i) throws Exception {
//                        return Document.parse("{_id: " + i+10 + ", data: " + "[{\"a\":39.981,\"c\":2,\"o\":116.343},{\"a\":39.964,\"c\":2,\"o\":116.396}]" + "}");
//                    }
//                });
//
//        MongoSpark.save(documents);


        JavaMongoRDD<Document> rdd = MongoSpark.load(jsc);
        System.out.println(rdd.count());
        System.out.println(rdd.first().toJson());

        JavaMongoRDD<Document> aggregatedRdd = rdd.withPipeline(singletonList(Document.parse("{ $match: { _id : { $gte : \"10s2008-10-23T02:53:25\" , $lte: \"10s2008-10-23T02:54:05\"} } }")));
        System.out.println(aggregatedRdd.count());
//        System.out.println(aggregatedRdd.first().toJson());

        List<Document> doc = singletonList(Document.parse("{ _id : { $gte : \"10s2008-10-23T02:53:25\" , $lte: \"10s2008-10-23T02:54:05\"} } "));
        List<Document> output = aggregatedRdd.collect();
        for (Document tuple : doc) {
            System.out.println(tuple.toJson());

        }
    }

}
