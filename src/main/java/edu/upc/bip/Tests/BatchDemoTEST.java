package edu.upc.bip.Tests;/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements.  See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;


/**
 * Created by Anas on 11/1/2016.
 */
public final class BatchDemoTEST {

        private static final Pattern SPACE = Pattern.compile(",");

        public static void main(String[] args) throws Exception {

            // create connection with HBase
            Configuration config = null;
            try {
                config = HBaseConfiguration.create();
                config.set("hbase.rootdir", "hdfs://localhost:54310/hbase");
                config.set("hbase.zookeeper.property.dataDir", "/home/osboxes/zookeeper-3.4.6");
                config.set("hbase.cluster.distributed", "true");
                //config.set("hbase.zookeeper.quorum", "127.0.0.1");
                //config.set("hbase.zookeeper.property.clientPort","2181");
                //config.set("hbase.master", "127.0.0.1:60000");
                HBaseAdmin.checkHBaseAvailable(config);
                System.out.println("HBase is running!");
            }
            catch (MasterNotRunningException e) {
                System.out.println("HBase is not running!");
                System.exit(1);
            }catch (Exception ce){
                ce.printStackTrace();
            }

//            config.set(TableInputFormat.INPUT_TABLE, "tableName");
//
//// new Hadoop API configuration
//            Job newAPIJobConfiguration1 = Job.getInstance(config);
//            newAPIJobConfiguration1.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, "tableName");
//            newAPIJobConfiguration1.setOutputFormatClass(org.apache.hadoop.hbase.mapreduce.TableOutputFormat.class);
//
//// create Key, Value pair to store in HBase
//            JavaPairRDD<ImmutableBytesWritable, Put> hbasePuts = javaRDD.mapToPair(
//                    new PairFunction<Row, ImmutableBytesWritable, Put>() {
//                        @Override
//                        public Tuple2<ImmutableBytesWritable, Put> call(Row row) throws Exception {
//
//                            Put put = new Put(Bytes.toBytes(row.getString(0)));
//                            put.add(Bytes.toBytes("columFamily"), Bytes.toBytes("columnQualifier1"), Bytes.toBytes(row.getString(1)));
//                            put.add(Bytes.toBytes("columFamily"), Bytes.toBytes("columnQualifier2"), Bytes.toBytes(row.getString(2)));
//
//                            return new Tuple2<ImmutableBytesWritable, Put>(new ImmutableBytesWritable(), put);
//                        }
//                    });

//            // save to HBase- Spark built-in API method
//            hbasePuts.saveAsNewAPIHadoopDataset(newAPIJobConfiguration1.getConfiguration());

            JavaSparkContext spark = new JavaSparkContext(
                    new SparkConf().setAppName("JavaWordCount").setMaster("local[200]").set("spark.executor.memory","8g").set("spark.driver.maxResultSize","2g")
            );
//            SparkSession spark = SparkSession
//                    .builder()
//                    .appName("JavaWordCount")
//                    .getOrCreate();
//0.0.0.0:19000
            //JavaPairRDD<String,String> files = spark.wholeTextFiles("hdfs://localhost:19000/kafka/testkafka/16-11-01");

            JavaRDD<String> lines = spark.textFile("hdfs://localhost:19000/kafka/plt-input/test/trajectories.*");
//            JavaPairRDD<String,String> pairs = lines.mapToPair(
//                    new PairFunction<String, String, String>() {
//                        @Override
//                        public Tuple2<String, String> call(String s) {
//                            return new Tuple2<>(s.substring(0,s.indexOf(';')), s.substring(s.indexOf(';')+1));
//                        }
//                    });
            JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
                @Override
                public Iterator<String> call(String s) {
                    return Arrays.asList(SPACE.split(s)).iterator();
                }
            });

            JavaPairRDD<String, Integer> ones = words.mapToPair(
                    new PairFunction<String, String, Integer>() {
                        @Override
                        public Tuple2<String, Integer> call(String s) {
                            return new Tuple2<>(s, 1);
                        }
                    });

            JavaPairRDD<String, Integer> counts = ones.reduceByKey(
                    new Function2<Integer, Integer, Integer>() {
                        @Override
                        public Integer call(Integer i1, Integer i2) {
                            return i1 + i2;
                        }
                    });

            List<Tuple2<String, Integer>> output = counts.collect();
            for (Tuple2<?,?> tuple : output) {
                System.out.println(tuple._1() + ": " + tuple._2());
            }
//            List<Tuple2<String, String>> output1 = pairs.collect();
//            for (Tuple2<?,?> tuple : output1) {
//                System.out.println(tuple._1() + ": " + tuple._2());
//            }
            spark.stop();
        }
}
