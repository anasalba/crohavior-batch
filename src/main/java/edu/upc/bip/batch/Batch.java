package edu.upc.bip.batch;/*
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



import edu.upc.bip.model.Coordinate;
import edu.upc.bip.model.Transaction;
import edu.upc.bip.utils.BatchUtils;
import edu.upc.bip.utils.HBaseUtils;
import edu.upc.bip.utils.TimestampComparator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.protobuf.generated.FilterProtos;
import org.apache.hadoop.hbase.util.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.storage.StorageLevel;
import org.codehaus.jackson.map.ObjectMapper;
import scala.Tuple2;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;


/**
 * Created by Anas on 11/1/2016.
 */
public final class Batch {

        private static final Pattern SPACE = Pattern.compile(";");

        private static final Integer Latitude = 0;
        private static final Integer Longtitude = 1;
        private static final Integer UserID = 7;
        private static final Integer TimeStamp = 4;
        private static final String tablename = "heatmapupc";
        private static final String[] familys = { "data" };
        private static ObjectMapper objectMapper = new ObjectMapper();
        private static DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        static List<LocalDateTime> localDateTimeList = new ArrayList<>();
        static String convertScanToString(Scan scan) throws IOException {

            ClientProtos.Scan proto = ProtobufUtil.toScan(scan);
            return org.apache.hadoop.hbase.util.Base64.encodeBytes(proto.toByteArray());
        }
        public static void main(String[] args) throws Exception {
            HBaseUtils.deleteTable(tablename);
            HBaseUtils.creatTable(tablename, familys);

/*            JavaSparkContext spark = new JavaSparkContext(
                    new SparkConf().setAppName("JavaWordCount").setMaster("local[20]").set("spark.executor.memory","8g").set("spark.driver.maxResultSize","2g")
            );

            JavaRDD<String> lines = spark.textFile("hdfs://localhost:54310/kafka/plt-input/test/trajectories.*");

            //lines to Transactions
            JavaRDD<Transaction> transactions = lines.map(
                    new Function<String, Transaction>() {
                        @Override
                        public Transaction call(String row) throws Exception {
                            String[] words = row.split(";");
                            LocalDateTime time = LocalDateTime.parse(words[TimeStamp], formatter);
                            Double latitude = Double.parseDouble(words[Latitude]);
                            Double longitude = Double.parseDouble(words[Longtitude]);
                            String userID = words[UserID];
                            Coordinate coordinate = new Coordinate(latitude, longitude);
                            return new Transaction(coordinate,time,userID);
                        }
                    }
            );*/
            localDateTimeList.add(LocalDateTime.now());
            JavaSparkContext spark = new JavaSparkContext(
                    new SparkConf().setAppName("JavaWordCount").setMaster("local[200]").set("spark.executor.memory","10g").set("spark.driver.maxResultSize","4g")
            );

            Configuration conf = null;
            Connection connection = null;

            conf = HBaseConfiguration.create();
            conf.clear();
            conf.set("hbase.zookeeper.quorum", "127.0.0.1");
            conf.set("hbase.zookeeper.property.clientPort","2181");

            connection = ConnectionFactory.createConnection(conf);
            Scan s = new Scan();
//            FilterList filterList = new FilterList(new PageFilter(400));
//            s.setFilter(filterList);
            //    s.setStartRow(Bytes.toBytes("t2008-10-23 02:53:151"));
            //    s.setStopRow(Bytes.toBytes("t2008-10-23 02:53:161"));
            conf.set(TableInputFormat.SCAN, convertScanToString(s));

            localDateTimeList.add(LocalDateTime.now());
            conf.set(TableInputFormat.INPUT_TABLE, "csvtohbase");
            JavaPairRDD<ImmutableBytesWritable, Result> hbaseRDD = spark.newAPIHadoopRDD(conf, TableInputFormat.class, ImmutableBytesWritable.class, Result.class);
            localDateTimeList.add(LocalDateTime.now());
            JavaRDD<Transaction> transactions = hbaseRDD.map(new Function<Tuple2<ImmutableBytesWritable,Result>, Transaction>() {
                public Transaction  call(Tuple2<ImmutableBytesWritable, Result> tuple) throws Exception {

                    try {
                        String[] values = Bytes.toString(tuple._2.getValue(Bytes.toBytes("data"),Bytes.toBytes(""))).split(";");
                        return new Transaction(new Coordinate(Double.parseDouble(values[0]),Double.parseDouble(values[1])),LocalDateTime.parse(values[2], formatter),values[3]);
                    } catch(Exception e) {
                        e.printStackTrace();
                        return null;
                    }
                }
            });
            //System.out.print(transactions.count());
            localDateTimeList.add(LocalDateTime.now());
            JavaRDD<Transaction> transactionstsWithRoundedCoordinates = BatchUtils.roundCoordinates(transactions,1000).persist(StorageLevel.MEMORY_ONLY());;
            localDateTimeList.add(LocalDateTime.now());



            LocalDateTime minTimestamp = transactionstsWithRoundedCoordinates.min(new TimestampComparator()).getTimestamp();
            LocalDateTime maxTimestamp = transactionstsWithRoundedCoordinates.max(new TimestampComparator()).getTimestamp();
            long duration = minTimestamp.until(maxTimestamp, ChronoUnit.SECONDS);
            int maxDetail = (int) duration;
            long[] steps = {5,10,15};
            for(long step:steps) {
                long timeStep = step;
                for (int i = 0, j = 0; j < 3600*4; i++, j += timeStep) {

                    if(i%60==0)
                    {
                        int cccccc=4;
                    }
                    LocalDateTime start = minTimestamp.plus(timeStep * i, ChronoUnit.SECONDS);
                    LocalDateTime end = minTimestamp.plus(timeStep * (i + 1), ChronoUnit.SECONDS);
                    JavaRDD<Transaction> measurementsFilteredByTime = BatchUtils.filterByTime(transactionstsWithRoundedCoordinates, start, end);
                    JavaPairRDD<Coordinate, Integer> counts = BatchUtils.countPerGridBox(measurementsFilteredByTime);

                    BatchUtils.writeJsonToHbase(counts, tablename, familys[0], step + "s" + start.toString(),objectMapper);
                }

            }
            localDateTimeList.add(LocalDateTime.now());
            spark.stop();
            localDateTimeList.add(LocalDateTime.now());
            for (LocalDateTime localDateTime:localDateTimeList)
            {
                System.out.println(localDateTime);
            }
        }
}
