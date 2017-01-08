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
import edu.upc.bip.utils.TimestampComparator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.codehaus.jackson.map.ObjectMapper;
import scala.Tuple2;

import java.io.IOException;
import java.io.Serializable;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;


/**
 * Created by Anas on 11/1/2016.
 */
public class BatchNEW implements Serializable{

        private static final String tablename = "heatmap";
        private static final String[] familys = { "data" };
        private static ObjectMapper objectMapper = new ObjectMapper();
        private static DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    String convertScanToString(Scan scan) throws IOException {

            ClientProtos.Scan proto = ProtobufUtil.toScan(scan);
            return Base64.encodeBytes(proto.toByteArray());
        }
    public void run(JavaSparkContext spark) throws Exception {

        Configuration conf = null;
        conf = HBaseConfiguration.create();
        conf.clear();
        conf.set("hbase.zookeeper.quorum", "127.0.0.1");
        conf.set("hbase.zookeeper.property.clientPort","2181");


        Scan s = new Scan();

        conf.set(TableInputFormat.SCAN, convertScanToString(s));


        conf.set(TableInputFormat.INPUT_TABLE, "transactions");
        JavaPairRDD<ImmutableBytesWritable, Result> hbaseRDD = spark.newAPIHadoopRDD(conf, TableInputFormat.class, ImmutableBytesWritable.class, Result.class);
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

        JavaRDD<Transaction> transactionstsWithRoundedCoordinates = BatchUtils.roundCoordinates(transactions,1000).cache();

        LocalDateTime minTimestamp = transactions.min(new TimestampComparator()).getTimestamp();
        LocalDateTime maxTimestamp = transactions.max(new TimestampComparator()).getTimestamp();
        long duration = minTimestamp.until(maxTimestamp, ChronoUnit.SECONDS);
        int maxDetail = (int) duration;
        long[] steps = {5,10,15};
        for(long step:steps) {
            long timeStep = step;
            for (int i = 0, j = 0; j < maxDetail; i++, j += timeStep) {
                LocalDateTime start = minTimestamp.plus(timeStep * i, ChronoUnit.SECONDS);
                LocalDateTime end = minTimestamp.plus(timeStep * (i + 1), ChronoUnit.SECONDS);
                JavaRDD<Transaction> measurementsFilteredByTime = BatchUtils.filterByTime(transactionstsWithRoundedCoordinates, start, end);
                JavaPairRDD<Coordinate, Integer> counts = BatchUtils.countPerGridBox(measurementsFilteredByTime);

                BatchUtils.writeJsonToHbase(counts, tablename, familys[0], step + "s" + start.toString(),objectMapper);
            }

        }
    }
    public void run(JavaSparkContext spark, String startRow , String endRow) throws Exception {

        Configuration conf = null;

        conf = HBaseConfiguration.create();
        conf.clear();
        conf.set("hbase.zookeeper.quorum", "127.0.0.1");
        conf.set("hbase.zookeeper.property.clientPort","2181");

        Scan s = new Scan();
        s.setStartRow(Bytes.toBytes(startRow));
        s.setStopRow(Bytes.toBytes(endRow));
        conf.set(TableInputFormat.SCAN, convertScanToString(s));

        conf.set(TableInputFormat.INPUT_TABLE, "transactions");
        JavaPairRDD<ImmutableBytesWritable, Result> hbaseRDD = spark.newAPIHadoopRDD(conf, TableInputFormat.class, ImmutableBytesWritable.class, Result.class);
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

        JavaRDD<Transaction> transactionstsWithRoundedCoordinates = BatchUtils.roundCoordinates(transactions,1000).cache();

        if(transactionstsWithRoundedCoordinates.count() > 0) {
            LocalDateTime minTimestamp = transactions.min(new TimestampComparator()).getTimestamp();
            LocalDateTime maxTimestamp = transactions.max(new TimestampComparator()).getTimestamp();
            long duration = minTimestamp.until(maxTimestamp, ChronoUnit.SECONDS);
            int maxDetail = (int) duration;
            long[] steps = {5, 10, 15};
            for (long step : steps) {
                long timeStep = step;
                for (int i = 0, j = 0; j < maxDetail; i++, j += timeStep) {
                    LocalDateTime start = minTimestamp.plus(timeStep * i, ChronoUnit.SECONDS);
                    LocalDateTime end = minTimestamp.plus(timeStep * (i + 1), ChronoUnit.SECONDS);
                    JavaRDD<Transaction> measurementsFilteredByTime = BatchUtils.filterByTime(transactionstsWithRoundedCoordinates, start, end);
                    JavaPairRDD<Coordinate, Integer> counts = BatchUtils.countPerGridBox(measurementsFilteredByTime);

                    BatchUtils.writeJsonToHbase(counts, tablename, familys[0], step + "s" + start.toString(), objectMapper);
                }

            }
        }
    }
}
