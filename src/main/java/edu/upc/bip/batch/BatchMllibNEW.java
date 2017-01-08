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


import edu.upc.bip.model.AssociationRulesLocalModel;
import edu.upc.bip.model.Coordinate;
import edu.upc.bip.model.FPGrowthLocalModel;
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
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.fpm.AssociationRules;
import org.apache.spark.mllib.fpm.FPGrowth;
import org.apache.spark.mllib.fpm.FPGrowthModel;
import org.apache.spark.storage.StorageLevel;
import org.codehaus.jackson.map.ObjectMapper;
import scala.Tuple2;

import java.io.IOException;
import java.io.Serializable;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;


/**
 * Created by Anas on 11/1/2016.
 */
public final class BatchMllibNEW implements Serializable{

        private static ObjectMapper objectMapper = new ObjectMapper();
        private static DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

        static String convertScanToString(Scan scan) throws IOException {

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
        //    s.setStartRow(Bytes.toBytes("t2008-10-23 02:53:151"));
        //    s.setStopRow(Bytes.toBytes("t2008-10-23 02:53:161"));
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

            JavaPairRDD<String, List<Transaction>> trajectories = transactionstsWithRoundedCoordinates.mapToPair(
                    new PairFunction<Transaction, String, List<Transaction>>() {
                        @Override
                        public Tuple2<String, List<Transaction>> call(Transaction s) {
                            List<Transaction> ss = new ArrayList<>();
                            ss.add(s);
                            return new Tuple2<>(s.getUserID(), ss);
                        }
                    }).reduceByKey(
                    new Function2<List<Transaction>, List<Transaction>, List<Transaction>>() {
                        @Override
                        public List<Transaction> call(List<Transaction> i1, List<Transaction> i2) {
                            List<Transaction> ss = new ArrayList<>();
                            ss.addAll(i1);
                            ss.addAll(i2);
                            ss.sort(new TimestampComparator());
                            return ss;
                        }
                    });

            JavaRDD<List<Coordinate>> anTrajectories = trajectories.map(
                    new Function<Tuple2<String, List<Transaction>>, List<Coordinate>>() {
                        @Override
                        public List<Coordinate> call(Tuple2<String, List<Transaction>> st) throws Exception {
                            List<Coordinate> c = new ArrayList<Coordinate>();
                            for (Transaction t : st._2) {
                                if (!c.contains(t.getRoundedCoordinate()))
                                    c.add(t.getRoundedCoordinate());
                            }
                            return c;
                        }
                    }
            ).persist(StorageLevel.MEMORY_ONLY());

            FPGrowth fpg = new FPGrowth()
                    .setMinSupport(0.05)
                    .setNumPartitions(100);
            FPGrowthModel<Coordinate> model = fpg.run(anTrajectories);

            List<FPGrowthLocalModel> fpGrowths = new ArrayList<>();
            for (FPGrowth.FreqItemset<Coordinate> itemset : model.freqItemsets().toJavaRDD().collect()) {
                fpGrowths.add(new FPGrowthLocalModel(itemset.javaItems(), itemset.freq()));
            }

            try {
                HBaseUtils.addRecord("datamining", "s" + minTimestamp + "e" + maxTimestamp, "data", "", objectMapper.writeValueAsString(fpGrowths));
            } catch (Exception e) {
                e.printStackTrace();
            }

            List<AssociationRulesLocalModel> associationRulesLocalModels = new ArrayList<>();
            double minConfidence = 0.8;
            for (AssociationRules.Rule<Coordinate> rule
                    : model.generateAssociationRules(minConfidence).toJavaRDD().collect()) {
                associationRulesLocalModels.add(new AssociationRulesLocalModel(rule.javaAntecedent(), rule.javaConsequent(), rule.confidence()));
            }
            try {
                HBaseUtils.addRecord("datamining", "r" + minTimestamp + "e" + maxTimestamp, "data", "", objectMapper.writeValueAsString(associationRulesLocalModels));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public void run(JavaSparkContext spark, String startRow , String endRow) throws Exception {

        Configuration conf = null;
        conf = HBaseConfiguration.create();
        conf.clear();
        conf.set("hbase.zookeeper.quorum", "127.0.0.1");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        Scan s = new Scan();
        s.setStartRow(Bytes.toBytes(startRow));
        s.setStopRow(Bytes.toBytes(endRow));
        conf.set(TableInputFormat.SCAN, convertScanToString(s));


        conf.set(TableInputFormat.INPUT_TABLE, "transactions");
        JavaPairRDD<ImmutableBytesWritable, Result> hbaseRDD = spark.newAPIHadoopRDD(conf, TableInputFormat.class, ImmutableBytesWritable.class, Result.class);
        JavaRDD<Transaction> transactions = hbaseRDD.map(new Function<Tuple2<ImmutableBytesWritable, Result>, Transaction>() {
            public Transaction call(Tuple2<ImmutableBytesWritable, Result> tuple) throws Exception {

                try {
                    String[] values = Bytes.toString(tuple._2.getValue(Bytes.toBytes("data"), Bytes.toBytes(""))).split(";");
                    return new Transaction(new Coordinate(Double.parseDouble(values[0]), Double.parseDouble(values[1])), LocalDateTime.parse(values[2], formatter), values[3]);
                } catch (Exception e) {
                    e.printStackTrace();
                    return null;
                }
            }
        });
        JavaRDD<Transaction> transactionstsWithRoundedCoordinates = BatchUtils.roundCoordinates(transactions,1000).cache();

        if (transactionstsWithRoundedCoordinates.count() > 0) {

            LocalDateTime minTimestamp = transactions.min(new TimestampComparator()).getTimestamp();
            LocalDateTime maxTimestamp = transactions.max(new TimestampComparator()).getTimestamp();

            JavaPairRDD<String, List<Transaction>> trajectories = transactionstsWithRoundedCoordinates.mapToPair(
                    new PairFunction<Transaction, String, List<Transaction>>() {
                        @Override
                        public Tuple2<String, List<Transaction>> call(Transaction s) {
                            List<Transaction> ss = new ArrayList<>();
                            ss.add(s);
                            return new Tuple2<>(s.getUserID(), ss);
                        }
                    }).reduceByKey(
                    new Function2<List<Transaction>, List<Transaction>, List<Transaction>>() {
                        @Override
                        public List<Transaction> call(List<Transaction> i1, List<Transaction> i2) {
                            List<Transaction> ss = new ArrayList<>();
                            ss.addAll(i1);
                            ss.addAll(i2);
                            ss.sort(new TimestampComparator());
                            return ss;
                        }
                    });

            JavaRDD<List<Coordinate>> anTrajectories = trajectories.map(
                    new Function<Tuple2<String, List<Transaction>>, List<Coordinate>>() {
                        @Override
                        public List<Coordinate> call(Tuple2<String, List<Transaction>> st) throws Exception {
                            List<Coordinate> c = new ArrayList<Coordinate>();
                            for (Transaction t : st._2) {
                                if (!c.contains(t.getRoundedCoordinate()))
                                    c.add(t.getRoundedCoordinate());
                            }
                            return c;
                        }
                    }
            ).persist(StorageLevel.MEMORY_ONLY());

            FPGrowth fpg = new FPGrowth()
                    .setMinSupport(0.05)
                    .setNumPartitions(100);
            FPGrowthModel<Coordinate> model = fpg.run(anTrajectories);

            List<FPGrowthLocalModel> fpGrowths = new ArrayList<>();
            for (FPGrowth.FreqItemset<Coordinate> itemset : model.freqItemsets().toJavaRDD().collect()) {
                fpGrowths.add(new FPGrowthLocalModel(itemset.javaItems(), itemset.freq()));
            }

            try {
                HBaseUtils.addRecord("datamining", "s" + minTimestamp + "e" + maxTimestamp, "data", "", objectMapper.writeValueAsString(fpGrowths));
            } catch (Exception e) {
                e.printStackTrace();
            }

            List<AssociationRulesLocalModel> associationRulesLocalModels = new ArrayList<>();
            double minConfidence = 0.8;
            for (AssociationRules.Rule<Coordinate> rule
                    : model.generateAssociationRules(minConfidence).toJavaRDD().collect()) {
                associationRulesLocalModels.add(new AssociationRulesLocalModel(rule.javaAntecedent(), rule.javaConsequent(), rule.confidence()));
            }
            try {
                HBaseUtils.addRecord("datamining", "r" + minTimestamp + "e" + maxTimestamp, "data", "", objectMapper.writeValueAsString(associationRulesLocalModels));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
