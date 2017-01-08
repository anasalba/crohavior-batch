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


import edu.upc.bip.model.*;
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
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;


/**
 * Created by Anas on 11/1/2016.
 */
public final class BatchMLLIB {

    private static final Pattern SPACE = Pattern.compile(";");

    private static final Integer Latitude = 0;
    private static final Integer Longtitude = 1;
    private static final Integer UserID = 7;
    private static final Integer TimeStamp = 4;
    private static final String tablename = "heatmap";
    private static final String[] familys = {"data"};
    private static ObjectMapper objectMapper = new ObjectMapper();
    private static DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    static List<LocalDateTime> localDateTimeList = new ArrayList<>();
    static String convertScanToString(Scan scan) throws IOException {

        ClientProtos.Scan proto = ProtobufUtil.toScan(scan);
        return Base64.encodeBytes(proto.toByteArray());
    }


    public static void main(String[] args) throws Exception {

        HBaseUtils.deleteTable("dataminingupc");
        HBaseUtils.creatTable("dataminingupc", new String[] {"data"});
        localDateTimeList.add(LocalDateTime.now());
        JavaSparkContext spark = new JavaSparkContext(
                new SparkConf().setAppName("MLLIB").setMaster("local[500]").set("spark.executor.memory", "12g").set("spark.driver.maxResultSize", "4g")
        );

        Configuration conf = null;
        Connection connection = null;

        conf = HBaseConfiguration.create();
        conf.clear();
        conf.set("hbase.zookeeper.quorum", "127.0.0.1");
        conf.set("hbase.zookeeper.property.clientPort", "2181");

        connection = ConnectionFactory.createConnection(conf);
        Scan s = new Scan();
//        FilterList filterList = new FilterList(new PageFilter(4000000));
//        s.setFilter(filterList);
        //    s.setStartRow(Bytes.toBytes("t2008-10-23 02:53:151"));
        //    s.setStopRow(Bytes.toBytes("t2008-10-23 02:53:161"));
        conf.set(TableInputFormat.SCAN, convertScanToString(s));


        conf.set(TableInputFormat.INPUT_TABLE, "csvtohbase");
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
        localDateTimeList.add(LocalDateTime.now());
        JavaRDD<Transaction> transactionstsWithRoundedCoordinates = BatchUtils.roundCoordinates(transactions, 10000).cache();
        LocalDateTime minTimestamp = transactionstsWithRoundedCoordinates.min(new TimestampComparator()).getTimestamp();
        LocalDateTime maxTimestamp = transactionstsWithRoundedCoordinates.max(new TimestampComparator()).getTimestamp();
        localDateTimeList.add(LocalDateTime.now());

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
        localDateTimeList.add(LocalDateTime.now());
        FPGrowth fpg = new FPGrowth()
                .setMinSupport(0.01)
                .setNumPartitions(2000);
        FPGrowthModel<Coordinate> model = fpg.run(anTrajectories);


            List<FPGrowthLocalModel> fpGrowths = new ArrayList<>();
//.filter(coordinateFreqItemset -> coordinateFreqItemset.javaItems().size()>38)
            for (FPGrowth.FreqItemset<Coordinate> itemset : model.freqItemsets().toJavaRDD().collect()) {
                fpGrowths.add(new FPGrowthLocalModel( itemset.javaItems() , itemset.freq()));
            }

            try {
                HBaseUtils.addRecord("dataminingupc", "s"+minTimestamp+"e"+maxTimestamp, "data", "", objectMapper.writeValueAsString(fpGrowths));
            } catch (Exception e) {
                e.printStackTrace();
            }
        localDateTimeList.add(LocalDateTime.now());
        List<AssociationRulesLocalModel> associationRulesLocalModels = new ArrayList<>();
        double minConfidence = 0.8;
        for (AssociationRules.Rule<Coordinate> rule
                : model.generateAssociationRules(minConfidence).toJavaRDD().collect()) {
            associationRulesLocalModels.add(new AssociationRulesLocalModel(rule.javaAntecedent(), rule.javaConsequent(), rule.confidence()));
        }
        try {
            HBaseUtils.addRecord("dataminingupc", "r" + minTimestamp + "e" + maxTimestamp, "data", "", objectMapper.writeValueAsString(associationRulesLocalModels));
        } catch (Exception e) {
            e.printStackTrace();
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
