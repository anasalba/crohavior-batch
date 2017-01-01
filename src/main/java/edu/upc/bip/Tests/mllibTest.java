package edu.upc.bip.Tests;

/**
 * Created by osboxes on 27/11/16.
 */

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import edu.upc.bip.model.Coordinate;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.fpm.*;

public class mllibTest {



    public static void main(String[] agrs) {
        JavaSparkContext spark = new JavaSparkContext(
                new SparkConf().setAppName("JavaWordCount").setMaster("local[200]").set("spark.executor.memory","8g").set("spark.driver.maxResultSize","2g")
        );
        JavaRDD<String> data = spark.parallelize(Arrays.asList(
                "r z h k p",
                "z y x w v u t s",
                "s x o n r",
                "x z y m t s q e",
                "z",
                "x z y r q t p"
        ));

        Coordinate c0 = new Coordinate(1,2);
        Coordinate c1 = new Coordinate(2,3);
        Coordinate c2 = new Coordinate(2,2);
        Coordinate c3 = new Coordinate(3,2);
        Coordinate c4 = new Coordinate(4,2);
        Coordinate c5 = new Coordinate(5,2);
        Coordinate c6 = new Coordinate(3,3);
        Coordinate c7 = new Coordinate(4,3);
        Coordinate c8 = new Coordinate(5,6);
        Coordinate c9 = new Coordinate(1,6);
        List<Coordinate> t1 = new ArrayList<>();
        t1.add(c0);t1.add(c1);t1.add(c2);t1.add(c3);
        List<Coordinate> t2 = new ArrayList<>();
        t2.add(c4);t2.add(c5);t2.add(c6);t2.add(c7);
        List<Coordinate> t3 = new ArrayList<>();
        t3.add(c2);t3.add(c3);t3.add(c4);t3.add(c5);
        List<Coordinate> t4 = new ArrayList<>();
        t4.add(c1);t4.add(c2);t4.add(c9);t4.add(c6);
        List<Coordinate> t5 = new ArrayList<>();
        t5.add(c1);t5.add(c2);t5.add(c7);t5.add(c8);
        JavaRDD<List<Coordinate>> transactions1 = spark.parallelize(Arrays.asList(
                t1,t2,t3,t4,t5
        ));

        JavaRDD<List<String>> transactions = data.map(
                new Function<String, List<String>>() {
                    public List<String> call(String line) {
                        String[] parts = line.split(" ");
                        return Arrays.asList(parts);
                    }
                }
        );

        FPGrowth fpg = new FPGrowth()
                .setMinSupport(0.2)
                .setNumPartitions(10);
        FPGrowthModel<Coordinate> model = fpg.run(transactions1);

        for (FPGrowth.FreqItemset<Coordinate> itemset : model.freqItemsets().toJavaRDD().collect()) {
            System.out.println("" + itemset.javaItems() + ", " + itemset.freq());
        }

        double minConfidence = 0.8;
        for (AssociationRules.Rule<Coordinate> rule
                : model.generateAssociationRules(minConfidence).toJavaRDD().collect()) {
            System.out.println(
                    rule.javaAntecedent() + " => " + rule.javaConsequent() + ", " + rule.confidence());
        }

//        PrefixSpan prefixSpan = new PrefixSpan()
//                .setMinSupport(0.5)
//                .setMaxPatternLength(5);
//        PrefixSpanModel<Integer> model1 = prefixSpan.run(transactions1);
//        for (PrefixSpan.FreqSequence<Integer> freqSeq: model1.freqSequences().toJavaRDD().collect()) {
//            System.out.println(freqSeq.javaSequence() + ", " + freqSeq.freq());
//        }
    }

}
