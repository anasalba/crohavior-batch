package edu.upc.bip.utils;


import edu.upc.bip.model.Coordinate;
import edu.upc.bip.model.Transaction;
import edu.upc.bip.utils.HBaseUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.codehaus.jackson.map.ObjectMapper;
import scala.Tuple2;

import java.io.IOException;
import java.io.OutputStream;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by osboxes on 08/12/16.
 */
public class BatchUtils {

    /**
     * Maps the measurements by rounding the coordinate.
     * The world is defined by a grid of boxes, each box has a size of 0.0005 by 0.0005.
     * Every mapping will be rounded to the center of the box it is part of.
     * Boundary cases will be rounded up, so a coordinate on (-0.00025,0) will be rounded to (0,0),
     * while the coordinate (0.00025,0) will be rounded to (0.0005,0).
     *
     * @param measurements | The dataset of measurements
     * @return A set of measurements with rounded coordinates
     */
    public static JavaRDD<Transaction> roundCoordinates(JavaRDD<Transaction> measurements, double roundFactor) {
        return measurements.map(
                new Function<Transaction, Transaction>() {
                    @Override
                    public Transaction call(Transaction measurement) throws Exception {

                        double roundedLatitude = (double) (Math.round((measurement.getCoordinate().getLatitude() * roundFactor))) / roundFactor;
                        double roundedLongitude = (double) (Math.round((measurement.getCoordinate().getLongitude() * roundFactor))) / roundFactor;
                        Coordinate roundedCoordinate = new Coordinate(roundedLatitude, roundedLongitude);
                        measurement.setRoundedCoordinate(roundedCoordinate);
                        return measurement;
                    }
                }
        );
    }

    /**
     * Filter the measurements in a given time period
     *
     * @param transactions | The dataset of measurements
     * @param start | Start of the time period
     * @param end   | End of the time period
     * @return A set of measurements in the given time period
     */
    public static JavaRDD<Transaction> filterByTime(JavaRDD<Transaction> transactions, LocalDateTime start, LocalDateTime end) {
        return transactions.filter(
                new Function<Transaction, Boolean>() {
                    @Override
                    public Boolean call(Transaction transaction) throws Exception {
                        return (transaction.getTimestamp().isEqual(start) || transaction.getTimestamp().isAfter(start))
                                && transaction.getTimestamp().isBefore(end);
                    }
                }
        );
    }

    /**
     * Reduces the dataset by counting the number of transactions for a specific grid box (rounded coordinate)
     *
     * @param transactions | The dataset of transactions
     * @return A set of tuples linking rounded coordinates to their number of occurrences
     */
    public static JavaPairRDD<Coordinate, Integer> countPerGridBox(JavaRDD<Transaction> transactions) {
        return transactions.mapToPair(
                new PairFunction<Transaction, Coordinate, Integer>() {
                    @Override
                    public Tuple2<Coordinate, Integer> call(Transaction transaction) throws Exception {
                        return new Tuple2<Coordinate, Integer>(transaction.getRoundedCoordinate(), 1);
                    }
                }
        ).reduceByKey(
                new Function2<Integer, Integer, Integer>() {
                    @Override
                    public Integer call(Integer a, Integer b) throws Exception {
                        return a + b;
                    }
                }
        );
    }

    /**
     * Write the result as JSON to the given outputstream
     * @param tuples | The dataset of rounded coordinates with their number of occurrences
     * @param objectMapper | ObjectMapper to map a Java object to a JSON string
     * @param outputStream | Outputstream to write the JSON to
     * @throws IOException
     */
    public static void writeJson(JavaPairRDD<Coordinate, Integer> tuples, ObjectMapper objectMapper, OutputStream outputStream) throws IOException {
        List<Map<String, Object>> gridBoxes = tuples.map(
                new Function<Tuple2<Coordinate, Integer>, Map<String, Object>>() {
                    @Override
                    public Map<String, Object> call(Tuple2<Coordinate, Integer> tuple) throws Exception {
                        Coordinate coordinate = tuple._1();
                        Map<String, Object> gridBox = new HashMap<>();
                        gridBox.put("a", coordinate.getLatitude());
                        gridBox.put("o", coordinate.getLongitude());
                        gridBox.put("c", tuple._2());
                        return gridBox;
                    }
                }
        ).collect();

        Map<String, Object> data = new HashMap<>();
        data.put("data", gridBoxes);
        objectMapper.writeValue(outputStream, data);
        outputStream.close();
    }

    /**
     * Write the result as JSON to the given outputstream
     * @param tuples | The dataset of rounded coordinates with their number of occurrences
     * @throws IOException
     */
    public static void writeJsonToHbase(JavaPairRDD<Coordinate, Integer> tuples, String tablename, String family, String rowID,ObjectMapper objectMapper) throws IOException {
        List<Map<String, Object>> gridBoxes = tuples.map(
                new Function<Tuple2<Coordinate, Integer>, Map<String, Object>>() {
                    @Override
                    public Map<String, Object> call(Tuple2<Coordinate, Integer> tuple) throws Exception {
                        Coordinate coordinate = tuple._1();
                        Map<String, Object> gridBox = new HashMap<>();
                        gridBox.put("a", coordinate.getLatitude());
                        gridBox.put("o", coordinate.getLongitude());
                        gridBox.put("c", tuple._2());
                        return gridBox;
                    }
                }
        ).collect();

        Map<String, Object> data = new HashMap<>();
        data.put("data", gridBoxes);

        try {
            HBaseUtils.addRecord(tablename, rowID, family, "", objectMapper.writeValueAsString(data));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
