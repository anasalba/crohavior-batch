package edu.upc.bip.Tests;

import edu.upc.bip.model.Operation;
import edu.upc.bip.utils.HBaseUtils;

import java.io.IOException;
import java.io.Serializable;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Created by osboxes on 30/11/16.
 */
public class dummytests implements Serializable{

    public static void main(String[] args) throws Exception {

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        LocalDateTime date = LocalDateTime.now().minusHours(1);
        System.out.println(formatter.format(date));

/*        HBaseUtils.creatTable("operations", new String[] {"data"});

        Operation.addToOperationsTable("2008-10-23T02:57:40", "2008-10-23T02:59:30",0.05,0.8);
        Operation.addToOperationsTable("2008-10-23T02:59:15", "2008-10-23T03:05:15",0.05,0.8);*/

        List<Operation> operations = Operation.getAllOperations();
        System.out.println(operations.size());
/*        DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
        Date start = formatter.parse("2008-10-23T02:53:15");
        Date end = formatter.parse("2008-10-23T04:34:50");
        System.out.println("r"+formatter.format(start)+"e"+formatter.format(end));
        System.out.println(HBaseUtils.getOneRecordValue("datamining","r"+formatter.format(start)+"e"+formatter.format(end)));*/


//        LocalDateTime time = LocalDateTime.parse("2008-10-23 02:53:15", formatter);
//
//        JavaSparkContext sc = new JavaSparkContext(
//                new SparkConf().setAppName("JavaWordCount").setMaster("local[200]").set("spark.executor.memory","8g").set("spark.driver.maxResultSize","2g")
//        );
//        BatchMllibNEW batchNEW = new BatchMllibNEW();
//        batchNEW.run(sc);

    }



}