package edu.upc.bip.Integrated;

import edu.upc.bip.batch.BatchMllibNEW;
import edu.upc.bip.batch.BatchNEW;
import edu.upc.bip.model.Operation;
import edu.upc.bip.utils.HBaseUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Created by osboxes on 19/12/16.
 */
public class Batch {
    public static void main(String args[]) throws Exception
    {
        int i=0;
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        Date date = new Date();
/*        HBaseUtils.deleteTable("heatmap");
        HBaseUtils.creatTable("heatmap", new String[] {"data"});
        HBaseUtils.deleteTable("datamining");
        HBaseUtils.creatTable("datamining", new String[] {"data"});*/

        //HBaseUtils.creatTable("operations", new String[] {"data"});
        Timer timer = new Timer ();
        TimerTask hourlyTask = new TimerTask () {
            @Override
            public void run (){
                JavaSparkContext sc = new JavaSparkContext(
                        new SparkConf().setAppName("JavaWordCount").setMaster("local[200]").set("spark.executor.memory","8g").set("spark.driver.maxResultSize","2g")
                );
                ExecutorService executorService = Executors.newFixedThreadPool(2);
                LocalDateTime batchStart = LocalDateTime.now().minusHours(1);
                String start = "t"+ formatter.format(batchStart);
                LocalDateTime batchEnd = LocalDateTime.now();
                String end = "t"+ formatter.format(batchEnd);
                // Start thread 1
                Future<Long> future1 = executorService.submit(new Callable<Long>() {
                    @Override
                    public Long call() throws Exception {

                        BatchNEW batchNEW = new BatchNEW();
                        batchNEW.run(sc,start,end);
                        return 0L;
                    }
                });
                // Start thread 2
                Future<Long> future2 = executorService.submit(new Callable<Long>() {
                    @Override
                    public Long call() throws Exception {
//                        JavaRDD<String> file2 = sc.textFile("/path/to/test_doc2");
                        BatchMllibNEW batchNEW = new BatchMllibNEW();
                        batchNEW.run(sc,start,end);

                        List<Operation> operations = Operation.getAllOperations();
                        for(Operation operation:operations)
                        {
                            Operation.deleteOperation("s"+operation.getToStart()+"e"+operation.getToEnd());
                            batchNEW.run(sc,"t"+operation.getToStart().replace('T',' '),"t"+operation.getToEnd().replace('T',' '));
                        }

                        return 1L;
                    }
                });
                // Wait thread 1
                try {
                System.out.println("File1:"+future1.get());
                // Wait thread 2
                System.out.println("File2:"+future2.get());
                }catch (Exception e){

                }
                sc.close();
            }
        };

// schedule the task to run starting now and then every hour...
        timer.schedule (hourlyTask, 0l, 1000*60*60);
    }
}
