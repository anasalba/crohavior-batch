package edu.upc.bip.batch;

import edu.upc.bip.model.Coordinate;
import edu.upc.bip.model.Project;
import edu.upc.bip.utils.HBaseUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by osboxes on 30/12/16.
 */
public class ProjectTest {

    public static void main(String[] args) {


        try {
            String tablename = "projects";
            String[] familys = { "project"};
            HBaseUtils.deleteTable(tablename);
            HBaseUtils.creatTable(tablename, familys);

            List<Coordinate> coordinateList = new ArrayList<>();
            coordinateList.add(new Coordinate(1,1));
            coordinateList.add(new Coordinate(1,2));
            coordinateList.add(new Coordinate(1,3));
            coordinateList.add(new Coordinate(2,1));
            coordinateList.add(new Coordinate(2,2));
            Project project = new Project("Ward Taye",1,1,coordinateList);

            HBaseUtils.addRecord(tablename, "u1p1", "project", "", project.toJson());
//            HBaseUtils2.addRecord(tablename, "baoniu", "course", "math", "89");
//
//            System.out.println("===========get one record========");
            String s= HBaseUtils.getOneRecordValue(tablename, "u1p1");
            Project p = new Project();
            p.toObject(s);
            int asd =3;
//
//            System.out.println("===========show all record========");
//            HBaseUtils2.getAllRecord(tablename);
//
//            System.out.println("===========del one record========");
//            HBaseUtils2.delRecord(tablename, "baoniu");
//            HBaseUtils2.getAllRecord(tablename);
//
//            System.out.println("===========show all record========");
//            HBaseUtils2.getAllRecord(tablename);

//            SparkConf sparkConf = new SparkConf().setAppName("JavaHBaseBulkGetExample ");
//            JavaSparkContext jsc = new JavaSparkContext(sparkConf);
//            Configuration conf = HBaseConfiguration.create();
//            JavaHBaseContext hbaseContext = new JavaHBaseContext(jsc, conf);
//            hbaseContext.hbaseRDD("sdfsdf", new Scan());
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}