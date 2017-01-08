package edu.upc.bip.Tests;


        import org.apache.hadoop.conf.Configuration;
        import org.apache.hadoop.fs.Path;
        import org.apache.hadoop.hbase.HBaseConfiguration;
        import org.apache.hadoop.hbase.KeyValue;
        import org.apache.hadoop.hbase.client.HTable;
        import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
        import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
        import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
        import org.apache.hadoop.mapreduce.Job;
        import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
        import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
        import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
        import org.apache.hadoop.util.GenericOptionsParser;

/**
 * HBase bulk import example<br>
 * Data preparation MapReduce job driver
 * <ol>
 * <li>args[0]: HDFS input path
 * <li>args[1]: HDFS output path
 * <li>args[2]: HBase table name
 * </ol>
 */
public class Driver {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        conf = HBaseConfiguration.create();
        conf.clear();
        conf.set("hbase.zookeeper.quorum", "127.0.0.1");
        conf.set("hbase.zookeeper.property.clientPort","2181");
        conf.set("hbase.table.name", "csvtohbase2");


        Job job = new Job(conf, "HBase Bulk Import Example");
        job.setJarByClass(HBaseKVMapper.class);

        job.setMapperClass(HBaseKVMapper.class);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(KeyValue.class);

        job.setInputFormatClass(TextInputFormat.class);

        HTable hTable = new HTable(conf,"csvtohbase2");

        // Auto configure partitioner and reducer
        HFileOutputFormat.configureIncrementalLoad(job, hTable);

        FileInputFormat.addInputPath(job, new Path("/home/osboxes/data_beiging_4hours"));
        //FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
