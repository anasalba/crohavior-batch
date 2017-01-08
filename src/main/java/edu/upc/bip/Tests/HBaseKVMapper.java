package edu.upc.bip.Tests;


        import java.io.IOException;
        import java.util.Locale;

        import org.apache.hadoop.conf.Configuration;
        import org.apache.hadoop.hbase.KeyValue;
        import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
        import org.apache.hadoop.hbase.util.Bytes;
        import org.apache.hadoop.io.LongWritable;
        import org.apache.hadoop.io.Text;
        import org.apache.hadoop.mapreduce.Mapper;

        import au.com.bytecode.opencsv.CSVParser;

/**
 * HBase bulk import example
 * <p>
 * Parses Facebook and Twitter messages from CSV files and outputs
 * <ImmutableBytesWritable, KeyValue>.
 * <p>
 * The ImmutableBytesWritable key is used by the TotalOrderPartitioner to map it
 * into the correct HBase table region.
 * <p>
 * The KeyValue value holds the HBase mutation information (column family,
 * column, and value)
 */
public class HBaseKVMapper extends
        Mapper<LongWritable, Text, ImmutableBytesWritable, KeyValue> {

    final static byte[] SRV_COL_FAM = "data".getBytes();

    CSVParser csvParser = new CSVParser();
    int tipOffSeconds = 0;
    String tableName = "";

    ImmutableBytesWritable hKey = new ImmutableBytesWritable();
    KeyValue kv;

    /** {@inheritDoc} */
    @Override
    protected void setup(Context context) throws IOException,
            InterruptedException {
        Configuration c = context.getConfiguration();

        tipOffSeconds = c.getInt("epoch.seconds.tipoff", 0);
        tableName = c.get("hbase.table.name");
    }

    /** {@inheritDoc} */
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        // Skip header
        if (value.find("lat") > -1) {
            return;
        }

        String[] fields = null;

        try {
            fields = csvParser.parseLine(value.toString());
        } catch (Exception ex) {
            context.getCounter("HBaseKVMapper", "PARSE_ERRORS").increment(1);
            return;
        }

        // Key: e.g. "1200:twitter:jrkinley"
        hKey.set(Bytes.toBytes(fields[4]+fields[7]));

        // Service columns
        if (!fields[0].equals("")) {
            kv = new KeyValue(hKey.get(), SRV_COL_FAM,
                    "".getBytes(), (fields[0]+";"+fields[1]+";"+fields[4]+";"+fields[7]).getBytes());
            context.write(hKey, kv);
        }
    }
}
