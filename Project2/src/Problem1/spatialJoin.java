package Problem1;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Created by test on 2/17/17.
 */
public class spatialJoin {
    public static class CustomerMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] customer = value.toString().split(",");
            if (Integer.valueOf(customer[3]) >= 2 && Integer.valueOf(customer[3]) <= 6) {
                context.write(value, NullWritable.get());
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Query1");
        job.setJarByClass(Query1.class);
        job.setMapperClass(CustomerMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
