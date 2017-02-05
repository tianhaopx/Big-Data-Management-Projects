package query.Query1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by tianhao on 1/31/17.
 * Write a job(s) that reports the customers whose CountryCode between 2 and 6 (inclusive).
 */
public class Query1 {

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
}
