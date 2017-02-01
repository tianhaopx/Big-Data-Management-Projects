package query;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;


public class Query2 {
    public static class TransactionMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {
        private FloatWritable result = new FloatWritable();
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] str_value = value.toString().split(",");
            result.set(Float.valueOf(str_value[2]));
            context.write(new Text(str_value[1]),result);
        }
    }

    public static class SumTransactionReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {
        private FloatWritable result = new FloatWritable();
        public void reduce(Text key, Iterable<FloatWritable> value, Context context) throws IOException, InterruptedException {
            float sum = 0.0f;
            for (FloatWritable val:value) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key,result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Query2");
        job.setJarByClass(Query2.class);
        job.setMapperClass(Query2.TransactionMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);
        job.setCombinerClass(Query2.SumTransactionReducer.class);
        job.setReducerClass(Query2.SumTransactionReducer.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
