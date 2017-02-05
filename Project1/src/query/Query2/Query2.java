package query.Query2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;


public class Query2 {
    public static class TransactionMapper extends Mapper<LongWritable, Text, Text, Text> {

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] str_value = value.toString().split(",");
            context.write(new Text(str_value[1]), new Text("1"+","+Float.valueOf(str_value[2])));
        }
    }

    public static class SumTransactionReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
            int numOfTrans = 0;
            float sum = 0.0f;
            for (Text str:value) {
                String[] str_value = str.toString().split(",");
                sum += Float.valueOf(str_value[1]);
                numOfTrans += Integer.valueOf(str_value[0]);
            }
            context.write(key, new Text(Integer.toString(numOfTrans)+","+Float.toString(sum)));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("mapred.textoutputformat.separator", ",");
        Job job = Job.getInstance(conf, "Query2");
        job.setJarByClass(Query2.class);
        job.setMapperClass(Query2.TransactionMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setCombinerClass(Query2.SumTransactionReducer.class);
        job.setReducerClass(Query2.SumTransactionReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
