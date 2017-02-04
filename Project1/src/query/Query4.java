package query;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;


public class Query4 {

    // for each mapper
    // we can construct the output value as the following
    // key: custID
    // value: country id, TransCount, TransMoney
    // for the combiner
    // key: country ID
    // value: customer id, TransCount, Min Trans, Max Trans
    // if the mapper do not contain the message we need
    // we set the value into null or 0

    public static class CustomerMapper extends Mapper<LongWritable, Text, Text, Text> {

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] customer = value.toString().split(",");
            context.write(new Text(customer[0]), new Text(customer[3]+",0,1000.0,10.0"));
        }
    }

    public static class TransactionMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] str_value = value.toString().split(",");
            context.write(new Text(str_value[1]), new Text("0,1,"+Float.valueOf(str_value[2])+","+Float.valueOf(str_value[2])));
        }
    }

    public static class SumTransactionAndSwitchReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
            int numOfTrans = 0;
            float min_sum = 1000.0f;
            float max_sum =10.0f;
            String ccode = "0";
            for (Text str:value) {
                String[] str_value = str.toString().split(",");
                ccode = (str_value[0].equals("0")) ? ccode:str_value[0];
                numOfTrans += Integer.valueOf(str_value[1]);
                min_sum = Math.min(min_sum,Float.valueOf(str_value[2]));
                max_sum = Math.max(max_sum,Float.valueOf(str_value[3]));
            }
            context.write(key, new Text(ccode+","+Integer.toString(numOfTrans)+","+Float.toString(min_sum)+","+Float.toString(max_sum)));
        }
    }

    public static class JoinReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
            int numOfTrans = 0;
            float min_sum = 1000.0f;
            float max_sum =10.0f;
            String new_key = "";
            for (Text str:value) {
                String[] str_value = str.toString().split(",");
                numOfTrans += Integer.valueOf(str_value[1]);
                min_sum = Math.min(min_sum,Float.valueOf(str_value[2]));
                max_sum = Math.max(max_sum,Float.valueOf(str_value[2]));
                new_key = str_value[0];
            }
            context.write(new Text(new_key), new Text(key.toString()+","+Integer.toString(numOfTrans)+","+Float.toString(min_sum)+","+Float.toString(max_sum)));
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("mapred.textoutputformat.separator", ",");
        Job job = Job.getInstance(conf, "Query4");
        job.setJarByClass(Query4.class);
        job.setMapperClass(Query4.CustomerMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setCombinerClass(Query4.SumTransactionAndSwitchReducer.class);
        job.setReducerClass(Query4.SumTransactionAndSwitchReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, TransactionMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, CustomerMapper.class);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
