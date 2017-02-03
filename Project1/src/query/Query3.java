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


public class Query3 {

    // for eahc mapper
    // we can construct the output value as the following
    // key: custID
    // value: custName, custAge, TransCount, TransMoneyCount, TransItem
    // if the mapper do not contain the message we need
    // we set the value into null or 0

    public static class CustomerMapper extends Mapper<LongWritable, Text, Text, Text> {

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] customer = value.toString().split(",");
            context.write(new Text(customer[0]), new Text(customer[1]+","+customer[2]+",0,0,20"));
        }
    }

    public static class TransactionMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] str_value = value.toString().split(",");
            context.write(new Text(str_value[1]), new Text("null,0,1"+","+Float.valueOf(str_value[2])+","+Integer.valueOf(str_value[3])));
        }
    }

    public static class SumTransactionReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
            String cname = "null";
            int cage = 0;
            int numOfTrans = 0;
            float sum = 0.0f;
            int numOfItem = 20;

            for (Text str:value) {
                String[] str_value = str.toString().split(",");
                cname = (str_value[0]== "null") ? "null":str_value[0];
                cage = Integer.valueOf(str_value[1]);
                numOfTrans += Integer.valueOf(str_value[2]);
                sum += Float.valueOf(str_value[3]);
                numOfItem = Math.min(numOfItem,Integer.valueOf(str_value[4]));
            }
            context.write(key, new Text(cname+","+Integer.toString(cage)+","+Integer.toString(numOfTrans)+","+Float.toString(sum)+","+Integer.toString(numOfItem)));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("mapred.textoutputformat.separator", ",");
        Job job = Job.getInstance(conf, "Query3");
        job.setJarByClass(Query3.class);
        job.setMapperClass(Query3.CustomerMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setCombinerClass(Query3.SumTransactionReducer.class);
        job.setReducerClass(Query3.SumTransactionReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, CustomerMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, TransactionMapper.class);
        
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
