package query.Query5;

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

import java.io.*;
import java.util.HashMap;


public class Query5 {

    // for eahc mapper
    // we can construct the output value as the following
    // key: custID
    // value: custName, custAge, TransCount, TransMoneyCount, TransItem
    // if the mapper do not contain the message we need
    // we set the value into null or 0

    private static HashMap<String, String> customMap = new HashMap<>();

    public static class CustomerCountMapper extends Mapper<LongWritable, Text, Text, Text> {

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] strings = value.toString().split(",");
            customMap.put(strings[0], strings[1]);
            context.write(new Text("-1"), new Text("1,0"));
        }
    }

    public static class TranscationCountMapper extends Mapper<LongWritable, Text, Text, Text> {

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] str_value = value.toString().split(",");
            context.write(new Text("-1"), new Text("0,1"));
            context.write(new Text(str_value[1]), new Text("1"));
        }
    }


    public static class SumAllReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
            int numsofCCount = 0;
            int numsofTCount = 0;
            int numOfTrans = 0;

            if (key.toString().equals("-1")) {
                for (Text str : value) {
                    String[] str_value = str.toString().split(",");
                    numsofCCount += Integer.valueOf(str_value[0].toString());
                    numsofTCount += Integer.valueOf(str_value[1].toString());
                }
            } else {
                for (Text str : value) {
                    numOfTrans += Integer.valueOf(str.toString());
                }
            }

            if (key.toString().equals("-1")) {
                context.write(key, new Text(Integer.toString(numsofCCount) + "," + Integer.toString(numsofTCount)));
            } else {
                context.write(key, new Text(Integer.toString(numOfTrans)));
            }

        }
    }

    public static class SumAllReducer_2 extends Reducer<Text, Text, Text, Text> {

        static int avg;

        public void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
            int numsofCCount = 0;
            int numsofTCount = 0;
            int numOfTrans = 0;
            if (key.toString().equals("-1")) {
                for (Text str : value) {
                    String[] str_value = str.toString().split(",");
                    numsofCCount += Integer.valueOf(str_value[0].toString());
                    numsofTCount += Integer.valueOf(str_value[1].toString());
                }
                avg = numsofTCount / numsofCCount;
                System.out.println(avg);
            } else {
                for (Text str : value) {
                    numOfTrans += Integer.valueOf(str.toString());
                }
            }

            if (key.toString().equals("-1")) {
                ;
            } else {
                if (numOfTrans >= avg) {
                    context.write(new Text(customMap.get(key.toString())), new Text(Integer.toString(numOfTrans)));
                }
            }

        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        conf.set("mapred.textoutputformat.separator", ",");
        Job job = Job.getInstance(conf, "Query5");
        job.setJarByClass(Query5.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setCombinerClass(Query5.SumAllReducer.class);
        job.setReducerClass(Query5.SumAllReducer_2.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);


        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, TranscationCountMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, CustomerCountMapper.class);

        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
