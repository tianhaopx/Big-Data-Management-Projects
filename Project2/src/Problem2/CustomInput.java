package Problem2;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by test on 2/19/17.
 */
public class CustomInput {

    public static class JSONInput extends FileInputFormat<Text, Text> {
        public RecordReader<Text, Text> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
            JSONRecordReader reader = new JSONRecordReader();
            reader.initialize(split,context);
            return reader;
        }

    }

    public static class JSONRecordReader extends RecordReader<Text, Text> {
        private int count = 0;
        private String temp_value = "";
        private LineRecordReader lineRecordReader = null;
        private Text key;
        private Text value;

        @Override
        public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
            close();
            lineRecordReader = new LineRecordReader();
            lineRecordReader.initialize(split, context);
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            if (!lineRecordReader.nextKeyValue()) {
                key = null;
                value = null;
                return false;
            }

            Text line = lineRecordReader.getCurrentValue();
            String str = line.toString();
            if (str.contains("}") || str.contains("},")) {
                key = new Text(Integer.toString(count));
                value = new Text(temp_value.replaceAll("\"",""));
                count++;
                temp_value = "";
                return true;
            }

            String arr = str.replaceAll("\\s+","");
            if (!arr.equals("{")) {
                temp_value += arr;
            }
            return nextKeyValue();
        }

        @Override
        public void close() throws IOException {
            if (null != lineRecordReader) {
                lineRecordReader.close();
                lineRecordReader = null;
            }
            key = null;
            value = null;
        }

        @Override
        public Text getCurrentKey() throws IOException, InterruptedException {
            return key;
        }

        @Override
        public Text getCurrentValue() throws IOException, InterruptedException {
            return value;
        }

        @Override
        public float getProgress() throws IOException, InterruptedException {
            return lineRecordReader.getProgress();
        }
    }

    public static class JsonMap extends Mapper<Text, Text, Text, Text> {
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            context.write(key,value);

        }
    }
//    public static class JsonReduce extends Reducer<Text, Text, Text, Text> {
//        public void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
//            int sumFemale = 0;
//            int sumMale = 0;
//            while (values.iterator().hasNext()) {
//                if (values.iterator().next().toString().equalsIgnoreCase("female")) {
//                    sumFemale = sumFemale + 1;
//                } else {
//                    sumMale = sumMale + 1;
//                }
//                output.collect(key, new Text("Number of Males: " + sumMale + " ; Number of Females: " + sumFemale));
//            }
//        }
//    }
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "CustomInput");
        job.setJarByClass(CustomInput.class);
        job.setMapperClass(JsonMap.class);
//        job.setReducerClass(JsonReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(JSONInput.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        }


}
