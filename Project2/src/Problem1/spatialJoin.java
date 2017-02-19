package Problem1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Created by test on 2/17/17.
 */
public class spatialJoin {

    public static class SpatialMapper extends Mapper<LongWritable, Text, Text, Text> {
        String ws;
        public void configure(Configuration job) {
            ws = job.get("Window");
        }
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] data = value.toString().split(",");
            if (data.length == 5) {
                int x_1 = Integer.valueOf(data[1]);
                int x_2 = Integer.valueOf(data[3]);
                int y_1 = Integer.valueOf(data[2]);
                int y_2 = Integer.valueOf(data[4]);
                // divide the rectangles to the different regions
                for (int i=(int)Math.floor(x_1/100);i<=(int)Math.floor(x_2/100);i++) {
                    for (int j=(int)Math.floor(y_1/100);j<=(int)Math.floor(y_2/100);j++)
                    context.write(new Text(Integer.toString(i)+"_"+Integer.toString(j)),new Text(value));
                }
            } else {
                // divide the points to the different regions
                int temp_x = (int)Math.floor(Integer.valueOf(data[0])/100);
                int temp_y = (int)Math.floor(Integer.valueOf(data[1])/100);
                context.write(new Text(Integer.toString(temp_x)+"_"+Integer.toString(temp_y)), new Text(value.toString()));
            }
        }
    }

    public static class SpatialCombiner extends Reducer<Text,Text,Text,Text>{
        public void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
            for (Text str:value) {
                String[] str_value = str.toString().split(",");
            }
        }
    }

    public static class SpatialReducer extends Reducer<Text,Text,Text,Text> {
        public void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
            List<String> P = new ArrayList<String>();
            List<String> R = new ArrayList<String>();

            while (value.iterator().hasNext()) {
                String str = value.iterator().next().toString();
                if (str.split(",").length != 2) {
                    R.add(str);
                } else {
                    P.add(str);
                }
            }
//            System.out.println(key.toString());
//            System.out.println(P.size());
//            System.out.println(R.size());
            for (String r:R) {
                String[] temp_r = r.split(",");
                String name = temp_r[0];
                int X_1 = Integer.valueOf(temp_r[1]);
                int Y_1 = Integer.valueOf(temp_r[2]);
                int X_2 = Integer.valueOf(temp_r[3]);
                int Y_2 = Integer.valueOf(temp_r[4]);
                for (String p:P) {
                    String[] temp_p = p.split(",");
                    int x = Integer.valueOf(temp_p[0]);
                    int y = Integer.valueOf(temp_p[1]);
                    if (x>=X_1 && x<=X_2 && y>=Y_1 && y<=Y_2) {
                        context.write(new Text(name),new Text(p));
                    }

                }
            }

        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "spatialJoin");
        job.setJarByClass(spatialJoin.class);
        job.setMapperClass(SpatialMapper.class);
        job.setReducerClass(SpatialReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
