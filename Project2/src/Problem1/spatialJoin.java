package Problem1;

import java.util.*;
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
    static int flag = 0;
    static int width_1 = 0;
    static int width_2 = 0;
    static int height_1 = 0;
    static int height_2 = 0;

    public static class SpatialMapper extends Mapper<LongWritable, Text, Text, Text> {
        String ws;
        public void configure(Configuration job) {
            ws = job.get("Window");
        }
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] data = value.toString().split(",");
            if (data.length == 5) {
                context.write(new Text("-1"),new Text(value));
            } else {
                int temp_x = Integer.valueOf(data[0]);
                int temp_y = Integer.valueOf(data[1]);
                if (temp_x % 5000 == temp_x && temp_y % 5000 == temp_y) {
                    context.write(new Text("0"), new Text(value.toString()));
                } else if (temp_x % 5000 != temp_x && temp_y % 5000 == temp_y) {
                    context.write(new Text("1"), new Text(value.toString()));
                } else if (temp_x % 5000 == temp_x && temp_y % 5000 != temp_y) {
                    context.write(new Text("2"), new Text(value.toString()));
                } else {
                    context.write(new Text("3"), new Text(value.toString()));
                }
            }
        }
    }

    public static class SpatialReducer extends Reducer<Text,Text,Text,Text> {
        static List<String> P = new ArrayList<String>();
        static List<String> R = new ArrayList<String>();
        public void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
            while (value.iterator().hasNext()) {
                String str = value.iterator().next().toString();
                if (str.split(",").length != 2) {
                    R.add(str);
                } else {

                    P.add(str);
                }
            }
            System.out.println(R.size());
            System.out.println(P.size());
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
                    if (x>X_1 && x<X_2 && y>Y_1 && y<Y_2) {
                        context.write(new Text(name),new Text(p));
                    }

                }
            }

        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length == 3) {
            // do not contain the window
            flag = -1;
        } else {
            flag = 1;
            // contain the window
            width_1 = Integer.valueOf(args[3]);
            width_2 = Integer.valueOf(args[4]);
            height_1 = Integer.valueOf(args[5]);
            height_2 = Integer.valueOf(args[6]);
        }
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
