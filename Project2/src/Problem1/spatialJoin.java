package Problem1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.hash.Hash;

/**
 * Created by test on 2/17/17.
 */
public class spatialJoin {
    static int flag = 0;
    static int width_1 = 0;
    static int width_2 = 0;
    static int height_1 = 0;
    static int height_2 = 0;
    static List<String> R = new ArrayList<String>();
    static List<String> P = new ArrayList<String>();
    public static class SpatialMapper extends Mapper<LongWritable, Text, Text, Text> {

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] data = value.toString().split(",");
            int n = 0;
            if (flag == -1) {
                System.out.println(data.length);
                if (data.length== 9) {
                    // examine whether there are point belong to this rectangle
                    for (String p:P) {
                        String[] temp = p.toString().split(",");
                        if (Integer.valueOf(temp[0]) >= Integer.valueOf(data[1]) && Integer.valueOf(temp[0]) <= Integer.valueOf(data[3])){
                            if (Integer.valueOf(temp[1]) >= Integer.valueOf(data[2]) && Integer.valueOf(temp[1]) <= Integer.valueOf(data[6])){
                                context.write(new Text(data[0]),new Text(p));
                            }
                        }
                    }
                    // add rectangle to the Hashmap
                    R.add(data[0]+","+data[1]+","+data[2]+","+data[3]+","+data[4]+","+data[5]+","+data[6]+","+data[7]+","+data[8]);
                } else {
                    // exam point whether belong to the rectangle
                    for (String r:R) {
                        String[] temp = r.toString().split(",");
                        if (Integer.valueOf(data[0]) >= Integer.valueOf(temp[1]) && Integer.valueOf(data[0]) <= Integer.valueOf(temp[3])){
                            if (Integer.valueOf(data[1]) >= Integer.valueOf(temp[2]) && Integer.valueOf(data[1]) <= Integer.valueOf(temp[6])){
                                context.write(new Text(temp[0]),new Text(data[0]+","+data[1]));
                            }
                        }
                    }
                    // if not add to the list
                    P.add(data[0]+","+data[1]);
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
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
