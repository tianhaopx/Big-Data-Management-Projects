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
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            String ws = conf.get("Window");
            int w_x_1 = 0;
            int w_x_2 = 10000;
            int w_y_1 = 0;
            int w_y_2 = 10000;
            if (!(ws.equals("null"))){
                String[] windows = ws.split(",");
                w_x_1 = Integer.valueOf(windows[0]);
                w_x_2 = Integer.valueOf(windows[2]);
                w_y_1 = Integer.valueOf(windows[1]);
                w_y_2 = Integer.valueOf(windows[3]);
            }
            String[] data = value.toString().split(",");
            if (data.length == 5) {
                // x_1 left x_2 right
                int x_1 = Integer.valueOf(data[1]);
                int x_2 = Integer.valueOf(data[1])+Integer.valueOf(data[3]);
                // y_1 down y_2 up
                int y_1 = Integer.valueOf(data[2])-Integer.valueOf(data[4]);
                int y_2 = Integer.valueOf(data[2]);
                // divide the rectangles to the different regions
                for (int i=(int)Math.floor(x_1/100);i<=(int)Math.floor(x_2/100);i++) {
                    for (int j=(int)Math.floor(y_1/100);j<=(int)Math.floor(y_2/100);j++)
                        if (x_1 >= w_x_1 && x_1 <= w_x_2 && y_1 >= w_y_1 && y_1 <= w_y_2) {
                            // down left rectangle inside the window
                            context.write(new Text(Integer.toString(i)+"_"+Integer.toString(j)),new Text(value.toString()));
                        }else if (x_1 >= w_x_1 && x_1 <= w_x_2 && y_2 >= w_y_1 && y_2 <= w_y_2){
                            // up left rectangle inside the window
                            context.write(new Text(Integer.toString(i)+"_"+Integer.toString(j)),new Text(value.toString()));
                        }else if (x_2 >= w_x_1 && x_2 <= w_x_2 && y_1 >= w_y_1 && y_1 <= w_y_2){
                            // down right rectangle inside the window
                            context.write(new Text(Integer.toString(i)+"_"+Integer.toString(j)),new Text(value.toString()));
                        }else if (x_2 >= w_x_1 && x_2 <= w_x_2 && y_2 >= w_y_1 && y_2 <= w_y_2){
                            // down right rectangle inside the window
                            context.write(new Text(Integer.toString(i)+"_"+Integer.toString(j)),new Text(value.toString()));
                        }else if (x_1 <= w_x_1 && x_2 >= w_x_2 && y_1 <= w_y_1 && y_2 >= w_y_2) {
                            // window inside the rectangle
                            context.write(new Text(Integer.toString(i)+"_"+Integer.toString(j)),new Text(value.toString()));
                        }
                }
            } else {
                // divide the points to the different regions
                int temp_x = (int)Math.floor(Integer.valueOf(data[0])/100);
                int temp_y = (int)Math.floor(Integer.valueOf(data[1])/100);
                if (Integer.valueOf(data[0]) >= w_x_1 && Integer.valueOf(data[0]) <= w_x_2 && Integer.valueOf(data[1]) >= w_y_1 && Integer.valueOf(data[1]) <= w_y_2) {
                    context.write(new Text(Integer.toString(temp_x) + "_" + Integer.toString(temp_y)), new Text(value.toString()));
                }
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
            for (String r:R) {
                String[] temp_r = r.split(",");
                String name = temp_r[0];
                int X_1 = Integer.valueOf(temp_r[1]);
                int Y_1 = Integer.valueOf(temp_r[2])-Integer.valueOf(temp_r[4]);
                int X_2 = Integer.valueOf(temp_r[1])+Integer.valueOf(temp_r[3]);
                int Y_2 = Integer.valueOf(temp_r[2]);
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
        if (args.length == 7) {
            conf.set("Window",args[3]+","+args[4]+","+args[5]+","+args[6]);
        } else {
            conf.set("Window","null");
        }
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
