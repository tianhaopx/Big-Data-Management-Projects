package Problem3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Created by test on 2/19/17.
 */
public class kMeans {
    public static class kMeansMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            String[] c1 = conf.get("C1").split(",");
            String[] c2 = conf.get("C2").split(",");
            String[] data = value.toString().split(",");
            double c1_x = Double.valueOf(c1[0]);
            double c1_y = Double.valueOf(c1[1]);
            double c2_x = Double.valueOf(c2[0]);
            double c2_y = Double.valueOf(c2[1]);
            double cur_x = Double.valueOf(data[0]);
            double cur_y = Double.valueOf(data[1]);
            double dis1 = Math.sqrt(Math.pow(cur_x - c1_x,2)+Math.pow(cur_y - c1_y,2));
            double dis2 = Math.sqrt(Math.pow(cur_x - c2_x,2)+Math.pow(cur_y - c2_y,2));
            if (dis1 > dis2) {
                context.write(new Text(conf.get("C1")),new Text(value));
            } else {
                context.write(new Text(conf.get("C2")),new Text(value));
            }

        }
    }

    public static class kMeansCombiner extends Reducer<Text,Text,Text,Text> {
        public void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
            int mean_x = 0;
            int mean_y = 0;
            int count = 0;
            for (Text str:value) {
                String[] data = str.toString().split(",");
                mean_x += Double.valueOf(data[0]);
                mean_y += Double.valueOf(data[1]);
                count += 1;
            }
            context.write(key, new Text(Double.toString(mean_x/count)+","+Double.toString(mean_y/count)));
        }
    }

    public static class kMeansReducer extends Reducer<Text,Text,Text,NullWritable> {
        public void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
            int mean_x = 0;
            int mean_y = 0;
            int count = 0;
            for (Text str:value) {
                String[] data = str.toString().split(",");
                mean_x += Double.valueOf(data[0]);
                mean_y += Double.valueOf(data[1]);
                count += 1;
            }
            context.write(new Text(Double.toString(mean_x/count)+","+Double.toString(mean_y/count)), NullWritable.get());
        }
    }

    public static List<String> getRandomCentroids(String f) throws Exception{
        List<String> temp = new ArrayList<String>();
        List<String> ret = new ArrayList<String>();
        BufferedReader br = new BufferedReader(new FileReader(f));
        String line;
        while ((line = br.readLine())!=null) {
            temp.add(line);
        }
        Random centroids = new Random();
        for (int i=1;i<=2;i++) {
            int index = centroids.nextInt(temp.size());
            ret.add(temp.get(index));
        }
        temp = null;
        return ret;
    }

    public static List<String> getCentroids(String f) throws Exception{
        List<String> ret = new ArrayList<String>();
        BufferedReader br = new BufferedReader(new FileReader(f));
        String line;
        while ((line = br.readLine())!=null) {
            ret.add(line);
        }
        return ret;
    }

    public static void main(String[] args) throws Exception{
        String c1;
        String c2;
        String prev_x = "";
        String prev_y = "";
        String input = args[0];
        String output = args[1];
        String file_name = "part-r-00000";
        boolean converge = false;
        for (int i=1;i<=5;i++) {
            if (converge == false){
                if (i == 1) {
                    List<String> a = getRandomCentroids(args[0]);
                    c1 = a.toArray()[0].toString();
                    c2 = a.toArray()[1].toString();
                } else {
                    String last_out = output+"output_"+Integer.toString(i-1)+"/"+file_name;
                    List<String> PrevC = getCentroids(last_out);
                    c1 = PrevC.toArray()[0].toString();
                    c2 = PrevC.toArray()[1].toString();
                    prev_x = c1;
                    prev_y = c2;
                }
                Configuration conf = new Configuration();
                conf.set("C1",c1);
                conf.set("C2",c2);
                Path temp_out = new Path(output+"output_"+Integer.toString(i)+"/");
                Job job = Job.getInstance(conf, "kMeans");
                job.setJarByClass(kMeans.class);
                job.setMapperClass(kMeansMapper.class);
                job.setCombinerClass(kMeansCombiner.class);
                job.setReducerClass(kMeansReducer.class);
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(Text.class);
                FileInputFormat.addInputPath(job, new Path(input));
                FileOutputFormat.setOutputPath(job, temp_out);
                job.waitForCompletion(true);
                List<String> CurrC = getCentroids(output+"output_"+Integer.toString(i)+"/"+file_name);
                if (prev_x.equals(CurrC.toArray()[0].toString()) && prev_y.equals(CurrC.toArray()[1].toString())) {
                    converge = true;
                }
            } else {
                System.out.println("We Converge!");
                break;
            }
        }

    }
}
