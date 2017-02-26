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
            Integer K = Integer.valueOf(conf.get("K"));

            String[] data = value.toString().split(",");
            double cur_x = Double.valueOf(data[0]);
            double cur_y = Double.valueOf(data[1]);


            List<Double> ans = new ArrayList<Double>();
            ans.add(Double.POSITIVE_INFINITY);
            ans.add((double)1);

            // read all the centroids from the setting
            // and compare to get the close one
            for (int i=0;i<K;i++){
                String[] temp = conf.get("C"+Integer.toString(i)).split(",");
                double temp_x = Double.valueOf(temp[0]);
                double temp_y = Double.valueOf(temp[1]);
                double dis = Math.sqrt(Math.pow(cur_x - temp_x,2)+Math.pow(cur_y - temp_y,2));

                if (dis<ans.get(0)) {
                    ans.set(0,dis);
                    ans.set(1,(double)i);
                }
            }

            context.write(new Text(conf.get("C"+Integer.toString(ans.get(1).intValue()))),new Text(value));


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
            context.write(key, new Text(Double.toString(mean_x)+","+Double.toString(mean_y)+","+Double.toString(count)));
        }
    }

    public static class kMeansReducer extends Reducer<Text,Text,Text,Text> {
        public void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
            int mean_x = 0;
            int mean_y = 0;
            int count = 0;
            for (Text str:value) {
                String[] data = str.toString().split(",");
                mean_x += Double.valueOf(data[0]);
                mean_y += Double.valueOf(data[1]);
                count += Double.valueOf(data[2]);
            }
            context.write(key,new Text(Double.toString(mean_x/count)+","+Double.toString(mean_y/count)));
        }
    }

    public static List<String> getRandomCentroids(String f, Integer n) throws Exception{
        List<String> temp = new ArrayList<String>();
        List<String> ret = new ArrayList<String>();
        BufferedReader br = new BufferedReader(new FileReader(f));
        String line;
        while ((line = br.readLine())!=null) {
            temp.add(line);
        }
        Random centroids = new Random();
        for (int i=1;i<=n;i++) {
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
        List<String> c = new ArrayList<String>();
        String input = args[0];
        String output = args[1];
        Integer k = Integer.valueOf(args[2]);
        String file_name = "part-r-00000";
        boolean converge = false;
        for (int i=1;i<=50;i++) {
            // if we converge
            if (converge == false){
                // for the 1 time, we generate some random centroids
                if (i == 1) {
                    List<String> a = getRandomCentroids(args[0],k);
                    for (int j=0;j<a.size();j++) {
                        c.add(a.toArray()[j].toString());
                    }
                } else {
                    String last_out = output+"output_"+Integer.toString(i-1)+"/"+file_name;
                    List<String> PrevC = getCentroids(last_out);

                    for (int j=0;j<PrevC.size();j++) {

                        String temp_x = PrevC.toArray()[j].toString().split(",")[2];
                        String temp_y = PrevC.toArray()[j].toString().split(",")[3];
                        c.add(temp_x+","+temp_y);
                    }

                }
                Configuration conf = new Configuration();
                conf.set("mapred.textoutputformat.separator", ",");
                conf.set("K",Integer.toString(k));

                for (int j=0;j<c.size();j++){
                    conf.set("C"+Integer.toString(j),c.get(j));
                }
                c.clear();


                Path temp_out = new Path(output+"output_"+Integer.toString(i)+"/");

                Job job = Job.getInstance(conf, "kMeans");

                job.setNumReduceTasks(1);
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
                for (int j=0;j<CurrC.size();j++){
                    String[] centroids = CurrC.get(j).split(",");
                    if (centroids[0].equals(centroids[2]) && centroids[1].equals(centroids[3])){
                        converge = true;
                    } else {
                        converge = false;
                    }
                }
            } else {
                System.out.println("We Converge!");
                break;
            }
        }

    }
}
