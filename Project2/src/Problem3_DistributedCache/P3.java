package Problem3_DistributedCache;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.*;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by test on 2/26/17.
 */
public class P3 extends Configured implements Tool {

    public static List<String> getCentroids(Path f) throws Exception{
        FileSystem fs = FileSystem.get(new Configuration());
        BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(f)));
        List<String> ret = new ArrayList<String>();
        String line;
        while ((line = br.readLine())!=null) {
            ret.add(line);
        }
        br.close();
        return ret;
    }


    public void writeDistributedCache(Path f, List<String> data) throws Exception{
        FileSystem fs = FileSystem.get(new Configuration());
        BufferedWriter br=new BufferedWriter(new OutputStreamWriter(fs.create(f,true)));
        for (int i=0;i<data.size();i++) {
            String[] temp = data.get(i).split(",");
            br.write(temp[2]+","+temp[3]);
            br.newLine();
        }
        br.close();
    }


    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Configuration(), new P3(), args);
        System.exit(exitCode);
    }

    public int run(String[] args) throws Exception {
        String DistributeCachePath = "Project2/input/kCenters";
        String input = args[0];
        String output = args[1];
        Double cov;
        if (args.length == 4) {
            cov = Double.valueOf(args[3]);
        } else {
            cov = 0.5;
        }
        String file_name = "part-r-00000";
        boolean converge = false;
        for (int i = 1; i <= 5; i++) {
            // if we converge
            if (!converge) {
                Path temp_out = new Path(output + "output_" + Integer.toString(i) + "/");

                Job job = new Job();
                job.setJarByClass(P3.class);


                FileInputFormat.addInputPath(job, new Path(input));
                FileOutputFormat.setOutputPath(job, temp_out);


                Configuration conf = job.getConfiguration();
                conf.set("mapred.textoutputformat.separator", ",");
                DistributedCache.addCacheFile(new URI(DistributeCachePath), conf);

                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(Text.class);
                job.setMapperClass(P3M.class);
                job.setCombinerClass(P3C.class);
                job.setReducerClass(P3R.class);
                job.waitForCompletion(true);
                List<String> CurrC = getCentroids(new Path(output + "output_" + Integer.toString(i) + "/" + file_name));
                for (int j = 0; j < CurrC.size(); j++) {
                    String[] centroids = CurrC.get(j).split(",");
                    double old_x = Double.valueOf(centroids[0]);
                    double old_y = Double.valueOf(centroids[1]);
                    double new_x = Double.valueOf(centroids[2]);
                    double new_y = Double.valueOf(centroids[3]);
                    double diff_x = Math.abs(old_x - new_x);
                    double diff_y = Math.abs(old_y - new_y);
                    if (diff_x <= cov && diff_y <= cov) {
                        converge = true;
                    } else {
                        converge = false;
                        writeDistributedCache(new Path(DistributeCachePath),CurrC);
                        break;
                    }
                }

            } else {
                System.out.println("We Converge!");
                return 1;
            }
        }
        System.out.println("We do not Converge!");
        return 1;
    }
}
