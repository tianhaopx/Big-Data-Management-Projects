package query.Query4;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.net.URI;

/**
 * Created by test on 2/5/17.
 */
public class App extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Configuration(), new App(), args);
        System.exit(exitCode);
    }

    public int run(String[] args) throws Exception {
        Job job = new Job();
        job.setJarByClass(App.class);


        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));


        Configuration conf = job.getConfiguration();
        DistributedCache.addCacheFile(new URI("/user/test/customer"),conf);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapperClass(Query4_Mapper.class);
        job.setCombinerClass(Query4_Reducer.class);
        job.setReducerClass(Query4_Reducer.class);

        boolean success = job.waitForCompletion(true);
        return success ? 0:1;
    }
}