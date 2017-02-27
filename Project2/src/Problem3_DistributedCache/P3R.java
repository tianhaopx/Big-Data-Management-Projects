package Problem3_DistributedCache;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by test on 2/26/17.
 */

    public  class P3R extends Reducer<Text,Text,Text,Text> {
        public void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
            double mean_x = 0;
            double mean_y = 0;
            double count = 0;
            for (Text str:value) {
                String[] data = str.toString().split(",");
                mean_x += Double.valueOf(data[0]);
                mean_y += Double.valueOf(data[1]);
                count += Double.valueOf(data[2]);
            }
            context.write(key,new Text(String.format("%.3f",mean_x/count)+","+String.format("%.3f",mean_y/count)));
        }
    }

