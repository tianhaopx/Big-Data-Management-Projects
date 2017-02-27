package Problem3_DistributedCache;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.*;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by test on 2/26/17.
 */
public class P3M extends Mapper<LongWritable, Text, Text, Text> {

    private List<String> c = new ArrayList<String>();
    private BufferedReader brReader;

    public void setup(Context context) throws IOException, InterruptedIOException {
        URI[] cacheFilesLocal = DistributedCache.getCacheFiles(context.getConfiguration());
        for (URI eachURI : cacheFilesLocal) {
            Path eachPath = new Path(eachURI);
            if (eachPath.getName().toString().trim().equals("kCenters")) {
                loadCentroids(eachPath, context);
            }
        }
    }

    private void loadCentroids(Path filepath, Context context) throws IOException {
        String strLineRead = "";
        brReader = new BufferedReader(new FileReader(filepath.toString()));
        while ((strLineRead = brReader.readLine()) != null) {
            if (strLineRead.split(",").length == 4) {
                String x = strLineRead.split(",")[2];
                String y = strLineRead.split(",")[3];
                c.add(x+","+y);
            } else {
                c.add(strLineRead);
            }
        }
        System.out.println(c.size());
        System.out.println("==============================");
    }

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        Integer K = c.size();

        String[] data = value.toString().split(",");
        double cur_x = Double.valueOf(data[0]);
        double cur_y = Double.valueOf(data[1]);


        List<Double> ans = new ArrayList<Double>();
        ans.add(Double.POSITIVE_INFINITY);
        ans.add((double)1);

        // read all the centroids from the setting
        // and compare to get the close one
        for (int i=0;i<K;i++){
            String[] temp = c.get(i).split(",");
            double temp_x = Double.valueOf(temp[0]);
            double temp_y = Double.valueOf(temp[1]);
            double dis = Math.sqrt(Math.pow(cur_x - temp_x,2)+Math.pow(cur_y - temp_y,2));

            if (dis<ans.get(0)) {
                ans.set(0,dis);
                ans.set(1,(double)i);
            }
        }
        System.out.println(c.size());
        System.out.println("==============================");
        context.write(new Text(c.get((ans.get(1).intValue()))),new Text(value));


    }
}
