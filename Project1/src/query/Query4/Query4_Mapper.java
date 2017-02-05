package query.Query4;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.HashMap;

public class Query4_Mapper extends org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, Text> {

        private static HashMap<String, String> CustomerMap = new HashMap<String, String>();
        private BufferedReader brReader;

        public void setup(Context context) throws IOException, InterruptedIOException {
            Path[] cacheFilesLocal = DistributedCache.getLocalCacheFiles(context.getConfiguration());
            for (Path eachPath : cacheFilesLocal) {
                if (eachPath.getName().toString().trim().equals("customer")) {
                    loadCustomerHashMap(eachPath, context);
                }
            }
        }

        private void loadCustomerHashMap(Path filepath, Context context) throws IOException {
            String strLineRead = "";
            brReader = new BufferedReader(new FileReader(filepath.toString()));

            while ((strLineRead = brReader.readLine()) != null) {
                String customerFieldArray[] = strLineRead.split(",");
                CustomerMap.put(customerFieldArray[0].trim(), customerFieldArray[3].trim());
            }
        }

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] str_value = value.toString().split(",");
            context.write(new Text(CustomerMap.get(str_value[1])), new Text("1," + Float.valueOf(str_value[2])+"," + Float.valueOf(str_value[2])));
        }
}


