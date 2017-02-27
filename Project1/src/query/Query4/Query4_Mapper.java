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
import java.net.URI;
import java.util.HashMap;

public class Query4_Mapper extends Mapper<LongWritable, Text, Text, Text> {

        private static HashMap<String, String> CustomerMap = new HashMap<String, String>();
        private static HashMap<String, String> CountryMap = new HashMap<String, String>();
        private BufferedReader brReader;

        public void setup(Context context) throws IOException, InterruptedIOException {
            URI[] cacheFilesLocal = DistributedCache.getCacheFiles(context.getConfiguration());
            for (URI eachURI : cacheFilesLocal) {
                Path eachPath = new Path(eachURI);
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
                String customerid = customerFieldArray[0].trim();
                String countrycode = customerFieldArray[3].trim();
                CustomerMap.put(customerid, countrycode);


                if (CountryMap.get(countrycode) != null) {
                    // contain this key
                    CountryMap.put(countrycode, Integer.toString(Integer.valueOf(CountryMap.get(countrycode))+1));
                } else {
                    // key might be present
                    if (CountryMap.containsKey(countrycode)) {
                        CountryMap.put(countrycode, "1");
                    } else {
                        // no such key
                        CountryMap.put(countrycode, "1");
                    }
                }
            }
        }

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] str_value = value.toString().split(",");
            String temp_key = CustomerMap.get(str_value[1])+","+CountryMap.get(CustomerMap.get(str_value[1]));
            context.write(new Text(temp_key), new Text(Float.valueOf(str_value[2])+"," + Float.valueOf(str_value[2])));
        }
}


