package query.Query4;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by test on 2/5/17.
 */
public class Query4_Reducer extends Reducer<Text, Text, Text, Text> {
    public void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
        int numOfTrans = 0;
        float min_trans = 1000.0f;
        float max_trans = 10.0f;
        for (Text str:value) {
            String[] str_value = str.toString().split(",");
            numOfTrans += Integer.valueOf(str_value[0]);
            min_trans = Math.min(min_trans,Float.valueOf(str_value[1]));
            max_trans = Math.max(max_trans,Float.valueOf(str_value[2]));
        }
        context.write(key, new Text(Integer.toString(numOfTrans)+","+Float.toString(min_trans)+","+Float.toString(max_trans)));
        }
    }

