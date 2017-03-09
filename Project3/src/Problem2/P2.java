package Problem2;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Map;

/**
 * Created by test on 3/8/17.
 */
public class P2 {
    public static Double calDensity(Tuple2 s, Map dic) {
        int cell = (int)s._1;
        double ans;
        //from now we find the neighbors

        //if the cell in the top left
        if (cell == 1) {
            // 3 neighbor 2,501,502
            ans = Double.valueOf(dic.get(cell).toString())/((int)dic.get(2)+(int)dic.get(501)+(int)dic.get(502));
            return ans;
        }
        //if the cell in the bottom left
        else if (cell == 249501) {
            // 3 neighbor 249000,249001,249501
            ans = Double.valueOf(dic.get(cell).toString())/((int)dic.get(249000)+(int)dic.get(249001)+(int)dic.get(249501));
            return ans;
        }
        // if the cell in the top right
        else if (cell == 500) {
            // 3 neighbor 499,999,1000
            ans = Double.valueOf(dic.get(cell).toString())/((int)dic.get(499)+(int)dic.get(999)+(int)dic.get(1000));
            return ans;
        }
        // if the cell in the bottom right
        else if (cell == 250000) {
            // 3 neighbor 249499,249500,249999
            ans = Double.valueOf(dic.get(cell).toString())/((int)dic.get(249499)+(int)dic.get(249500)+(int)dic.get(249999));
            return ans;
        }
        // if the cell on the top edge
        else if ((cell>1 && cell <500)) {
            ans = Double.valueOf(dic.get(cell).toString())/((int)dic.get(cell-1)+(int)dic.get(cell+1)+(int)dic.get(cell+499)+(int)dic.get(cell+500)+(int)dic.get(cell-1)+(int)dic.get(cell+501));
            return ans;
        }
        // if the cell on the bottom edge
        else if (cell>249501 && cell<250000) {
            ans = Double.valueOf(dic.get(cell).toString())/((int)dic.get(cell-1)+(int)dic.get(cell+1)+(int)dic.get(cell-499)+(int)dic.get(cell-500)+(int)dic.get(cell-1)+(int)dic.get(cell-501));
            return ans;
        }
        // if the cell on the left edge
        else if (cell%500==1 && cell != 1 && cell != 249501){
            ans = Double.valueOf(dic.get(cell).toString())/((int)dic.get(cell-500)+(int)dic.get(cell-499)+(int)dic.get(cell+1)+(int)dic.get(cell+500)+(int)dic.get(cell+501));
            return ans;
        }
        // if the cell on the right edge
        else if (cell%500==0 && cell != 500 && cell != 250000){
            ans = Double.valueOf(dic.get(cell).toString())/((int)dic.get(cell-500)+(int)dic.get(cell-501)+(int)dic.get(cell-1)+(int)dic.get(cell+500)+(int)dic.get(cell+499));
            return ans;
        }
        // the cell in the middle
        else {
            ans = Double.valueOf(dic.get(cell).toString())/((int)dic.get(cell-1)+(int)dic.get(cell+1)+(int)dic.get(cell-501)+(int)dic.get(cell-500)+(int)dic.get(cell-499)+(int)dic.get(cell+499)+(int)dic.get(cell+500)+(int)dic.get(cell+501));
            return ans;
        }
    }

    public static void main(String[] args) {
        class GetSplit implements Function<String, String[]> {
            public String[] call(String s) { return s.split(","); }
        }


        // calculate the nodes in each cube
        SparkConf conf = new SparkConf().setAppName("P2").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String[]> coorFile = sc.textFile(args[0]).map(new GetSplit());
        JavaPairRDD<Integer, Integer> coordinates = coorFile
                .mapToPair(coor -> new Tuple2<>((int)(Math.floor(Integer.valueOf(coor[0])/20)+Math.floor(Math.abs(Integer.valueOf(coor[1])-9999)/20)*500+1), 1))
                .reduceByKey((a,b)->a+b);

        //find the nearby
        Map temp = coordinates.collectAsMap();
        // calculate the density
//        JavaRDD<String> results = coordinates
//                .map(new GetNeighbor(temp));

        JavaPairRDD<Double, Integer> density = coordinates
                .mapToPair(coor -> new Tuple2<>(calDensity(coor,temp),coor._1))
                .sortByKey(false);

        density.saveAsTextFile(args[1]);
    }
}
