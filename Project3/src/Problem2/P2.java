package Problem2;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;


/**
 * Created by test on 3/8/17.
 */
public class P2 {
    public static Double calDensity(Tuple2 s) {
        double ans;
        String[] temp = s._2.toString().split(",");
        ans = Double.valueOf(temp[0])/Double.valueOf(temp[1]);
        return ans;
    }

    public static void main(String[] args) {
        class GetSplit implements Function<String, String[]> {
            public String[] call(String s) { return s.split(","); }
        }

        class AssignCell implements PairFlatMapFunction<String[], Integer, String> {
            public Iterator<Tuple2<Integer,String>> call(String[] s){
                ArrayList<Tuple2<Integer,String>> list = new ArrayList<>();
                int cell = (int)(Math.floor(Integer.valueOf(s[0])/20)+Math.floor(Math.abs(Integer.valueOf(s[1])-9999)/20)*500+1);

                //if the cell in the top left
                if (cell == 1) {
                    // 3 neighbor 2,501,502
                    // |1  |2  |
                    // |501|502|
                    for (int i:new int[] {1,2,501,502}){
                        if (i==1){
                            list.add(new Tuple2<>(i,"1"+",0"));
                        } else {
                            list.add(new Tuple2<>(i,"0"+",1"));
                        }
                    }
                    return list.iterator();
                }
                //if the cell in the bottom left
                else if (cell == 249501) {
                    // 3 neighbor 249502,249001,249002
                    // |249001  |249002  |
                    // |249501  |249502  |
                    for (int i:new int[] {249501,249001,249002,249502}){
                        if (i==249501){
                            list.add(new Tuple2<>(i,"1"+",0"));
                        } else {
                            list.add(new Tuple2<>(i,"0"+",1"));
                        }
                    }
                    return list.iterator();
                }
                // if the cell in the top right
                else if (cell == 500) {
                    // 3 neighbor 499,999,1000
                    // |499  |500   |
                    // |999  |1000  |
                    for (int i:new int[] {500,499,999,1000}){
                        if (i==500){
                            list.add(new Tuple2<>(i,"1"+",0"));
                        } else {
                            list.add(new Tuple2<>(i,"0"+",1"));
                        }
                    }
                    return list.iterator();
                }
                // if the cell in the bottom right
                else if (cell == 250000) {
                    // 3 neighbor 249499,249500,249999
                    // |249499  |249500  |
                    // |249999  |250000  |
                    for (int i:new int[] {250000,249499,249500,249999}){
                        if (i==250000){
                            list.add(new Tuple2<>(i,"1"+",0"));
                        } else {
                            list.add(new Tuple2<>(i,"0"+",1"));
                        }
                    }
                    return list.iterator();
                }
                // if the cell on the top edge
                else if ((cell>1 && cell <500)) {
                    // |cell-1    |cell      |cell+1    |
                    // |cell+499  |cell+500  |cell+501  |
                    for (int i:new int[] {cell,cell-1,cell+1,cell+499,cell+500,cell+501}){
                        if (i==cell){
                            list.add(new Tuple2<>(i,"1"+",0"));
                        } else {
                            list.add(new Tuple2<>(i,"0"+",1"));
                        }
                    }
                    return list.iterator();
                }
                // if the cell on the bottom edge
                else if (cell>249501 && cell<250000) {
                    // |cell-501    |cell-500      |cell-499    |
                    // |cell-1      |cell          |cell+1      |
                    for (int i:new int[] {cell,cell-1,cell+1,cell-499,cell-500,cell-501}){
                        if (i==cell){
                            list.add(new Tuple2<>(i,"1"+",0"));
                        } else {
                            list.add(new Tuple2<>(i,"0"+",1"));
                        }
                    }
                    return list.iterator();
                }
                // if the cell on the left edge
                else if (cell%500==1){
                    // |cell-500   |cell-499      |
                    // |cell       |cell+1        |
                    // |cell+500   |cell+501      |
                    for (int i:new int[] {cell,cell-500,cell-499,cell+1,cell+500,cell+501}){
                        if (i==cell){
                            list.add(new Tuple2<>(i,"1"+",0"));
                        } else {
                            list.add(new Tuple2<>(i,"0"+",1"));
                        }
                    }
                    return list.iterator();
                }
                // if the cell on the right edge
                else if (cell%500==0){
                    // |cell-501   |cell-500      |
                    // |cell-1     |cell          |
                    // |cell+499   |cell+500      |
                    for (int i:new int[] {cell,cell-500,cell-501,cell-1,cell+500,cell+499}){
                        if (i==cell){
                            list.add(new Tuple2<>(i,"1"+",0"));
                        } else {
                            list.add(new Tuple2<>(i,"0"+",1"));
                        }
                    }
                    return list.iterator();
                }
                // the cell in the middle
                else {
                    // |cell-501   |cell-500      |cell-499    |
                    // |cell-1     |cell          |cell+1      |
                    // |cell+499   |cell+500      |cell+501    |
                    for (int i:new int[] {cell,cell-1,cell+1,cell-501,cell-500,cell-499,cell+501,cell+500,cell+499}){
                        if (i==cell){
                            list.add(new Tuple2<>(i,"1"+",0"));
                        } else {
                            list.add(new Tuple2<>(i,"0"+",1"));
                        }
                    }
                    return list.iterator();
                }
            }
        }

        class SumCell implements Function2<String,String,String> {
            public String call(String a, String b) {
                String[] temp1 = a.split(",");
                String[] temp2 = b.split(",");
                int self_cell =  Integer.valueOf(temp1[0])+Integer.valueOf(temp2[0]);
                int relative_cell = Integer.valueOf(temp1[1])+Integer.valueOf(temp2[1]);
                return self_cell+","+relative_cell;
            }
        }

        // calculate the nodes in each cube
        SparkConf conf = new SparkConf().setAppName("P2").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String[]> coorFile = sc.textFile(args[0]).map(new GetSplit());
        JavaPairRDD<Integer, String> coordinates = coorFile
                .flatMapToPair(new AssignCell())
                .reduceByKey(new SumCell());

        // better one
        JavaPairRDD<Double, Integer> density = coordinates
                .mapToPair(coor -> new Tuple2<>(calDensity(coor),coor._1))
                .sortByKey(false);

        density.saveAsTextFile(args[1]);
    }
}
