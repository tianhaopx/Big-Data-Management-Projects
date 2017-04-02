package Problem2;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by test on 3/18/17.
 */
public class Step3 {
    public static void main(String[] args) {

        class SumNeighbor implements Function2<String, String, String> {
            public String call(String a, String b) {
                return a + "," + b;
            }
        }

        SparkConf conf = new SparkConf().setAppName("Step3").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);


        JavaRDD<Tuple2<Integer,Double>> density = sc.objectFile(args[0]);

        JavaRDD<Tuple2<Integer,Double>> temp = sc.objectFile(args[1]);

        JavaPairRDD<Integer, Double> ans = temp
                .mapToPair(coor->new Tuple2<Integer, Double>(coor._1,coor._2));

        final Broadcast<Map> topMap = sc.broadcast(ans.collectAsMap());

        class get_neighbor_Density implements PairFlatMapFunction<Tuple2<Integer, Double>, Integer, String> {
            public Iterator<Tuple2<Integer, String>> call(Tuple2 s) {
                int cell = (int) s._1;
                double density = (double) s._2;
                ArrayList<Tuple2<Integer, String>> list = new ArrayList<>();
                Map temp = topMap.getValue();
                if (cell == 1) {
                    // 3 neighbor 2,501,502
                    // |1  |2  |
                    // |501|502|
                    for (int i : new int[]{2, 501, 502}) {
                        if (temp.containsKey(i)) {
                            list.add(new Tuple2<>(i, cell + "," + String.format("%.3f",density)));
                        }
                    }
                    return list.iterator();
                }
                //if the cell in the bottom left
                else if (cell == 249501) {
                    // 3 neighbor 249502,249001,249002
                    // |249001  |249002  |
                    // |249501  |249502  |
                    for (int i : new int[]{249001, 249002, 249502}) {
                        if (temp.containsKey(i)) {
                            list.add(new Tuple2<>(i, cell + "," + String.format("%.3f",density)));
                        }
                    }
                    return list.iterator();
                }
                // if the cell in the top right
                else if (cell == 500) {
                    // 3 neighbor 499,999,1000
                    // |499  |500   |
                    // |999  |1000  |
                    for (int i : new int[]{499, 999, 1000}) {
                        if (temp.containsKey(i)) {
                            list.add(new Tuple2<>(i, cell + "," + String.format("%.3f",density)));
                        }
                    }
                    return list.iterator();
                }
                // if the cell in the bottom right
                else if (cell == 250000) {
                    // 3 neighbor 249499,249500,249999
                    // |249499  |249500  |
                    // |249999  |250000  |
                    for (int i : new int[]{249499, 249500, 249999}) {
                        if (temp.containsKey(i)) {
                            list.add(new Tuple2<>(i, cell + "," + String.format("%.3f",density)));
                        }
                    }
                    return list.iterator();
                }
                // if the cell on the top edge
                else if ((cell > 1 && cell < 500)) {
                    // |cell-1    |cell      |cell+1    |
                    // |cell+499  |cell+500  |cell+501  |
                    for (int i : new int[]{cell - 1, cell + 1, cell + 499, cell + 500, cell + 501}) {
                        if (temp.containsKey(i)) {
                            list.add(new Tuple2<>(i, cell + "," + String.format("%.3f",density)));
                        }
                    }
                    return list.iterator();
                }
                // if the cell on the bottom edge
                else if (cell > 249501 && cell < 250000) {
                    // |cell-501    |cell-500      |cell-499    |
                    // |cell-1      |cell          |cell+1      |
                    for (int i : new int[]{cell - 1, cell + 1, cell - 499, cell - 500, cell - 501}) {
                        if (temp.containsKey(i)) {
                            list.add(new Tuple2<>(i, cell + "," + String.format("%.3f",density)));
                        }
                    }
                    return list.iterator();
                }
                // if the cell on the left edge
                else if (cell % 500 == 1) {
                    // |cell-500   |cell-499      |
                    // |cell       |cell+1        |
                    // |cell+500   |cell+501      |
                    for (int i : new int[]{cell - 500, cell - 499, cell + 1, cell + 500, cell + 501}) {
                        if (temp.containsKey(i)) {
                            list.add(new Tuple2<>(i, cell + "," + String.format("%.3f",density)));
                        }
                    }
                    return list.iterator();
                }
                // if the cell on the right edge
                else if (cell % 500 == 0) {
                    // |cell-501   |cell-500      |
                    // |cell-1     |cell          |
                    // |cell+499   |cell+500      |
                    for (int i : new int[]{cell - 500, cell - 501, cell - 1, cell + 500, cell + 499}) {
                        if (temp.containsKey(i)) {
                            list.add(new Tuple2<>(i, cell + "," + String.format("%.3f",density)));
                        }
                    }
                    return list.iterator();
                }
                // the cell in the middle
                else {
                    // |cell-501   |cell-500      |cell-499    |
                    // |cell-1     |cell          |cell+1      |
                    // |cell+499   |cell+500      |cell+501    |
                    for (int i : new int[]{cell - 1, cell + 1, cell - 501, cell - 500, cell - 499, cell + 501, cell + 500, cell + 499}) {
                        if (temp.containsKey(i)) {
                            list.add(new Tuple2<>(i, cell + "," + String.format("%.3f",density)));
                        }
                    }
                    return list.iterator();
                }
            }
        }

        JavaPairRDD<Integer, String> neighbor_density = density
                .flatMapToPair(new get_neighbor_Density())
                .reduceByKey(new SumNeighbor());

        neighbor_density.saveAsTextFile(args[2]+"/step3");

    }

}
