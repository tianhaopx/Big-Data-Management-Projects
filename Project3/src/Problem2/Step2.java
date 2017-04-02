package Problem2;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import scala.Serializable;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

/**
 * Created by test on 3/18/17.
 */
public class Step2 {

    static class MyTupleComparator implements Comparator<Tuple2<Integer, Double>>, Serializable {
        final static P2.MyTupleComparator INSTANCE = new P2.MyTupleComparator();

        // note that the comparison is performed on the key's frequency
        // assuming that the second field of Tuple2 is a count or frequency
        public int compare(Tuple2<Integer, Double> t1, Tuple2<Integer, Double> t2) {
            return -t1._2.compareTo(t2._2);    // sort descending
            // return t1._2.compareTo(t2._2);  // sort ascending
        }

    }

    public static void main(String[] args) {
        class GetSplit implements Function<String, String[]> {
            public String[] call(String s) {
                return s.split(",");
            }
        }

        class AssignCell implements PairFlatMapFunction<String[], Integer, String> {
            public Iterator<Tuple2<Integer, String>> call(String[] s) {
                ArrayList<Tuple2<Integer, String>> list = new ArrayList<>();
                int cell = (int) (Math.floor(Integer.valueOf(s[0]) / 20) + Math.floor(Math.abs(Integer.valueOf(s[1]) - 9999) / 20) * 500 + 1);

                //if the cell in the top left
                if (cell == 1) {
                    // 3 neighbor 2,501,502
                    // |1  |2  |
                    // |501|502|
                    for (int i : new int[]{1, 2, 501, 502}) {
                        if (i == 1) {
                            list.add(new Tuple2<>(i, "1" + ",0,3"));
                        } else {
                            list.add(new Tuple2<>(i, "0" + ",1,0"));
                        }
                    }
                    return list.iterator();
                }
                //if the cell in the bottom left
                else if (cell == 249501) {
                    // 3 neighbor 249502,249001,249002
                    // |249001  |249002  |
                    // |249501  |249502  |
                    for (int i : new int[]{249501, 249001, 249002, 249502}) {
                        if (i == 249501) {
                            list.add(new Tuple2<>(i, "1" + ",0,3"));
                        } else {
                            list.add(new Tuple2<>(i, "0" + ",1,0"));
                        }
                    }
                    return list.iterator();
                }
                // if the cell in the top right
                else if (cell == 500) {
                    // 3 neighbor 499,999,1000
                    // |499  |500   |
                    // |999  |1000  |
                    for (int i : new int[]{500, 499, 999, 1000}) {
                        if (i == 500) {
                            list.add(new Tuple2<>(i, "1" + ",0,3"));
                        } else {
                            list.add(new Tuple2<>(i, "0" + ",1,0"));
                        }
                    }
                    return list.iterator();
                }
                // if the cell in the bottom right
                else if (cell == 250000) {
                    // 3 neighbor 249499,249500,249999
                    // |249499  |249500  |
                    // |249999  |250000  |
                    for (int i : new int[]{250000, 249499, 249500, 249999}) {
                        if (i == 250000) {
                            list.add(new Tuple2<>(i, "1" + ",0,3"));
                        } else {
                            list.add(new Tuple2<>(i, "0" + ",1,0"));
                        }
                    }
                    return list.iterator();
                }
                // if the cell on the top edge
                else if ((cell > 1 && cell < 500)) {
                    // |cell-1    |cell      |cell+1    |
                    // |cell+499  |cell+500  |cell+501  |
                    for (int i : new int[]{cell, cell - 1, cell + 1, cell + 499, cell + 500, cell + 501}) {
                        if (i == cell) {
                            list.add(new Tuple2<>(i, "1" + ",0,5"));
                        } else {
                            list.add(new Tuple2<>(i, "0" + ",1,0"));
                        }
                    }
                    return list.iterator();
                }
                // if the cell on the bottom edge
                else if (cell > 249501 && cell < 250000) {
                    // |cell-501    |cell-500      |cell-499    |
                    // |cell-1      |cell          |cell+1      |
                    for (int i : new int[]{cell, cell - 1, cell + 1, cell - 499, cell - 500, cell - 501}) {
                        if (i == cell) {
                            list.add(new Tuple2<>(i, "1" + ",0,5"));
                        } else {
                            list.add(new Tuple2<>(i, "0" + ",1,0"));
                        }
                    }
                    return list.iterator();
                }
                // if the cell on the left edge
                else if (cell % 500 == 1) {
                    // |cell-500   |cell-499      |
                    // |cell       |cell+1        |
                    // |cell+500   |cell+501      |
                    for (int i : new int[]{cell, cell - 500, cell - 499, cell + 1, cell + 500, cell + 501}) {
                        if (i == cell) {
                            list.add(new Tuple2<>(i, "1" + ",0,5"));
                        } else {
                            list.add(new Tuple2<>(i, "0" + ",1,0"));
                        }
                    }
                    return list.iterator();
                }
                // if the cell on the right edge
                else if (cell % 500 == 0) {
                    // |cell-501   |cell-500      |
                    // |cell-1     |cell          |
                    // |cell+499   |cell+500      |
                    for (int i : new int[]{cell, cell - 500, cell - 501, cell - 1, cell + 500, cell + 499}) {
                        if (i == cell) {
                            list.add(new Tuple2<>(i, "1" + ",0,5"));
                        } else {
                            list.add(new Tuple2<>(i, "0" + ",1,0"));
                        }
                    }
                    return list.iterator();
                }
                // the cell in the middle
                else {
                    // |cell-501   |cell-500      |cell-499    |
                    // |cell-1     |cell          |cell+1      |
                    // |cell+499   |cell+500      |cell+501    |
                    for (int i : new int[]{cell, cell - 1, cell + 1, cell - 501, cell - 500, cell - 499, cell + 501, cell + 500, cell + 499}) {
                        if (i == cell) {
                            list.add(new Tuple2<>(i, "1" + ",0,8"));
                        } else {
                            list.add(new Tuple2<>(i, "0" + ",1,0"));
                        }
                    }
                    return list.iterator();
                }
            }
        }

        class SumCell implements Function2<String, String, String> {
            public String call(String a, String b) {
                String[] temp1 = a.split(",");
                String[] temp2 = b.split(",");
                int self_cell = Integer.valueOf(temp1[0]) + Integer.valueOf(temp2[0]);
                int relative_cell = Integer.valueOf(temp1[1]) + Integer.valueOf(temp2[1]);
                int relative_cell_count = 0;
                if (Integer.valueOf(temp1[2]) != 0) {
                    relative_cell_count = Integer.valueOf(temp1[2]);
                } else if (Integer.valueOf(temp2[2]) != 0) {
                    relative_cell_count = Integer.valueOf(temp2[2]);
                }
                return self_cell + "," + relative_cell+","+relative_cell_count;
            }
        }

        class CalDensity implements PairFunction<Tuple2<Integer, String>,Integer,Double> {
            public Tuple2<Integer, Double> call(Tuple2 a) {
                double ans;
                String[] temp = a._2.toString().split(",");
                ans = Double.valueOf(temp[0]) / (Double.valueOf(temp[1])/(Double.valueOf(temp[2])));
                Tuple2<Integer, Double> ret = new Tuple2<>((int)a._1,Double.valueOf(String.format("%.3f",ans)));
                return ret;
            }
        }

        // calculate the nodes in each cube
        SparkConf conf = new SparkConf().setAppName("Step2").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String[]> coorFile = sc.textFile(args[0]).map(new GetSplit());

        JavaPairRDD<Integer, String> coordinates = coorFile
                .flatMapToPair(new AssignCell())
                .reduceByKey(new SumCell());

        // report for the step2
        JavaPairRDD<Integer, Double> density = coordinates
                .mapToPair(new CalDensity())
                .coalesce(1);

        density.saveAsObjectFile(args[1]+"/density");

        List<Tuple2<Integer, Double>> topNResult = density.takeOrdered(50, MyTupleComparator.INSTANCE);

        JavaRDD<Tuple2<Integer, Double>> temp = sc.parallelize(topNResult);
        JavaPairRDD<Integer, Double> ans = temp
                .mapToPair(coor->new Tuple2<Integer, Double>(coor._1,coor._2));

        ans.saveAsObjectFile(args[1]+"/step2");

    }
}
