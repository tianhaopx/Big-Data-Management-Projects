package Problem1;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

/**
 * Created by test on 3/7/17.
 */
public class P1 {

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("Problem 1")
                .config("spark.master", "local")
                .getOrCreate();

        Dataset<Row> df = spark.read().csv(args[0]);
        // T1: Filter out (drop) the transactions from T whose total amount is less than $200
        df.createOrReplaceTempView("temp");
        Dataset<Row> result1 = spark.sql("SELECT * FROM temp where _c2 > 200");
        result1.show();

        // T2: Over T1, group the transactions by the Number of Items it has, and
        // for each group calculate the sum of total amounts, the average of total amounts, the min and the max of the total amounts.
        Dataset<Row> result2 = result1.groupBy("_c3").agg(functions.sum("_c2"),functions.avg("_c2"),functions.max("_c2"),functions.min("_c2"));
        result2.show();

        // T3: Over T1, group the transactions by customer ID,
        // for each group report the customer ID, and the transactions’ count.
        Dataset<Row> result3 = result1.groupBy("_c1").count();
        result3.show();

        // T4:
        //Filter out (drop) the transactions from T whose total amount is less than $600
        Dataset<Row> result4 = spark.sql("SELECT * FROM temp where _c2 > 600");
        result4.show();

        // T5: Over T4, group the transactions by customer ID,
        // for each group report the customer ID, and the transactions’ count.
        Dataset<Row> result5 = result4.groupBy("_c1").count();
        result5.show();

        // T6: Select the customer IDs whose  T5.count * 3 < T3 count
        result5 = result5.select(functions.col("_c1"),functions.col("count").multiply(3).alias("count3"));
        Dataset<Row> result6 = result5.join(result3,result5.col("_c1").equalTo(result3.col("_c1")));
        result6.createOrReplaceTempView("temp");
        result6 = spark.sql("SELECT * FROM temp where count3 < count");
        result6.show();


    }
}
