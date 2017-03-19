package Problem1;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

import static org.apache.spark.sql.functions.col;

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

        Dataset<Row> df = spark.read()
                        .format("csv")
                        .option("header",true)
                        .load(args[0]);
        // T1: Filter out (drop) the transactions from T whose total amount is less than $200
        Dataset<Row> result1 = df.filter(col("transNum").gt(200));
        //df.createOrReplaceTempView("temp");
        //Dataset<Row> result1 = spark.sql("SELECT * FROM temp where _c2 > 200");
        result1.show();

        // T2: Over T1, group the transactions by the Number of Items it has, and
        // for each group calculate the sum of total amounts, the average of total amounts, the min and the max of the total amounts.
        Dataset<Row> result2 = result1.groupBy("itemNum").agg(functions.sum("transNum"),functions.avg("transNum"),functions.max("transNum"),functions.min("transNum"));
        result2.show();

        // T3: Over T1, group the transactions by customer ID,
        // for each group report the customer ID, and the transactions’ count.
        Dataset<Row> result3 = result1.groupBy("custID").count();
        result3.show();

        // T4:
        //Filter out (drop) the transactions from T whose total amount is less than $600
        Dataset<Row> result4 = df.filter(col("transNum").gt(600));
        result4.show();

        // T5: Over T4, group the transactions by customer ID,
        // for each group report the customer ID, and the transactions’ count.
        Dataset<Row> result5 = result4.groupBy("custID").count();
        result5.show();

        // T6: Select the customer IDs whose  T5.count * 3 < T3 count
        result5 = result5.select(col("custID"), col("count").multiply(3).alias("count*3"));
        Dataset<Row> result6 = result5.join(result3,"custID");
        result6 = result6.filter(col("count*3").lt(col("count")));
        result6.show();

    }
}
