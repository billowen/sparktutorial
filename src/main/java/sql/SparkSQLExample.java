package sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.col;

public class SparkSQLExample {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("Java Spark SQL basic example")
                .getOrCreate();

        runBasicDataFrameExample(spark);

        spark.stop();
    }

    private static void runBasicDataFrameExample(SparkSession spark) {
        Dataset<Row> df = spark.read().json("src/main/resources/people.json");
//        df.show();
//        df.printSchema();
//        df.select(col("name")).show();
//        df.select(col("name"), col("age").plus(1)).show();
//        df.filter(col("age").gt(21)).show();
//        df.groupBy("age").count().show();

        df.createOrReplaceTempView("people");
        Dataset<Row> sqlDF = spark.sql("SELECT * FROM people");
        sqlDF.show();
    }
}
