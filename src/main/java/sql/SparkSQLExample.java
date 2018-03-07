package sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

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
        df.show();
    }
}
