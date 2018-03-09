package sql.streaming;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

public class CreateStreamingDatasets {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().appName("JavaCreateStreamSource").getOrCreate();
        StructType userSchema = new StructType().add("name", "string").add("age", "integer");
        Dataset<Row> csvDF = spark.readStream().option("sep", ";")
            .schema(userSchema)
            .csv("src/main/resources/people.csv");
        csvDF.printSchema();

        spark.stop();
    }
}
