package sql.streaming;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;

import java.util.Arrays;

public class StructuredNetworkWordCount {
    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: StructuredNetworkWordCount <hostname> <port>");
            System.exit(1);
        }
        String hostname = args[0];
        int port = Integer.parseInt(args[1]);
        SparkSession spark = SparkSession.builder()
                .appName("JavaStructuredNetworkWordCount")
                //.master("local[4]")
                .getOrCreate();
        Dataset<Row> lines = spark.readStream()
                .format("socket")
                .option("host", hostname)
                .option("port", port)
                .load();
        System.out.println(lines.isStreaming());
        lines.printSchema();
        Dataset<String> words = lines.as(Encoders.STRING())
                .flatMap(
                        (FlatMapFunction<String, String>)  x -> Arrays.asList(x.split(" ")).iterator(),
                        Encoders.STRING()
                );
        Dataset<Row> wordCounts = words.groupBy("value").count();
        StreamingQuery query = wordCounts.writeStream()
                .outputMode("complete")
                .format("console")
                .start();
        query.awaitTermination();

    }
}
