import org.apache.parquet.Strings;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.codehaus.janino.Java;
import scala.Tuple2;

import java.awt.font.FontRenderContext;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class RDDSample {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("RDD Sample").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);
        JavaRDD<Integer> distData = sc.parallelize(data);
        System.out.println(distData.reduce((a, b) -> a + b));

        JavaRDD<String> distFile = sc.textFile("build.gradle");
        System.out.println(distFile.map(String::length).reduce((a,b)->a+b));

        JavaRDD<String> words = distFile.flatMap(s->Arrays.stream(s.split(" ")).iterator());
        JavaPairRDD<String, Integer> pairs = words.mapToPair(s -> new Tuple2<>(s, 1));
        JavaPairRDD<String, Integer> counts = pairs.reduceByKey((a, b) -> a +b);
        List<Tuple2<String, Integer>> array = counts.collect();
        System.out.println(array);

        sc.stop();
    }
}
