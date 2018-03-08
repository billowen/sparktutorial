package sql;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Int;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.spark.sql.functions.col;

public class SparkSQLExample {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("Java Spark SQL basic example")
                .getOrCreate();

        //runBasicDataFrameExample(spark);
        //runDatasetCreationExample(spark);
        //runInferSchemaExample(spark);
        runProgrammaticSchemaExample(spark);

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

    public static class Person implements Serializable {
        private String name;
        private int age;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public int getAge() {
            return age;
        }

        public void setAge(int age) {
            this.age = age;
        }
    }

    private static void runDatasetCreationExample(SparkSession spark) {
        Person person = new Person();
        person.setName("Andy");
        person.setAge(32);

        Encoder<Person> personEncoder = Encoders.bean(Person.class);
        Dataset<Person> javaBeanDS = spark.createDataset(
                Collections.singletonList(person),
                personEncoder
        );
        javaBeanDS.show();

        Encoder<Integer> integerEncoder = Encoders.INT();
        Dataset<Integer> primitiveDS = spark.createDataset(Arrays.asList(1, 2, 3), integerEncoder);
        Dataset<Integer> transformedDS = primitiveDS.map((MapFunction<Integer, Integer>)  value -> value + 1, integerEncoder);
        System.out.println(transformedDS.collect().toString());
    }

    private static void runInferSchemaExample(SparkSession spark) {
        JavaRDD<Person> peopleRDD = spark.read()
                .textFile("src/main/resources/people.txt")
                .javaRDD()
                .map(line -> {
                    String[] parts = line.split(",");
                    Person person = new Person();
                    person.setName(parts[0]);
                    person.setAge(Integer.parseInt(parts[1].trim()));
                    return person;
                });
        Dataset<Row> peopleDF = spark.createDataFrame(peopleRDD, Person.class);
        peopleDF.createOrReplaceTempView("people");
        Dataset<Row> teenagersDF = spark.sql("SELECT name FROM people WHERE age BETWEEN 13 AND 19");
        teenagersDF.show();

        Encoder<String> stringEncoder = Encoders.STRING();
        Dataset<String> teenagerNamesByIndexDF = teenagersDF.map(
                (MapFunction<Row, String>) row -> "Name: " + row.getString(0),
                stringEncoder
        );
        teenagerNamesByIndexDF.show();

        Dataset<String> teenagerNamesByFieldDF = teenagersDF.map(
                (MapFunction<Row, String>) row -> "Name: " + row.<String>getAs("name"),
                stringEncoder
        );
        teenagerNamesByFieldDF.show();
    }

    private static void runProgrammaticSchemaExample(SparkSession spark) {
        JavaRDD<String> peopleRDD = spark.sparkContext()
                .textFile("src/main/resources/people.txt", 1)
                .toJavaRDD();
        String schemaString = "name age";

        List<StructField> fields = new ArrayList<>();
        for (String fieldName : schemaString.split(" ")) {
            StructField field = DataTypes.createStructField(fieldName, DataTypes.StringType, true);
            fields.add(field);
        }
        StructType schema = DataTypes.createStructType(fields);

        JavaRDD<Row> rowRDD = peopleRDD.map(record -> {
            String[] attributes = record.split(",");
            return RowFactory.create(attributes[0], attributes[1].trim());
        });

        Dataset<Row> peopleDF = spark.createDataFrame(rowRDD, schema);
        peopleDF.createOrReplaceTempView("people");
        Dataset<Row> result = spark.sql("SELECT name FROM people");

        Dataset<String> namesDF = result.map(
                (MapFunction<Row, String>) row -> "Name: " + row.getString(0),
                Encoders.STRING()
        );
        namesDF.show();
    }
}
