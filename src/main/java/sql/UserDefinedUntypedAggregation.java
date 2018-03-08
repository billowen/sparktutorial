package sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

public class UserDefinedUntypedAggregation {
    public static class MyAverage extends UserDefinedAggregateFunction {
        private StructType inputSchema;
        private StructType bufferSchema;

        public MyAverage() {
            List<StructField> inputFields = new ArrayList<>();
            inputFields.add(DataTypes.createStructField("inputColumn", DataTypes.LongType, true));
            inputSchema = DataTypes.createStructType(inputFields);

            List<StructField> bufferFields = new ArrayList<>();
            bufferFields.add(DataTypes.createStructField("sum", DataTypes.LongType, true));
            bufferFields.add(DataTypes.createStructField("count", DataTypes.LongType, true));
            bufferSchema = DataTypes.createStructType(bufferFields);
        }

        @Override
        public StructType inputSchema() {
            return inputSchema;
        }

        @Override
        public StructType bufferSchema() {
            return bufferSchema;
        }

        @Override
        public DataType dataType() {
            return DataTypes.DoubleType;
        }

        // Whether this function always returns the same output on the identical input
        @Override
        public boolean deterministic() {
            return true;
        }

        @Override
        public void initialize(MutableAggregationBuffer buffer) {
            buffer.update(0, 0L);
            buffer.update(1, 0L);
        }

        // Updates the given aggregation buffer `buffer` with new input data from `input`
        @Override
        public void update(MutableAggregationBuffer buffer, Row input) {
            if (!input.isNullAt(0)) {
                long updateSum = buffer.getLong(0) + input.getLong(0);
                long updateCount = buffer.getLong(1) + 1;
                buffer.update(0, updateSum);
                buffer.update(1, updateCount);
            }
        }

        // Merges two aggregation buffers and stores the updated buffer values back to `buffer1`
        @Override
        public void merge(MutableAggregationBuffer buffer1, Row buffer2) {
            long mergeSum = buffer1.getLong(0) + buffer2.getLong(0);
            long mergeCount = buffer1.getLong(1) + buffer2.getLong(1);
            buffer1.update(0, mergeSum);
            buffer1.update(1, mergeCount);
        }

        @Override
        public Double evaluate(Row buffer) {
            return ((double) buffer.getLong(0)) / buffer.getLong(1);
        }
    }

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("Java Spark SQL aggregation example")
                .getOrCreate();
        spark.udf().register("myAverage", new MyAverage());
        Dataset<Row> df = spark.read().json("src/main/resources/employees.json");
        df.createOrReplaceTempView("employees");
        df.show();

        Dataset<Row> result = spark.sql("SELECT myAverage(salary) as average_salary FROM employees");
        result.show();
    }
}
