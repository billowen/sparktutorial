package sql;

import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Aggregator;
import scala.tools.cmd.gen.AnyVals;

import java.io.Serializable;

public class UserDefinedTypedAggregation {
    public static class Employee implements Serializable {
        private String name;
        private long salary;

//        public Employee(String name, long salary) {
//            this.name = name;
//            this.salary = salary;
//        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public long getSalary() {
            return salary;
        }

        public void setSalary(long salary) {
            this.salary = salary;
        }
    }

    public static class Average implements Serializable {
        private long sum;
        private long count;

        public Average() {}

        public Average(long sum, long count) {
            this.sum = sum;
            this.count = count;
        }

        public long getSum() {
            return sum;
        }

        public void setSum(long sum) {
            this.sum = sum;
        }

        public long getCount() {
            return count;
        }

        public void setCount(long count) {
            this.count = count;
        }
    }

    public static class MyAverage extends Aggregator<Employee, Average, Double> {
        public Average zero() {
            return new Average(0L, 0L);
        }

        // Combine two value to produce a new value. For performance, the function
        // may modify `buffer` and return it instead of constructing a new object
        public Average reduce(Average buffer, Employee employee) {
            long newSum = buffer.getSum() + employee.getSalary();
            long newCount = buffer.getCount() + 1;
            buffer.setSum(newSum);
            buffer.setCount(newCount);
            return buffer;
        }

        // Merge two intermediate values
        public Average merge(Average b1, Average b2) {
            long mergedSum = b1.getSum() + b2.getSum();
            long mergedCount = b1.getCount() + b2.getCount();
            b1.setSum(mergedSum);
            b1.setCount(mergedCount);
            return b1;
        }

        public Double finish(Average reduction) {
            return ((double) reduction.getSum()) / reduction.getCount();
        }

        public Encoder<Average> bufferEncoder() {
            return Encoders.bean(Average.class);
        }

        public Encoder<Double> outputEncoder() {
            return Encoders.DOUBLE();
        }


    }

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("Java Spark SQL aggregate example").getOrCreate();
        Encoder<Employee> employeeEncoder = Encoders.bean(Employee.class);
        Dataset<Employee> ds = spark.read().json("src/main/resources/employees.json").as(employeeEncoder);
        ds.show();

        MyAverage myAverage = new MyAverage();
        TypedColumn<Employee, Double> averageSalary = myAverage.toColumn().name("average_salary");
        Dataset<Double> result = ds.select(averageSalary);
        result.show();
    }
}
