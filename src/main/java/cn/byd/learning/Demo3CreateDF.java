package cn.byd.learning;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.*;

import java.util.Arrays;

public class Demo3CreateDF {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().master("local[3]").appName("this is a test").getOrCreate();

        // Create an RDD of Person objects from a text file
        JavaRDD<String> peopleRDD = spark.read().textFile("file:///D:\\MyWork\\project\\prj_oa\\data\\people.txt").javaRDD();
        RDD<String> rdd = spark.read().textFile("file:///D:\\MyWork\\project\\prj_oa\\data\\people.txt").rdd();

        JavaRDD<Person> mapRDD = peopleRDD.map(line -> {
            String[] parts = line.split(",");
            Person person = new Person();
            person.setName(parts[0]);
            person.setAge(Integer.parseInt(parts[1]));
            return person;
        });

        // Apply a schema to an RDD of JavaBeans to get a DataFrame
        Dataset<Row> personDF = spark.createDataFrame(mapRDD, Person.class);
        personDF.show();

        // Register the DataFrame as a temporary view
        personDF.createOrReplaceTempView("people");


        // SQL statements can be run by using the sql methods provided by spark
        Dataset<Row> sql = spark.sql("select * from people where age BETWEEN 13 AND 19");
        sql.show();

        //The columns of a row in the result can be accessed by field index
        Encoder<String> stringEncoder = Encoders.STRING();
        Dataset<String> stringDataset = sql.map(
                (MapFunction<Row, String>) row -> "name:" + row.getString(1), stringEncoder);
        stringDataset.show();


    }
}
