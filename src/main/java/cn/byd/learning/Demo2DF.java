package cn.byd.learning;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;

import java.util.Arrays;
import java.util.Collections;

import static org.apache.spark.sql.functions.col;

public class Demo2DF {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().master("local[4]").appName("DF").getOrCreate();

        //通过读取json文件创建df
        /*Dataset<Row> peopleDS = spark.read().json("file:///D:\\MyWork\\project\\prj_oa\\data\\people.json");

        peopleDS.show();

        peopleDS.select("name").show();

        peopleDS.filter(col("age").gt(22)).show();

        peopleDS.select(col("name"),col("age").plus(2)).show();

        peopleDS.groupBy("age").count().show();*/

        // Create an instance of a Bean class
        Person person = new Person();
        person.setName("tom");
        person.setAge(21);

        // Encoders are created for Java beans
        Encoder<Person> personEncoder = Encoders.bean(Person.class);
        Dataset<Person> personDS = spark.createDataset(
                Collections.singletonList(person), personEncoder
        );
        personDS.show();

        // Encoders for most common types are provided in class Encoders
        Encoder<Integer> integerEncoder = Encoders.INT();
        Dataset<Integer> intDS = spark.createDataset(Arrays.asList(1, 2, 3), integerEncoder);
        Dataset<Integer> mapDS = intDS.map((MapFunction<Integer, Integer>) value -> value + 2, integerEncoder);
        mapDS.show();

        Dataset<Person> personDataset = spark.read().json("file:///D:\\MyWork\\project\\prj_oa\\data\\people.json").as(personEncoder);
        personDataset.show();
        Dataset<Row> rowDataset = spark.read().json("file:///D:\\MyWork\\project\\prj_oa\\data\\people.json");
        rowDataset.show();
    }
}
