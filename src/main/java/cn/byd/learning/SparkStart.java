package cn.byd.learning;

import org.apache.spark.sql.*;

import static org.apache.spark.sql.functions.col;

public class SparkStart {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("learning")
                .master("local[4]")
                .getOrCreate();

//        Dataset<Row> df = spark.read().json("file:///D:\\MyWork\\project\\prj_oa\\data\\people.json");

        Dataset<Person> personDS = spark.read().json("file:///D:\\MyWork\\project\\prj_oa\\data\\people.json").as(Encoders.bean(Person.class));

        personDS.show();

        /*df.printSchema();
        df.select("name").show();

        df.select(col("name"),col("age").plus(1)).show();

        df.filter(col("age").gt(21)).show();

        Dataset<Row> groupDS = df.groupBy(new Column("age")).count();
        groupDS.show();*/



    }
}
