package cn.byd.learning;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.*;

import java.util.*;

public class SparkDataSet {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().master("local[4]").appName("mySpark").getOrCreate();
        SparkContext sc = spark.sparkContext();

        Person person = new Person();
        Person person1 = new Person();
        person.setName("Andy");
        person.setAge(31);
        person1.setName("tom");
        person1.setAge(25);


        Encoder<Person> personEncoder = Encoders.bean(Person.class);
        ArrayList<Person> personList = new ArrayList<>();
        personList.add(person);
        personList.add(person1);


        Dataset<Person> ds = spark.createDataset(
                Collections.singletonList(person) , personEncoder
        );
        ds.show();

        /*List<Long> arr = new ArrayList<>();
        arr.add(1L);
        arr.add(2L);
        arr.add(3L);*/

        /*Encoder<Long> longEncoder = Encoders.LONG();
//        Dataset<Long> primitiveDS = spark.createDataset(arr, longEncoder);
        Dataset<Long> primitiveDS = spark.createDataset(Arrays.asList(1L,2L,3L), longEncoder);

        Dataset<Long> mapDS = primitiveDS.map((MapFunction<Long, Long>) fun -> {
            long sum = fun + 1L;
            return sum;
        }, longEncoder);
//        Dataset<Long> map1DS = primitiveDS.map((MapFunction<Long, Long>) value -> value + 1L, longEncoder);
        mapDS.show();

        Dataset<Person> DfToDs = spark.read().json("file:///D:\\MyWork\\project\\prj_oa\\data\\people.json").as(personEncoder);
        DfToDs.show();*/

        /*JavaRDD<String> stringJavaRDD = spark.read().textFile("file:///D:\\MyWork\\project\\prj_oa\\data\\people.txt").javaRDD();
        JavaRDD<Person> mapRDD = stringJavaRDD.map(line -> {
            String[] parts = line.split(",");
            Person person1 = new Person();
            person1.setName(parts[0]);
            person1.setAge(Integer.parseInt(parts[1]));
            return person1;
        });

        Dataset<Row> dataFrame = spark.createDataFrame(mapRDD, Person.class);
        dataFrame.show();*/
        sc.stop();
        spark.close();
    }
}

