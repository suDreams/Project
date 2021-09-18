package cn.byd.learning;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Demo01 {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().master("local[4]").appName("Dataset").getOrCreate();
        JavaRDD<String> peopleRDD = spark.read().textFile("file:///D:\\MyWork\\project\\prj_oa\\data\\people.txt").javaRDD();
        JavaRDD<Person> mapRDD = peopleRDD.map(line -> {
            String[] parts = line.split(",");
            Person person = new Person();
            person.setName(parts[0]);
            person.setAge(Integer.parseInt(parts[1]));

            return person;

        });

        Dataset<Row> df = spark.createDataFrame(mapRDD, Person.class);
        df.show();


    }
}
