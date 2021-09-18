package cn.byd.learning;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;


public class SchemaRowToDF {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().master("local[4]").appName("createDF").getOrCreate();
        SparkContext sc = spark.sparkContext();

        // Create an RDD
        JavaRDD<String> peopleRDD = sc.textFile("file:///D:\\MyWork\\project\\prj_oa\\data\\people.txt", 1).toJavaRDD();

        // Generate the schema based on the string of schema
        List<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField("name", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("age",DataTypes.IntegerType,false));
        StructType schema = DataTypes.createStructType(fields);

        // Convert records of the RDD (people) to Rows
        JavaRDD<Row> rowRDD = peopleRDD.map((Function<String, Row>) record -> {
            String[] parts = record.split(",");
            Row row = RowFactory.create(parts[0], Integer.parseInt(parts[1].trim()));
            return row;
        });

        // Apply the schema to the RDD
        Dataset<Row> dataFrame = spark.createDataFrame(rowRDD, schema);
        // Creates a temporary view using the DataFrame
        dataFrame.createOrReplaceTempView("people");

        // SQL can be run over a temporary view created using DataFrames
        Dataset<Row> sql = spark.sql("select * from people");
//        sql.show();

        // The results of SQL queries are DataFrames and support all the normal RDD operations
        // The columns of a row in the result can be accessed by field index or by field name
        Dataset<String> mapFunc = sql.map((MapFunction<Row, String>) record -> "name:" + record.getString(0), Encoders.STRING());
        mapFunc.show();


        spark.close();

    }
}

