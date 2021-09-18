package cn.byd.learning;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.*;

public class CustomerSpark {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().master("local[4]").appName("customer").getOrCreate();

        JavaRDD<String> customerRDD = spark.sparkContext().textFile("file:///D:\\MyWork\\project\\prj_oa\\data\\customers.csv", 1).toJavaRDD();

         JavaRDD<Customers> map_customer_RDD = customerRDD.map(lines -> {
            String[] parts = lines.replace("\"", "").split(",");
            Customers customer = new Customers();
            customer.setCustomer_id(parts[0]);
            customer.setCustomer_fname(parts[1]);
            customer.setCustomer_lname(parts[2]);
            customer.setCustomer_email(parts[3]);
            customer.setCustomer_password(parts[4]);
            customer.setCustomer_street(parts[5]);
            customer.setCustomer_city(parts[6]);
            customer.setCustomer_state(parts[7]);
            customer.setCustomer_zipcode(parts[8]);
            return customer;
        });

        Dataset<Row> customerDF = spark.createDataFrame(map_customer_RDD, Customers.class);

        customerDF.show();

       /* Encoder<Customers> customersEncoder = Encoders.bean(Customers.class);
        Dataset<Customers> customersDataset = spark.read().csv("file:///D:\\MyWork\\project\\prj_oa\\data\\customers.csv").as(customersEncoder);
        customersDataset.show();*/

        spark.close();
    }
}
