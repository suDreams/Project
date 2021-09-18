package cn.byd.learning;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.*;

public class OrdersSpark {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().master("local[4]").appName("Order").getOrCreate();

        JavaRDD<String> orderRDD = spark.sparkContext().textFile("file:///D:\\MyWork\\project\\prj_oa\\data\\orders.csv", 1).toJavaRDD();

        JavaRDD<String> customerRDD = spark.sparkContext().textFile("file:///D:\\MyWork\\project\\prj_oa\\data\\customers.csv", 1).toJavaRDD();

        JavaRDD<Orders> ordersRDD = orderRDD.map(line -> {
            String[] parts = line.replace("\"", "").split(",");
            Orders orders = new Orders();
            orders.setOrder_id(parts[0]);
            orders.setOrder_date(parts[1]);
            orders.setOrder_customer_id(parts[2]);
            orders.setOrder_status(parts[3]);
            return orders;
        });

        Dataset<Row> orderDF = spark.createDataFrame(ordersRDD, Orders.class);


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

        Dataset<Row> resultDF = customerDF.join(orderDF, new Column("customer_id").equalTo(new Column("order_customer_id")))
                .select(functions.concat_ws("-", new Column("customer_fname"), new Column("customer_lname")).as("name"), new Column("order_status"))
                .groupBy("name")
                .agg(functions.count("order_status").as("number"))
                .sort(functions.desc("number"));
        resultDF.show();


        spark.close();
    }
}
