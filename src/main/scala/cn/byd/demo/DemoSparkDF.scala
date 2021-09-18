package cn.byd.demo

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

object DemoSparkDF {
  case class Customer(
                       customer_id : String,
                        customer_fname : String,
                        customer_lname : String,
                        customer_email : String,
                        customer_password : String ,
                        customer_street : String,
                        customer_city : String,
                        customer_state : String,
                        customer_zipcode : String
                     )
  case class Order(
                    order_id           : String,
                    order_date         : String,
                    order_customer_id  : String,
                    order_status       : String
                  )
  case class OrderItem(
                        order_item_id         : String,
                        order_item_order_id   : String,
                        order_item_product_id : String,
                        order_item_quantity   : String,
                        order_item_subtotal : Double ,
                        order_item_product_price : Double
                      )
  case class Product (
                       product_id : String,
                       product_category_id : String,
                       product_name : String,
                       product_description : String,
                       product_price : Double,
                       product_image : String
                     )
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[4]").appName("DataFrame").getOrCreate()
    val sc = spark.sparkContext

    import spark.implicits._
    import org.apache.spark.sql.functions._
    //customers
    val customersRDD: RDD[String] = sc.textFile("file:///D:\\MyWork\\project\\prj_oa\\data\\customers.csv")
    val customerDF: DataFrame = customersRDD.map(lines => {
      val line = lines.replace("\"", "")
      val parts = line.split(",")
      Customer(parts(0), parts(1), parts(2), parts(3), parts(4), parts(5), parts(6), parts(7), parts(8))
    }).toDF()
//    customerDF.show()

    //order_items
    val orderItemRdd: RDD[String] = sc.textFile("file:///D:\\MyWork\\project\\prj_oa\\data\\order_items.csv")
    val orderItemDF: DataFrame = orderItemRdd.map(lines => {
      val parts: Array[String] = lines.replace("\"", "").split(",")
      OrderItem(parts(0), parts(1), parts(2), parts(3), parts(4).toDouble, parts(5).toDouble)
    }).toDF()

    //orders
    val orderRDD: RDD[String] = sc.textFile("file:///D:\\MyWork\\project\\prj_oa\\data\\orders.csv")
    val orderDF: DataFrame = orderRDD.map(lines => {
      val parts: Array[String] = lines.replace("\"", "").split(",")
      Order(parts(0), parts(1), parts(2), parts(3))
    }).toDF()

    //product
    val productRDD: RDD[String] = sc.textFile("file:///D:\\MyWork\\project\\prj_oa\\data\\products.csv")
    productRDD.map(lines => {
      val parts: Array[String] = lines.replace("\"", "").split(",")
      Product(parts(0),parts(1),parts(2),parts(3),parts(4).toDouble,parts(5))
    })

    val result: DataFrame = customerDF.join(orderDF, customerDF("customer_id") === orderDF("order_customer_id"))
      .filter(orderDF("order_status") === "COMPLETE")
        .select(concat_ws("-",col("customer_fname"),col("customer_lname")))
        .agg(sum("order_customer_id").as("total_consumption"))

    result.show()


    spark.close()
  }
}
