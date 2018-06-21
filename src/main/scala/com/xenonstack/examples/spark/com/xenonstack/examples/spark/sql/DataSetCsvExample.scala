package com.xenonstack.examples.spark.com.xenonstack.examples.spark.sql

import org.apache.spark.sql.{SaveMode, SparkSession}

object DataSetCsvExample {

  def main(args: Array[String]): Unit = {

    val BASE_PATH = "file:///home/dev/workspace/spark-demos"

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("Spark SQL basic example")
      .getOrCreate()

    val df = spark.read.
      format("com.databricks.spark.csv").
      option("header", "true").
      option("inferSchema", "true")
      .load(BASE_PATH + "/Customers.csv")

    df.createOrReplaceTempView("customers")

    println("---------------------select * from customers------------------------")

    spark.sql("select * from customers").show()

    spark.sql("select * from customers").show(false)


    println("-------------------------select count(*) from customers---------------------------")

    spark.sql("select count(*) from customers").show()

    println("-------------------------select country,count(*) from customers group by country---------------------")

    spark.sql("select country,count(*) from customers group by country").show()

    println("----------------------select country,gender,count(*) from customers group by country,gender-------------------------")

    spark.sql("select country,gender,count(*) from customers group by country,gender").show()

    println("-----------------------select country,count(*) from customers where Gender='Male' group by country-----------------------")

    spark.sql("select country,count(*) from customers where Gender='Male' group by country").show()

    println("-------------------select * from customers where Country in('China','Russia')---------------------")

    spark.sql("select * from customers where Country in('China','Russia')").show()

    println("------------------select * from customers where Country in('China','Russia') and Email like '%yelp%'----------------")
    spark.sql("select * from customers where  Country in('China','Russia','Malaysia','Philippines') and first_name like 'Ja%' ").show()

    spark.sql("select * from customers where  Country in('China','Russia','Malaysia','Philippines') and first_name like 'Ja%' ").
      write.option("header", true).format("com.databricks.spark.csv").save(BASE_PATH + "/sql-results")


  }
}