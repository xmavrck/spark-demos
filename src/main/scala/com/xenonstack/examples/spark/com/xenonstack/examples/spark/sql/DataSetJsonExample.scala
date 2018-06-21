package com.xenonstack.examples.spark.com.xenonstack.examples.spark.sql

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions.explode

object DataSetExample {

  def main(args: Array[String]): Unit = {

    val BASE_PATH = "file:///home/dev/workspace/spark-demos"

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("Spark SQL basic example")
      .getOrCreate()

    val df = spark.read.json(BASE_PATH + "/user.json")


    df.createOrReplaceTempView("users")

//    SELECT id, part.lock, part.key FROM mytable EXTERNAL VIEW explode(parts) parttable AS part;

    spark.sql("select results.name,results.gender,results.company from users  EXTERNAL VIEW explode(results) parttable AS results ").show()


  }
}