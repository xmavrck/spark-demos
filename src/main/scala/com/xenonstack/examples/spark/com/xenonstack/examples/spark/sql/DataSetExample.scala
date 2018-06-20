package com.xenonstack.examples.spark.com.xenonstack.examples.spark.sql

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object DataSetExample {

  def main(args: Array[String]): Unit = {

    val BASE_PATH = "file:///home/dev/workspace/spark-demos"

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("Spark SQL basic example")
      .getOrCreate()

    val df = spark.read.json(BASE_PATH + "/user.json")

    df.printSchema()

    df.createOrReplaceTempView("users")

//    val explodedDF:Dataset[Row]=df.explode()
//
//    explodedDF.
//
//    spark.sql("select results from users limit 10").show()

  }
}