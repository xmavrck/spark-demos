package com.xenonstack.examples.spark

import org.apache.spark.{SparkConf, SparkContext}

object SparkWordCountDemo {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("wordcount").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val textFile = sc.textFile("file:///home/dev/workspace/spark-demos/names.txt")

    val counts = textFile.flatMap(line => line.split(","))

      .map(word => (word, 1))

      .reduceByKey((accumulator, currentValue) => (accumulator + currentValue))

    counts.foreach(println)

  }

}