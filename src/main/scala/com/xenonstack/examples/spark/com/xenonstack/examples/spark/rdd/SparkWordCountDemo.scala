package com.xenonstack.examples.spark.com.xenonstack.examples.spark.rdd

import org.apache.spark.{SparkConf, SparkContext}

object SparkWordCountDemo {

  def main(args: Array[String]): Unit = {

    val BASE_PATH = "file:///home/dev/workspace/spark-demos"

    val conf = new SparkConf().setAppName("wordcount").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val textFile = sc.textFile(BASE_PATH + "/names.txt")

    val counts = textFile.flatMap(line => line.split(","))
      .map(word => (word, 1))
      .reduceByKey((accumulator, currentValue) => (accumulator + currentValue))

    println("counts  " + counts.getNumPartitions)

    //     runs a function on each object
    counts.foreach(println)

    // return only 2 elements to avoid OOM errors
    println(counts.take(2))


    println(counts.first())

    // to save the data in single partition
    counts.coalesce(1).saveAsTextFile(BASE_PATH + "/results")

    val countsRepartitioned = counts.repartition(10)

    println("countsRepartitioned " + countsRepartitioned.getNumPartitions)

    // to save the data in single partition
    countsRepartitioned.saveAsTextFile(BASE_PATH + "/results_partitioned")


    val totalWords = textFile.flatMap(line => line.split(","))
      .map(s => s.length)
      .reduce((a, b) => a + b)

    println("Reduce " + totalWords)

  }

}