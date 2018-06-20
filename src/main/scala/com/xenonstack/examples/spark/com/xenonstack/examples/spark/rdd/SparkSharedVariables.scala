package com.xenonstack.examples.spark.com.xenonstack.examples.spark.rdd

import org.apache.spark.{SparkConf, SparkContext}

object SparkSharedVariables {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("shared_variables").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val broadcastVar = sc.broadcast(Array(1, 2, 3))

    broadcastVar.value.foreach(e => println(e))


    val accum = sc.longAccumulator("My Accumulator")

    val data = sc.parallelize(Array(1, 2, 3, 4))

    data.foreach(x => accum.add(x))

    println("accum " + accum.value)

    val accumLazy = sc.longAccumulator

    val mapTask = data.map { x => accumLazy.add(x); x }

    // Here, accum is still 0 because no actions have caused the map operation to be computed.
    println("accumLazy Before Action " + accumLazy.value)

    mapTask.collect()

    println("accumLazy After Action  " + accumLazy.value)
  }

}