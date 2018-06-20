package com.xenonstack.examples.spark.com.xenonstack.examples.spark.rdd

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object SparkPersistenceDemo {

  def main(args: Array[String]): Unit = {

    val BASE_PATH = "file:///home/dev/workspace/spark-demos"

    val conf = new SparkConf().setAppName("persistence demo").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val textFile = sc.textFile(BASE_PATH + "/names.txt")

    val lines = textFile.flatMap(line => line.split(","))

    //    lines.cache()  only for in-memory in desearlized form


    /*    In this storage level, RDD is stored as deserialized Java object in the JVM.
        If the size of RDD is greater than memory, It will not cache some partition and recompute them
        next time whenever needed. In this level the space used for storage is very high,
        the CPU computation time is low, the data is stored in-memory. It does not make use of the disk. */
    lines.persist(StorageLevel.MEMORY_ONLY)


    /* This level of Spark store the RDD as serialized Java object (one-byte array per partition).
    It is more space efficient as compared to deserialized objects, especially when it uses fast serializer.
    But it increases the overhead on CPU.
    In this level the storage space is low, the CPU computation time is high and the data is stored in-memory.
     It does not make use of the disk. */
    //    lines.persist(StorageLevel.MEMORY_ONLY_SER)


    /* In this level, RDD is stored as deserialized Java object in the JVM. When the size
     of RDD is greater than the size of memory, it stores the excess partition on the disk,
      and retrieve from disk whenever required.
     In this level the space used for storage is high, the CPU computation time is medium,
     it makes use of both in-memory and on disk storage. */
    //    lines.persist(StorageLevel.MEMORY_AND_DISK)


    /*It is similar to MEMORY_ONLY_SER, but it drops the partition that does not fits into memory to disk,
     rather than recomputing each time it is needed. In this storage level,
     The space used for storage is low, the CPU computation time is high,
     it makes use of both in-memory and on disk storage.*/
    //    lines.persist(StorageLevel.MEMORY_AND_DISK_SER)


    /*In this storage level, RDD is stored only on disk. The space used for storage is low,
    the CPU computation time is high and it makes use of on disk storage.*/
    //    lines.persist(StorageLevel.DISK_ONLY)


    /*Same as above , but it replicates the cache to 2 nodes*/
//    lines.persist(StorageLevel.DISK_ONLY_2)
//    lines.persist(StorageLevel.MEMORY_AND_DISK_2)


    // OffHeap Memory means outside JVM , but in RAM
//    lines.persist(StorageLevel.OFF_HEAP)

    val countTuples = lines.map(word => (word, 1))

    val counts = countTuples.reduceByKey((accumulator, currentValue) => (accumulator + currentValue))

    //     runs a function on each object
    counts.foreach(println)

    val totalWords = lines
      .map(s => s.length)
      .reduce((a, b) => a + b)

    println("Reduce " + totalWords)


    // true means synchonous
    lines.unpersist(true)
  }

}