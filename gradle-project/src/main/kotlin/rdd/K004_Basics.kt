package org.kwangsun.spark.kotlin.rdd

import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.SparkConf


fun main(args: Array<String>) {
    val appName = "Spark Example"
    val master = "local[*]"

    val conf = SparkConf().setAppName(appName).setMaster(master)
    val sc = JavaSparkContext(conf)

    val lines = sc.textFile("data.txt")
    val lineLengths = lines.map { s -> s.length }
    //lineLengths.persist(StorageLevel.MEMORY_ONLY());
    val totalLength = lineLengths.reduce { a, b -> a!! + b!! }

    println(totalLength)
}