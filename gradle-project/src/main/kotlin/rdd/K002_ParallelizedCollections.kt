package org.kwangsun.spark.kotlin.rdd

import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.SparkConf


fun main(args: Array<String>) {
    val appName = "Spark Example"
    val master = "local[*]"

    val conf = SparkConf().setAppName(appName).setMaster(master)
    val sc = JavaSparkContext(conf)

    val data = listOf(1, 2, 3, 4, 5)
    val distData = sc.parallelize(data)

    distData.collect().forEach{i -> println(i)}
}