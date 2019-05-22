package org.kwangsun.spark.kotlin.rdd

import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaSparkContext


fun main(args: Array<String>) {
    val conf = SparkConf()
        .setMaster("local")
        .setAppName("Kotlin Spark Test")

    val sc = JavaSparkContext(conf)
    val count = sc.parallelize(listOf(1,2,3,4)).count()
    println(count)
}
