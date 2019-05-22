package org.kwangsun.spark.kotlin.rdd

import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.SparkConf
import scala.Tuple2


fun main(args: Array<String>) {
    val appName = "Spark Example"
    val master = "local[*]"

    val conf = SparkConf().setAppName(appName).setMaster(master)
    val sc = JavaSparkContext(conf)

    val lines = sc.textFile("data.txt")
    val pairs = lines.mapToPair { s -> Tuple2(s, 1) }
    val counts = pairs.reduceByKey { a, b -> a!! + b!! }

    counts.collect().forEach{ println(it) }

}
