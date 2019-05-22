package org.kwangsun.spark.kotlin.rdd

import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.SparkConf


fun main(args: Array<String>) {
    val appName = "Spark Example"
    val master = "local[*]"

    val conf = SparkConf().setAppName(appName).setMaster(master)
    val sc = JavaSparkContext(conf)

    val broadcastVar = sc.broadcast(intArrayOf(1, 2, 3))
    broadcastVar.value()
    // returns [1, 2, 3]

    broadcastVar.value().forEach { println(it) }

}
