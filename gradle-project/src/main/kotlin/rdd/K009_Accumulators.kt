package org.kwangsun.spark.kotlin.rdd

import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.SparkConf
import java.util.Arrays




fun main(args: Array<String>) {
    val appName = "Spark Example"
    val master = "local[*]"

    val conf = SparkConf().setAppName(appName).setMaster(master)
    val sc = JavaSparkContext(conf)

    val accum = sc.sc().longAccumulator()
    sc.parallelize(listOf(1, 2, 3, 4)).foreach{ x -> accum.add(x?.toLong()) }

    accum.value()
    // returns 10

    println( accum.value() )

}
