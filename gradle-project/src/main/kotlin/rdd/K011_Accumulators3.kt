package org.kwangsun.spark.kotlin.rdd

import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.SparkConf
import org.apache.spark.util.AccumulatorV2
import io.netty.handler.codec.smtp.SmtpRequests.data






fun main(args: Array<String>) {
    val appName = "Spark Example"
    val master = "local[*]"

    val conf = SparkConf().setAppName(appName).setMaster(master)
    val sc = JavaSparkContext(conf)

    val data = listOf(1, 2, 3, 4, 5)
    fun f(x: Int): Int = x*100

    val accum = sc.sc().longAccumulator()
    val rdd = sc.parallelize(data).map { x -> accum.add(x?.toLong());f(x);}
    rdd.collect().forEach{it -> println(it)}

}
