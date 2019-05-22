package org.kwangsun.spark.kotlin.rdd

import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaRDD
import java.io.Serializable


object MyFunction : Serializable {
    fun func1(s: String): Int {
        return s.length
    }

    fun func2(a: Int, b: Int): Int {
        return a!! + b!!
    }
}

fun main(args: Array<String>) {
    val appName = "Spark Example"
    val master = "local[*]"

    val conf = SparkConf().setAppName(appName).setMaster(master)
    val sc = JavaSparkContext(conf)

    val lines = sc.textFile("data.txt")

    val lineLengths = lines.map(MyFunction::func1)
    val totalLength = lineLengths.reduce(MyFunction::func2)

    println(totalLength)
}

class MyClass : Serializable {
    fun func1(s: String): String = s.length.toString()
    fun doStuff(rdd: JavaRDD<String>): JavaRDD<String> = rdd.map(this::func1)
}


class MyClass2 : Serializable {
    val field = "Hello"
    fun doStuff(rdd: JavaRDD<String>): JavaRDD<String> = rdd.map{ x -> field + x    }
}


class MyClass3 : Serializable {
    val field = "Hello"
    fun doStuff(rdd: JavaRDD<String>): JavaRDD<String> {
        val field_ = this.field
        return rdd.map{ x -> field_ + x }
    }
}
