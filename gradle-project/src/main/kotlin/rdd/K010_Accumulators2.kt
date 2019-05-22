package org.kwangsun.spark.kotlin.rdd

import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.SparkConf
import org.apache.spark.util.AccumulatorV2


//class VectorAccumulatorV2 : AccumulatorV2<MyVector, MyVector> {
//
//    val myVector = MyVector.createZeroVector();
//
//    override fun reset(): Unit = myVector.reset();
//
//    override fun add(v: MyVector): Unit = myVector.add(v);
//
//    override fun isZero(): Boolean = myVector.isZero();
//}
//
//class MyVector {
//    fun reset(): Unit = TODO();
//    fun add(v: MyVector): Unit = TODO();
//    fun isZero(): Boolean = TODO();
//    companion object {
//        fun createZeroVector() = MyVector();
//    }
//}
//
//fun main(args: Array<String>) {
//    val appName = "Spark Example"
//    val master = "local[*]"
//
//    val conf = SparkConf().setAppName(appName).setMaster(master)
//    val sc = JavaSparkContext(conf)
//
//    VectorAccumulatorV2 myVectorAcc = new VectorAccumulatorV2();
//    sc.sc().register(myVectorAcc, "MyVectorAcc1");
//
//    println( accum.value() )
//
//}
