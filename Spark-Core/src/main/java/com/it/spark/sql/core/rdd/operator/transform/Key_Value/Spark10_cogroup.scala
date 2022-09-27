package com.it.spark.sql.core.rdd.operator.transform.Key_Value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ZuYingFang
 * @time 2022-03-24 15:01
 * @description reduceByKey分区内和分区间计算规则是相同的
 */
object Spark10_cogroup {

    def main(args: Array[String]): Unit = {
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("operator")
        val sc = new SparkContext(sparkConf)

        // RDD算子：cogroup————在类型为(K,V)和(K,W)的RDD上调用，返回一个(K,(Iterable<V>,Iterable<W>))类型的RDD
        val rdd1: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("4", 4), ("4", 90), ("a", 6),("c",9)))
        val rdd2: RDD[(String, String)] = sc.makeRDD(List(("a", "b"), ("a", "c"), ("a", "d"), ("4", "l"), ("5", "5")))
        val cogroupRDD: RDD[(String, (Iterable[Int], Iterable[String]))] = rdd1.cogroup(rdd2)
        cogroupRDD.collect().foreach(println)
        sc.stop()
//        (a,(CompactBuffer(1, 6),CompactBuffer(b, c, d)))
//        (c,(CompactBuffer(9),CompactBuffer()))
//        (4,(CompactBuffer(4, 90),CompactBuffer(l)))
//        (5,(CompactBuffer(),CompactBuffer(5)))

    }

}
