package com.it.spark.sql.core.rdd.operator.transform.DoubleValue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ZuYingFang
 * @time 2022-03-24 14:39
 * @description
 */
object Spark02_union {

    def main(args: Array[String]): Unit = {
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("operator")
        val sc = new SparkContext(sparkConf)


        // union：取两个RDD的并集，返回一个新的RDD，不可指定分区数，分区数为两个rdd分区数的和，不会给元素去重
        val rdd1: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 1)
        val rdd2: RDD[Int] = sc.makeRDD(List(3, 4, 5, 6), 1)

        val rdd3: RDD[Int] = rdd1.union(rdd2)

        rdd3.saveAsTextFile("output")

        sc.stop()
    }

}
