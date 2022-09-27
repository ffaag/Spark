package com.it.spark.sql.core.rdd.operator.transform.DoubleValue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ZuYingFang
 * @time 2022-03-24 14:39
 * @description
 */
object Spark03_subtract {

    def main(args: Array[String]): Unit = {
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("operator")
        val sc = new SparkContext(sparkConf)


        // subtract：取两个RDD的差集，返回一个新的RDD，可以指定最终rdd的分区数
        val rdd1: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
        val rdd2: RDD[Int] = sc.makeRDD(List(3, 4, 5, 6))

        val rdd3: RDD[Int] = rdd1.subtract(rdd2, 1)

        rdd3.saveAsTextFile("output")

        sc.stop()
    }

}
