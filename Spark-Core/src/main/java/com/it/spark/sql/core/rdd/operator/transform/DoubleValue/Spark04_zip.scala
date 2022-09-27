package com.it.spark.sql.core.rdd.operator.transform.DoubleValue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ZuYingFang
 * @time 2022-03-24 14:39
 * @description
 */
object Spark04_zip {

    def main(args: Array[String]): Unit = {
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("operator")
        val sc = new SparkContext(sparkConf)


        // zip：拉链，一个为key，另外一个为value，多余的直接不管了，不可指定分区数
        // 两个rdd分区数必须相同，得到的rdd3也是相同的，两个rdd元素数量也必须相同
        val rdd1: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)
        val rdd2: RDD[Int] = sc.makeRDD(List(3, 4, 5, 6), 2)

        val rdd3: RDD[(Int, Int)] = rdd1.zip(rdd2)

        rdd3.saveAsTextFile("output")

        sc.stop()
    }

}
