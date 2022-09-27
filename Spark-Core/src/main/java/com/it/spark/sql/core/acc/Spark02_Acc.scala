package com.it.spark.sql.core.acc

import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ZuYingFang
 * @time 2022-03-27 14:45
 * @description
 */
object Spark02_Acc {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
        val sc = new SparkContext(sparkConf)


        // 分区内计算，分区间也要计算
        val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

        // 获取系统的累加器，Spark默认就提供了简单数据聚合的累加器
        val sumAcc: LongAccumulator = sc.longAccumulator("sum")
        rdd.foreach( num => sumAcc.add(num))
        println(sumAcc.value)   // 10

        sc.stop()


    }

}
