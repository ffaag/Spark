package com.it.spark.sql.core.rdd.operator.transform.Key_Value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ZuYingFang
 * @time 2022-03-24 15:01
 * @description reduceByKey分区内和分区间计算规则是相同的
 */
object Spark04_aggregateByKey {

    def main(args: Array[String]): Unit = {
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("operator")
        val sc = new SparkContext(sparkConf)

        // RDD算子：aggregateByKey————根据不同的计算规则进行分区内和分区间计算
        // 第一个参数列表需要传递一个初始值，用于碰到第一个key时和value进行分区内计算
        // 第二个参数列表，两个函数，对应分区内和分区间
        val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("4", 4), ("4", 90), ("a", 6)), 2)
        val aggregateByKeyRDD: RDD[(String, Int)] = rdd.aggregateByKey(0)((x, y) => math.max(x, y), _ + _)
        aggregateByKeyRDD.collect().foreach(println)

        sc.stop()
    }

}
