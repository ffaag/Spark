package com.it.spark.sql.core.rdd.operator.transform.Value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ZuYingFang
 * @time 2022-03-23 16:31
 * @description
 */
object Spark07_filter {

    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)


        // RDD算子-filter：按照指定规则(函数返回true/flase)对数据进行筛选，不改变数据的分区
        val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

        val filterRDD: RDD[Int] = rdd.filter(_ % 2 != 0)

        filterRDD.collect().foreach(println)

    }

}
