package com.it.spark.sql.core.rdd.operator.transform.Value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ZuYingFang
 * @time 2022-03-23 16:31
 * @description
 */
object Spark07_filter_Test {

    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)


        // RDD算子-filter：按照指定规则(函数返回true/flase)对数据进行筛选，不改变数据的分区
        val rdd: RDD[String] = sc.textFile("datas/apache.log")
        val filterRDD: RDD[String] = rdd.filter(_.split(" ")(3).split(":")(0).equals("17/05/2015"))
        filterRDD.collect().foreach(println)

    }

}
