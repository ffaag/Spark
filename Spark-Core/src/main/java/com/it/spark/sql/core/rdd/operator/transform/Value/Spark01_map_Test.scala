package com.it.spark.sql.core.rdd.operator.transform.Value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ZuYingFang
 * @time 2022-03-23 16:31
 * @description
 */
object Spark01_map_Test {

    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)


        // RDD算子  map
        val rdd: RDD[String] = sc.textFile("datas/apache.log")
        // 把长的字符串转化为短的字符串
        val mapRDD: RDD[String] = rdd.map(_.split(" ")(6))
        mapRDD.collect().foreach(println)

        sc.stop()

    }

}
