package com.it.spark.sql.core.rdd.operator.transform.Value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ZuYingFang
 * @time 2022-03-23 16:31
 * @description
 */
object Spark04_flatMap_Test {

    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)


        // RDD算子
        val rdd: RDD[String] = sc.makeRDD(List("Hello Spark", "Hello World"))

        val mapRDD: RDD[String] = rdd.flatMap(s => s.split(" "))
        mapRDD.collect().foreach(println)

        sc.stop()

    }

}
