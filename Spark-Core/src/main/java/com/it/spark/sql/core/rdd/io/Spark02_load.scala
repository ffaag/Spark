package com.it.spark.sql.core.rdd.io

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ZuYingFang
 * @time 2022-03-27 12:57
 * @description
 */
object Spark02_load {

    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("operator")
        val sc = new SparkContext(sparkConf)

        val rdd: RDD[String] = sc.textFile("output1")
        println(rdd.collect().mkString(", "))


        val rdd1: RDD[(String, Int)] = sc.objectFile[(String, Int)]("output2")
        println(rdd1.collect().mkString(", "))

        val rdd2: RDD[(String, Int)] = sc.sequenceFile[String, Int]("output3")
        println(rdd2.collect().mkString(", "))


        sc.stop()

    }

}
