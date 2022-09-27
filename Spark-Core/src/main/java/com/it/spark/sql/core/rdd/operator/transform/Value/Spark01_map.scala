package com.it.spark.sql.core.rdd.operator.transform.Value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ZuYingFang
 * @time 2022-03-23 16:31
 * @description
 */
object Spark01_map {

    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)


        // RDD算子  map
        val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
        val mapRDD: RDD[Int] = rdd.map(_ * 2)
        mapRDD.collect().foreach(println)


        sc.stop()

    }

}
