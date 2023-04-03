package com.it.spark.sql.core.rdd.operator.transform.Key_Value

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
 * @author ZuYingFang
 * @time 2022-03-24 15:01
 * @description
 */
object Spark03_reduceByKey {

    def main(args: Array[String]): Unit = {
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("operator")
        val sc = new SparkContext(sparkConf)

        // RDD算子：reduceByKey————将数据按照相同的Key对Value进行聚合
        val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 4), ("c", 90), ("a", 6), ("b", 175)), 2)
        val reduceByKeyRDD: RDD[(String, Int)] = rdd.reduceByKey(_ + _)
        reduceByKeyRDD.saveAsTextFile("output")


        sc.stop()
    }

}
