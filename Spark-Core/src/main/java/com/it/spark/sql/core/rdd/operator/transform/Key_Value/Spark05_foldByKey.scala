package com.it.spark.sql.core.rdd.operator.transform.Key_Value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ZuYingFang
 * @time 2022-03-24 15:01
 * @description reduceByKey分区内和分区间计算规则是相同的
 */
object Spark05_foldByKey {

    def main(args: Array[String]): Unit = {
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("operator")
        val sc = new SparkContext(sparkConf)

        // RDD算子：foldByKey————当分区内和分区间计算规则相同时，其实和reduceByKey差不多，只是多了个初始值
        val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("4", 4), ("4", 90), ("a", 6)), 2)

        val foldByKeyRDD: RDD[(String, Int)] = rdd.foldByKey(0)(_ + _)
        foldByKeyRDD.collect().foreach(println)
        sc.stop()
    }

}
